# pip install google-cloud-aiplatform==1.19.0
import copy
import logging
import time
import typing
from typing import Callable, Mapping, Optional


from ...components import structures
from .. import storage_providers
from ..storage_providers import google_cloud_storage
from . import interfaces
from ._container_utils import _resolve_command_line_and_paths


from google.cloud import aiplatform
from google.cloud.aiplatform.compat.types import job_state as gca_job_state
import google.cloud.logging


_logger = logging.getLogger(__name__)

_JOB_NAME_LOG_ANNOTATION_KEY = "google_cloud_vertex_ai_custom_job_name"


_CUSTOM_JOB_DEFAULT_MACHINE_TYPE = "e2-standard-4"  # Required. https://cloud.google.com/vertex-ai/docs/training/configure-compute#machine-types


_T = typing.TypeVar("_T")


def _assert_type(value: typing.Any, typ: typing.Type[_T]) -> _T:
    if not isinstance(value, typ):
        raise TypeError(f"Expected type {typ}, but got {type(value)}: {value}")
    return value


class GoogleCloudVertexAiCustomJobLauncher(interfaces.ContainerTaskLauncher):
    """Launcher that uses Google Cloud Vertex AI CustomJob"""

    def __init__(
        self,
        project_id: Optional[str] = None,
        location: Optional[str] = None,
    ):
        aiplatform.init(project=project_id, location=location)

    def launch_container_task(
        self,
        task_spec: structures.TaskSpec,
        input_uri_readers: Mapping[str, storage_providers.UriReader],
        output_uri_writers: Mapping[str, storage_providers.UriWriter],
        log_uri_writer: storage_providers.UriWriter,
    ) -> interfaces.LaunchedContainer:
        component_ref: structures.ComponentReference = task_spec.component_ref
        component_spec: structures.ComponentSpec = component_ref.spec
        container_spec: structures.ContainerSpec = (
            component_spec.implementation.container
        )

        container_input_paths_map = {}
        container_output_paths_map = {}
        for name, uri_reader in input_uri_readers.items():
            uri = _assert_type(
                uri_reader.uri, google_cloud_storage.GoogleCloudStorageUri
            ).uri
            container_path = uri.replace("gs://", "/gcs/")
            container_input_paths_map[name] = container_path

        for name, uri_writer in output_uri_writers.items():
            uri = _assert_type(
                uri_writer.uri, google_cloud_storage.GoogleCloudStorageUri
            ).uri
            container_path = uri.replace("gs://", "/gcs/")
            container_output_paths_map[name] = container_path

        # Getting artifact values when needed
        # Design options: We could download/mount all artifacts and optionally read from local files,
        # but mounting can be more expensive compared to downloading.
        def artifact_value_getter(
            value: storage_providers.UriReader, type_name: str
        ) -> str:
            assert isinstance(value, storage_providers.UriReader)
            return value.download_as_text()

        resolved_cmd = _resolve_command_line_and_paths(
            component_spec=component_spec,
            arguments=input_uri_readers,
            input_path_generator=container_input_paths_map.get,
            output_path_generator=container_output_paths_map.get,
            argument_serializer=artifact_value_getter,
        )

        container_image = container_spec.image
        container_env = container_spec.env or {}
        container_command = list(resolved_cmd.command) + list(resolved_cmd.args)

        job_labels = {"sdk": "cloud-pipelines-net"}

        # These parameters and templates will be customizable in the future.
        replica_count = 1
        main_worker_pool_spec = {}
        extra_worker_pool_specs = []
        main_worker_pool_spec["container_spec"] = {
            "image_uri": container_image,
            "command": container_command,
            "env": [
                {
                    "name": name,
                    "value": value,
                }
                for name, value in container_env.items()
            ],
        }
        main_worker_pool_spec["replica_count"] = 1
        main_worker_pool_spec.setdefault("machine_spec", {}).setdefault(
            "machine_type", _CUSTOM_JOB_DEFAULT_MACHINE_TYPE
        )
        worker_pool_specs = [main_worker_pool_spec]
        if replica_count > 1:
            worker_pool_spec_1 = copy.copy(main_worker_pool_spec)
            worker_pool_spec_1["replica_count"] = replica_count - 1
            worker_pool_specs.append(worker_pool_spec_1)
        worker_pool_specs.extend(extra_worker_pool_specs)

        custom_job = aiplatform.CustomJob(
            # Passing None to auto-generate the name
            display_name=None,
            worker_pool_specs=worker_pool_specs,
            labels=job_labels,
            # Currently these parameters are required by the high-level CustomJob.
            # But we do not want to pass anything here.
            # We will remove the base_output_directory in next lines.
            base_output_dir="gs://dummy",
            staging_bucket="gs://dummy",
        )
        # We do not use this functionality and need to disable it to prevent failures.
        del custom_job.job_spec.base_output_directory
        try:
            custom_job.submit()
        except:
            _logger.debug(
                f"Failed to launch Google Cloud Vertex AI CustomJob: {custom_job.to_dict()}"
            )
            raise

        _logger.info(
            f"Launched Google Cloud Vertex AI CustomJob: {custom_job.resource_name}"
        )

        def upload_logs(text_log: str):
            log_uri_writer.upload_from_text(text_log)

        return _LaunchedGoogleCloudVertexAiCustomJob(
            custom_job=custom_job, upload_logs=upload_logs
        )


class _LaunchedGoogleCloudVertexAiCustomJob(interfaces.LaunchedContainer):
    def __init__(
        self,
        custom_job: aiplatform.CustomJob,
        upload_logs: Callable[[str], None],
    ):
        self._custom_job = custom_job
        self._upload_logs = upload_logs
        self._execution_result: Optional[interfaces.ContainerExecutionResult] = None

    def wait_for_completion(
        self,
        on_log_entry_callback: Optional[
            Callable[[interfaces.ProcessLogEntry], None]
        ] = None,
    ) -> interfaces.ContainerExecutionResult:
        # This function is not thread-safe. It can only be called once.
        if self._execution_result:
            # TODO: Stream the logs?
            return self._execution_result

        # TODO: Disable the Vertex SDK logging when it polls the job
        self._custom_job._block_until_complete()

        # CustomJob writes logs to Google Cloud Logging. We need to get the logs from there.
        # TODO: Support streaming the logs
        log = interfaces.ProcessLog()
        text_log_chunks = []
        # TODO: Investigate the issue where logs are empty for most jobs right after completion, but appear a bit later.
        # Workaround: Wait until the logs appear
        all_cloud_log_entries = []
        for _ in range(10):
            all_cloud_log_entries = list(
                google.cloud.logging.Client().list_entries(
                    filter_=f'resource.labels.job_id="{self._custom_job.name}"'
                )
            )
            # Heuristic: There should be at least 5 messages (6 really, by the 6th can be 5 min late):
            # 1. service: Started
            # 2,3,4: gcsfuse
            # 5. service: Ended
            # TODO: Improve the heuristic - check that there are 2+ messages from service
            if len(all_cloud_log_entries) > 5:
                break
            time.sleep(5)
        for cloud_log_entry in all_cloud_log_entries:
            if (
                isinstance(cloud_log_entry.payload, dict)
                and cloud_log_entry.payload.get("name") != "root"
            ):
                log_entry_time = cloud_log_entry.timestamp
                message = cloud_log_entry.payload["message"]
                log_entry = interfaces.ProcessLogEntry(
                    message_bytes=message.encode("utf-8"),
                    time=log_entry_time,
                    annotations={
                        _JOB_NAME_LOG_ANNOTATION_KEY: self._custom_job.resource_name
                    },
                )
                if on_log_entry_callback:
                    on_log_entry_callback(log_entry)
                log.add_entry(log_entry)
                text_log_chunks.append(
                    log_entry_time.isoformat(sep=" ", timespec="seconds")
                    + "\t"
                    + message
                )
        log.close()

        self._upload_logs("".join(text_log_chunks))

        start_time = self._custom_job.create_time
        end_time = self._custom_job.update_time

        exit_code = -1
        if self._custom_job.state == gca_job_state.JobState.JOB_STATE_SUCCEEDED:
            exit_code = 0
        else:
            # TODO: Get exit code
            _logger.warning(
                f"CustomJob {self._custom_job.name} failed with error: {self._custom_job.error}"
            )

        self._execution_result = interfaces.ContainerExecutionResult(
            start_time=start_time,
            end_time=end_time,
            exit_code=exit_code,
            log=log,
        )
        return self._execution_result

    def terminate(self, grace_period_seconds: Optional[int] = None):
        self._custom_job.cancel()
