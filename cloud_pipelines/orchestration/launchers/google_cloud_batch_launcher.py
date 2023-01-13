import datetime
import json
import logging
from pathlib import PurePosixPath
import typing
from typing import Callable, Mapping, Optional
import time
import uuid


from ...components import structures
from ..._components.components._components import _resolve_command_line_and_paths
from .. import storage_providers
from ..storage_providers import google_cloud_storage
from . import interfaces
from .naming_utils import sanitize_file_name


from google.cloud import batch_v1
import google.auth


_logger = logging.getLogger(__name__)

_JOB_NAME_LOG_ANNOTATION_KEY = "google_cloud_batch_job_name"

_POLL_INTERVAL_SECONDS = 10


_T = typing.TypeVar("_T")


def _assert_type(value: typing.Any, typ: typing.Type[_T]) -> _T:
    if not isinstance(value, typ):
        raise TypeError(f"Expected type {typ}, but got {type(value)}: {value}")
    return value


class GoogleCloudBatchLauncher(interfaces.ContainerTaskLauncher):
    """Launcher that uses Google Cloud Batch"""

    def __init__(
        self,
        project_id: Optional[str] = None,
        location: str = "us-central1",
        batch_client: Optional[batch_v1.BatchServiceClient] = None,
    ):
        if not project_id:
            _, project_id = google.auth.default()
        self._project_id = project_id
        self._location = location
        self._batch_client = batch_client or batch_v1.BatchServiceClient()

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

        input_names = list(input.name for input in component_spec.inputs or [])
        output_names = list(output.name for output in component_spec.outputs or [])

        container_inputs_root = PurePosixPath("/tmp/inputs/")
        container_outputs_root = PurePosixPath("/tmp/outputs/")
        container_input_paths_map = {}
        container_output_paths_map = {}
        mount_path_to_gcs_uri = {}
        container_volume_mounts = []
        for name, uri_accessor in input_uri_readers.items():
            uri = _assert_type(
                uri_accessor.uri, google_cloud_storage.GoogleCloudStorageUri
            ).uri
            uri_dir, _, file_name = uri.rpartition("/")
            container_dir = str(container_inputs_root / sanitize_file_name(name))
            container_path = container_dir + "/" + file_name
            container_input_paths_map[name] = container_path
            mount_path_to_gcs_uri[container_dir] = uri_dir
            container_volume_mounts.append(f"{container_dir}:{container_dir}:ro")

        for name, uri_accessor in output_uri_writers.items():
            uri = _assert_type(
                uri_accessor.uri, google_cloud_storage.GoogleCloudStorageUri
            ).uri
            uri_dir, _, file_name = uri.rpartition("/")
            container_dir = str(container_outputs_root / sanitize_file_name(name))
            container_path = container_dir + "/" + file_name
            container_output_paths_map[name] = container_path
            mount_path_to_gcs_uri[container_dir] = uri_dir
            container_volume_mounts.append(f"{container_dir}:{container_dir}:rw")

        # Logging to GCS URI does not seem to work. See https://issuetracker.google.com/issues/261316003
        # Workaround: Mount the logs location and write to a local path.
        log_uri = _assert_type(
            log_uri_writer.uri, google_cloud_storage.GoogleCloudStorageUri
        ).uri
        log_uri_dir, _, log_file_name = log_uri.rpartition("/")
        log_container_dir = "/tmp/log/"
        log_container_path = log_container_dir + "/" + log_file_name
        mount_path_to_gcs_uri[log_container_dir] = log_uri_dir
        container_volume_mounts.append(f"{log_container_dir}:{log_container_dir}:rw")

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
        container_env = container_spec.env
        container_command = list(resolved_cmd.command) + list(resolved_cmd.args)

        job_labels = {"sdk": "cloud-pipelines-net"}

        batch_job_template = {}
        # Need to load template through JSON.
        # See bug: https://github.com/googleapis/proto-plus-python/issues/344
        # batch_job = batch_v1.Job(batch_job_template)
        batch_job = batch_v1.Job.from_json(json.dumps(batch_job_template))

        # We need to be careful with references here.
        # When setting message attribute, the source message is copied.
        # So the changes to the inner message source do not translate to the outer message.
        if not batch_job.task_groups:
            batch_job.task_groups = [batch_v1.TaskGroup()]
        batch_task_group = batch_job.task_groups[0]

        if not batch_task_group.task_spec.runnables:
            batch_task_group.task_spec.runnables = [batch_v1.Runnable()]
        batch_runnable = batch_task_group.task_spec.runnables[0]

        batch_runnable.container.image_uri = container_image
        batch_runnable.container.entrypoint = container_command[0]
        batch_runnable.container.commands = container_command[1:]
        # Only needed to set input volumes to read-only
        batch_runnable.container.volumes = container_volume_mounts
        batch_runnable.environment.variables = container_env

        for mount_dir, gcs_dir_uri in mount_path_to_gcs_uri.items():
            volume = batch_v1.Volume()
            volume.mount_path = mount_dir
            # ! Need to remove the "gs://" URI prefix
            # ! Need to add trailing slash
            volume.gcs = batch_v1.GCS(remote_path=gcs_dir_uri[len("gs://") :] + "/")
            batch_task_group.task_spec.volumes.append(volume)

        batch_job.labels = job_labels
        # Logs do not seem to work. See https://issuetracker.google.com/issues/261316003
        # Workaround: Mount the logs location and write to a local path.
        batch_job.logs_policy.destination = batch_v1.LogsPolicy.Destination.PATH
        batch_job.logs_policy.logs_path = log_container_path

        def job_to_string(batch_job: batch_v1.Job) -> str:
            job_dict = batch_v1.Job.to_dict(
                batch_job,
                including_default_value_fields=False,
                preserving_proto_field_name=False,
                use_integers_for_enums=False,
            )
            return json.dumps(job_dict, indent=2)

        date_string = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")

        # TODO: Remove job_id generation when the Google Cloud Batch bug bug is fixed.
        # See https://github.com/googleapis/python-batch/issues/76
        job_id = "job-" + date_string + "-" + uuid.uuid4().hex[0:5]

        try:
            launched_batch_job = self._batch_client.create_job(
                job=batch_job,
                parent=f"projects/{self._project_id}/locations/{self._location}",
                job_id=job_id,
            )
        except:
            _logger.debug(
                f"Failed to launch Google Cloud Batch job: {job_to_string(batch_job)}"
            )
            raise

        # "projects/.../locations/.../jobs/..."
        job_name = launched_batch_job.name
        _logger.info(f"Launched Google Cloud Batch job: {job_name}")

        return _LaunchedGoogleCloudBatchJob(
            job_name=job_name, batch_client=self._batch_client
        )


class _LaunchedGoogleCloudBatchJob(interfaces.LaunchedContainer):
    def __init__(
        self,
        job_name: str,
        batch_client: batch_v1.BatchServiceClient,
    ):
        self._job_name = job_name
        self._batch_client = batch_client
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
        log = interfaces.ProcessLog()
        # TODO: Support getting the logs
        log.close()

        while True:
            # TODO: Properly globally throttle the API requests
            time.sleep(_POLL_INTERVAL_SECONDS)
            launched_batch_job = self._batch_client.get_job(name=self._job_name)
            if launched_batch_job.status.state in (
                batch_v1.JobStatus.State.SUCCEEDED,
                batch_v1.JobStatus.State.FAILED,
            ):
                break

        start_time = launched_batch_job.create_time
        end_time = launched_batch_job.update_time

        exit_code = -1
        if launched_batch_job.status.state == batch_v1.JobStatus.State.SUCCEEDED:
            exit_code = 0
        else:
            last_event_description = None
            for status_event in launched_batch_job.status.status_events:
                new_exit_code = status_event.task_execution.exit_code
                if new_exit_code > 0:
                    exit_code = new_exit_code
                last_event_description = status_event.description
            _logger.warning(
                f"Job {self._job_name} failed with error: {last_event_description}"
            )

        self._execution_result = interfaces.ContainerExecutionResult(
            start_time=start_time,
            end_time=end_time,
            exit_code=exit_code,
            log=log,
        )
        return self._execution_result

    def terminate(self, grace_period_seconds: Optional[int] = None):
        self._batch_client.delete_job(name=self._job_name)
