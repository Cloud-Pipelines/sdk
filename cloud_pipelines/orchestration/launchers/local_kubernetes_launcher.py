import datetime
import logging
import os
from pathlib import Path, PurePosixPath
import shutil
import tempfile
from typing import Callable, Dict, List, Mapping, Optional

from kubernetes import config as k8s_config
from kubernetes import client as k8s_client
from kubernetes import watch as k8s_watch

from ...components import structures
from ..._components.components._components import _resolve_command_line_and_paths

from .. import artifact_stores
from . import interfaces
from .kubernetes_utils import (
    wait_for_pod_to_stop_pending,
    wait_for_pod_to_succeed_or_fail,
)
from .naming_utils import sanitize_file_name, sanitize_kubernetes_resource_name


_logger = logging.getLogger(__name__)

_POD_NAME_LOG_ANNOTATION_KEY = "kubernetes_pod_name"


class LocalKubernetesContainerLauncher(interfaces.ContainerTaskLauncher):
    """Launcher that uses single-node Kubernetes (uses hostPath for data passing)"""

    def __init__(
        self,
        namespace: str = "default",
        service_account: str = None,
        service_account_name: str = None,
        client: k8s_client.ApiClient = None,
    ):
        self._namespace = namespace
        self._service_account = service_account
        self._service_account_name = service_account_name

        if client:
            self._k8s_client = client
        else:
            try:
                k8s_config.load_incluster_config()
            except:
                k8s_config.load_kube_config()
            self._k8s_client = k8s_client.ApiClient()

    def launch_container_task(
        self,
        task_spec: structures.TaskSpec,
        artifact_store: artifact_stores.ArtifactStore,
        input_artifacts: Mapping[str, artifact_stores.Artifact] = None,
        on_log_entry_callback: Optional[
            Callable[[interfaces.ProcessLogEntry], None]
        ] = None,
    ) -> interfaces.ContainerExecutionResult:
        component_ref: structures.ComponentReference = task_spec.component_ref
        component_spec: structures.ComponentSpec = component_ref.spec
        container_spec: structures.ContainerSpec = (
            component_spec.implementation.container
        )

        input_artifacts = input_artifacts or {}

        input_names = list(input.name for input in component_spec.inputs or [])
        output_names = list(output.name for output in component_spec.outputs or [])

        # Not using with tempfile.TemporaryDirectory() as tempdir: due to: OSError: [WinError 145] The directory is not empty
        # Python 3.10 supports the ignore_cleanup_errors=True parameter.
        try:
            tempdir = tempfile.mkdtemp()

            host_workdir = os.path.join(tempdir, "work")
            # host_logdir = os.path.join(tempdir, 'logs')
            host_input_paths_map = {
                name: os.path.join(tempdir, "inputs", sanitize_file_name(name), "data")
                for name in input_names
            }  # Or just use random temp dirs/subdirs
            host_output_paths_map = {
                name: os.path.join(tempdir, "outputs", sanitize_file_name(name), "data")
                for name in output_names
            }  # Or just use random temp dirs/subdirs

            Path(host_workdir).mkdir(parents=True, exist_ok=True)

            container_input_root = PurePosixPath("/tmp/inputs/")
            container_output_root = PurePosixPath("/tmp/outputs/")
            container_input_paths_map = {
                name: str(container_input_root / sanitize_file_name(name) / "data")
                for name in input_names
            }  # Or just user random temp dirs/subdirs
            container_output_paths_map = {
                name: str(container_output_root / sanitize_file_name(name) / "data")
                for name in output_names
            }  # Or just user random temp dirs/subdirs

            # Getting artifact values when needed
            # Design options: We could download/mount all artifacts and optionally read from local files,
            # but mounting can be more expensive compared to downloading.
            def artifact_value_getter(
                value: artifact_stores.Artifact, input_type
            ) -> str:
                assert isinstance(value, artifact_stores.Artifact)
                return value.read_text()

            resolved_cmd = _resolve_command_line_and_paths(
                component_spec=component_spec,
                arguments=input_artifacts,
                input_path_generator=container_input_paths_map.get,
                output_path_generator=container_output_paths_map.get,
                argument_serializer=artifact_value_getter,
            )

            # Getting the input data
            for input_name in resolved_cmd.input_paths.keys():
                input_host_path = host_input_paths_map[input_name]
                input_artifact = input_artifacts[input_name]
                Path(input_host_path).parent.mkdir(parents=True, exist_ok=True)
                input_artifact.download(path=input_host_path)

            # Preparing the output locations
            for output_host_path in host_output_paths_map.values():
                Path(output_host_path).parent.mkdir(parents=True, exist_ok=True)

            volumes = []
            volume_mounts = []

            for input_name in input_names:
                host_dir = os.path.dirname(host_input_paths_map[input_name])
                if os.name == "nt":
                    host_dir = windows_path_to_docker_path(host_dir)
                container_dir = os.path.dirname(container_input_paths_map[input_name])
                volume_name = sanitize_kubernetes_resource_name("inputs-" + input_name)
                volumes.append(
                    k8s_client.V1Volume(
                        name=volume_name,
                        host_path=k8s_client.V1HostPathVolumeSource(
                            path=host_dir,
                            # type=?
                        ),
                    )
                )
                volume_mounts.append(
                    k8s_client.V1VolumeMount(
                        name=volume_name,
                        mount_path=container_dir,
                        # mount_propagation=?
                        read_only=False,  # We're copying the input data anyways, so it's OK if the container modifies it.
                        # sub_path=....
                    )
                )

            for output_name in output_names:
                host_dir = os.path.dirname(host_output_paths_map[output_name])
                if os.name == "nt":
                    host_dir = windows_path_to_docker_path(host_dir)
                container_dir = os.path.dirname(container_output_paths_map[output_name])
                volume_name = sanitize_kubernetes_resource_name(
                    "outputs-" + output_name
                )
                volumes.append(
                    k8s_client.V1Volume(
                        name=volume_name,
                        host_path=k8s_client.V1HostPathVolumeSource(
                            path=host_dir,
                            # type=?
                        ),
                    )
                )
                volume_mounts.append(
                    k8s_client.V1VolumeMount(
                        name=volume_name,
                        mount_path=container_dir,
                        # sub_path=....
                    )
                )

            container_env = [
                k8s_client.V1EnvVar(name=name, value=value)
                for name, value in (container_spec.env or {}).items()
            ]
            main_container_spec = k8s_client.V1Container(
                name="main",
                image=container_spec.image,
                command=resolved_cmd.command,
                args=resolved_cmd.args,
                env=container_env,
                volume_mounts=volume_mounts,
            )

            pod_spec = k8s_client.V1PodSpec(
                init_containers=[],
                containers=[
                    main_container_spec,
                ],
                volumes=volumes,
                restart_policy="Never",
                service_account=self._service_account,
                service_account_name=self._service_account_name,
            )

            pod = k8s_client.V1Pod(
                api_version="v1",
                kind="Pod",
                metadata=k8s_client.V1ObjectMeta(
                    # name='',
                    generate_name="task-pod-",
                    # namespace=self._namespace,
                    labels={},
                    annotations={},
                    owner_references=[
                        # k8s_client.V1OwnerReference(),
                    ],
                ),
                spec=pod_spec,
            )

            # Note: Need to reuse the k8s ApiClient.
            # Otherwise doing `k8s_client.CoreV1Api().*` every time leads to warnings during unit tests:
            # ResourceWarning: unclosed <ssl.SSLSocket fd=1056, family=AddressFamily.AF_INET, type=SocketKind.SOCK_STREAM, proto=0, laddr=('127.0.0.1', 56456), raddr=('127.0.0.1', 6443)>
            core_api = k8s_client.CoreV1Api(api_client=self._k8s_client)
            pod_res = core_api.create_namespaced_pod(
                namespace=self._namespace,
                body=pod,
            )

            _logger.info(f"Created pod {pod_res.metadata.name}")

            pod_name = pod_res.metadata.name
            wait_for_pod_to_stop_pending(
                pod_name=pod_name,
                namespace=self._namespace,
                api_client=self._k8s_client,
                timeout_seconds=30,
            )
            pod = wait_for_pod_to_succeed_or_fail(
                pod_name=pod_name,
                namespace=self._namespace,
                api_client=self._k8s_client,
            )
            if not pod:
                raise NotImplementedError
            pod_status: k8s_client.V1PodStatus = pod.status
            container_statuses: List[
                k8s_client.V1ContainerStatus
            ] = pod_status.container_statuses
            main_container_statuses = [
                container_status
                for container_status in container_statuses
                if container_status.name == "main"
            ]
            if len(main_container_statuses) != 1:
                raise RuntimeError(
                    f"Cannot get the main container status form the pod: {pod}"
                )
            main_container_status = main_container_statuses[0]
            main_container_state: k8s_client.V1ContainerState = (
                main_container_status.state
            )
            main_container_terminated_state: k8s_client.V1ContainerStateTerminated = (
                main_container_state.terminated
            )
            start_time = main_container_terminated_state.started_at
            end_time = main_container_terminated_state.finished_at
            exit_code = main_container_terminated_state.exit_code

            log = interfaces.ProcessLog()
            try:
                for text in k8s_watch.Watch().stream(
                    core_api.read_namespaced_pod_log,
                    name=pod_res.metadata.name,
                    namespace=pod_res.metadata.namespace,
                ):
                    log_entry = interfaces.ProcessLogEntry(
                        message_bytes=text.encode("utf-8"),
                        time=datetime.datetime.utcnow(),
                        annotations={
                            _POD_NAME_LOG_ANNOTATION_KEY: pod_res.metadata.name
                        },
                    )
                    if on_log_entry_callback:
                        on_log_entry_callback(log_entry=log_entry)
                    log.add_entry(log_entry)
            except k8s_client.ApiException as e:
                _logger.exception(f"Exception while reading the logs.", exc_info=True)
            log.close()

            # Storing the output data
            output_artifacts = None
            succeeded = exit_code == 0
            if succeeded:
                output_artifacts = {}
                for output_name in output_names:
                    output_host_path = host_output_paths_map[output_name]
                    artifact = artifact_store.upload(path=output_host_path)
                    output_artifacts[output_name] = artifact

            execution_result = interfaces.ContainerExecutionResult(
                start_time=start_time,
                end_time=end_time,
                exit_code=exit_code,
                log=log,
                output_artifacts=output_artifacts,
            )
            return execution_result
        finally:
            shutil.rmtree(tempdir, ignore_errors=True)


def windows_path_to_docker_path(path: str) -> str:
    if os.name != "nt":
        return path

    path_obj = Path(path)
    if not path_obj.is_absolute():
        path_obj = path_obj.resolve()

    path_parts = list(path_obj.parts)
    # Changing the drive syntax: "C:\" -> "c"
    path_parts[0] = path_parts[0][0].lower()
    # WSL2 Docker path fix. See https://stackoverflow.com/questions/62812948/volume-mounts-not-working-kubernetes-and-wsl-2-and-docker/63524931#63524931
    posix_path = PurePosixPath("/run/desktop/mnt/host/", *path_parts)
    return str(posix_path)
