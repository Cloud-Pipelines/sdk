import os
from pathlib import Path, PurePosixPath
import shutil
import tempfile
from typing import Callable

from kubernetes import config as k8s_config
from kubernetes import client as k8s_client

from ...components import structures
from ..._components.components._components import _resolve_command_line_and_paths

from .kubernetes_utils import (
    wait_for_pod_to_stop_pending,
    wait_for_pod_to_succeed_or_fail,
)
from .naming_utils import sanitize_file_name, sanitize_kubernetes_resource_name


class LocalKubernetesContainerLauncher:
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
        download: Callable[[str], str],
        upload: Callable[[str], str],
        input_uris_map: dict = None,
        output_uris_map: dict = None,
    ):
        input_uris_map = input_uris_map or {}
        output_uris_map = output_uris_map or {}

        input_names = list(input_uris_map.keys())
        output_names = list(output_uris_map.keys())

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

            # Getting input data
            for input_name, input_uri in input_uris_map.items():
                input_host_path = host_input_paths_map[input_name]
                Path(input_host_path).parent.mkdir(parents=True, exist_ok=True)
                download(input_uri, input_host_path)

            for output_host_path in host_output_paths_map.values():
                Path(output_host_path).parent.mkdir(parents=True, exist_ok=True)

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

            component_spec = task_spec.component_ref.spec

            resolved_cmd = _resolve_command_line_and_paths(
                component_spec=component_spec,
                arguments=task_spec.arguments,
                input_path_generator=container_input_paths_map.get,
                output_path_generator=container_output_paths_map.get,
            )

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
                for name, value in (
                    component_spec.implementation.container.env or {}
                ).items()
            ]
            main_container_spec = k8s_client.V1Container(
                name="main",
                image=component_spec.implementation.container.image,
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

            core_api = k8s_client.CoreV1Api(api_client=self._k8s_client)
            pod_res = core_api.create_namespaced_pod(
                namespace=self._namespace,
                body=pod,
            )

            print("Pod name:")
            print(pod_res.metadata.name)

            pod_name = pod_res.metadata.name
            wait_for_pod_to_stop_pending(
                pod_name=pod_name, api_client=self._k8s_client, timeout_seconds=30
            )
            wait_for_pod_to_succeed_or_fail(
                pod_name=pod_name, api_client=self._k8s_client
            )

            # Storing the output data
            for output_name, output_uri in output_uris_map.items():
                output_host_path = host_output_paths_map[output_name]
                upload(output_host_path, output_uri)
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
