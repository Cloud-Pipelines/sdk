import abc
from typing import Dict, Mapping

from ...components import structures
from .. import artifact_stores

__all__ = ["ContainerTaskLauncher"]


class ContainerTaskLauncher(abc.ABC):
    @abc.abstractmethod
    def launch_container_task(
        self,
        task_spec: structures.TaskSpec,
        artifact_store: artifact_stores.ArtifactStore,
        input_artifacts: Mapping[str, artifact_stores.Artifact] = None,
    ) -> Dict[str, artifact_stores.Artifact]:
        raise NotImplementedError
