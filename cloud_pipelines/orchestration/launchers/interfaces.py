import abc
import dataclasses
import datetime
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
    ) -> "ContainerExecutionResult":
        raise NotImplementedError


@dataclasses.dataclass
class ContainerExecutionResult:
    start_time: datetime.datetime
    end_time: datetime.datetime
    exit_code: int
    # TODO: Replace with logs_artifact
    logs: bytes
    output_artifacts: Dict[str, artifact_stores.Artifact]
