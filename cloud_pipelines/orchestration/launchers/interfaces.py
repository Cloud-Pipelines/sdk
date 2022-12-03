import abc
import dataclasses
import datetime
from typing import Callable, Dict, List, Mapping, Optional

from ...components import structures
from .. import storage_providers

__all__ = ["ContainerTaskLauncher", "ProcessLog", "ProcessLogEntry"]


class ContainerTaskLauncher(abc.ABC):
    @abc.abstractmethod
    def launch_container_task(
        self,
        task_spec: structures.TaskSpec,
        input_uri_readers: Mapping[str, storage_providers.UriReader],
        output_uri_writers: Mapping[str, storage_providers.UriWriter],
        on_log_entry_callback: Optional[Callable[["ProcessLogEntry"], None]] = None,
    ) -> "ContainerExecutionResult":
        raise NotImplementedError


@dataclasses.dataclass
class ContainerExecutionResult:
    start_time: datetime.datetime
    end_time: datetime.datetime
    exit_code: int
    # TODO: Replace with logs_artifact
    log: "ProcessLog"


class ProcessLog:
    def __init__(self):
        self._log_entries: List["ProcessLogEntry"] = []
        self.completed = False

    def add_entry(self, log_entry: "ProcessLogEntry"):
        if self.completed:
            raise RuntimeError("Log has already been closed")
        self._log_entries.append(log_entry)

    def close(self):
        self.completed = True

    def __str__(self) -> str:
        log_bytes = b"".join(
            list(log_entry.message_bytes for log_entry in self._log_entries)
        )
        return log_bytes.decode("utf-8")


@dataclasses.dataclass
class ProcessLogEntry:
    message_bytes: bytes
    time: Optional[datetime.datetime] = None
    annotations: Optional[dict] = None

    def __str__(self) -> str:
        result = self.message_bytes.decode("utf-8")
        if self.time:
            result = self.time.isoformat(sep=" ") + ": " + result
        return result
