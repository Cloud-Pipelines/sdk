import logging
from typing import Any, Callable, Optional

from kubernetes import client as k8s_client
from kubernetes import watch as k8s_watch


def wait_for_pod_state(
    pod_name: str,
    predicate: Callable[[Any], bool],
    api_client: k8s_client.ApiClient = None,
    timeout_seconds: int = None,
) -> Optional[k8s_client.V1Pod]:
    pod_watch = k8s_watch.Watch()
    # label_selector=pod_name does not work
    core_api = k8s_client.CoreV1Api(api_client=api_client)
    for event in pod_watch.stream(
        core_api.list_pod_for_all_namespaces, timeout_seconds=timeout_seconds
    ):
        # event_type = event["type"]
        obj = event["object"]  # Also event['raw_object']
        kind = obj.kind
        name = obj.metadata.name
        if kind == "Pod" and name == pod_name:
            if predicate(obj):
                pod_watch.stop()
                return obj
    return None


def wait_for_pod_to_stop_pending(
    pod_name: str,
    api_client: k8s_client.ApiClient = None,
    timeout_seconds: int = 30,
) -> Optional[k8s_client.V1Pod]:
    logging.info("wait_for_pod_to_stop_pending({})".format(pod_name))
    return wait_for_pod_state(
        pod_name=pod_name,
        predicate=lambda pod: pod.status.phase != "Pending",
        api_client=api_client,
        timeout_seconds=timeout_seconds,
    )


def wait_for_pod_to_succeed_or_fail(
    pod_name: str,
    api_client: k8s_client.ApiClient = None,
    timeout_seconds: int = None,
) -> Optional[k8s_client.V1Pod]:
    logging.info("wait_for_pod_to_succeed_or_fail({})".format(pod_name))
    return wait_for_pod_state(
        pod_name=pod_name,
        # Pod phase is one of: Pending,Running,Succeeded,Failed,Unknown
        predicate=lambda pod: pod.status.phase in ("Succeeded", "Failed"),
        api_client=api_client,
        timeout_seconds=timeout_seconds,
    )
