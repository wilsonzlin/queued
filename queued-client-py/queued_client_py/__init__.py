from dataclasses import asdict
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from urllib.parse import quote
import json
import msgpack
import requests


class QueuedUnauthorizedError(Exception):
    def __init__(self):
        super().__init__("Authorization failed")


class QueuedApiError(Exception):
    def __init__(self, status: int, error: Optional[str], error_details: Optional[Any]):
        error_message = f"Request to queued failed with status {status}: {error}"
        if error_details:
            error_message += f"\n\n\tDetails: {json.dumps(error_details, indent=2)}"
        super().__init__(error_message)
        self.status = status
        self.error = error
        self.error_details = error_details


def qpp(name: str) -> str:
    return f"/queue/{quote(name, safe='')}"


@dataclass
class QueueMetrics:
    empty_poll_counter: int
    message_counter: int
    missing_delete_counter: int
    missing_update_counter: int
    successful_delete_counter: int
    successful_poll_counter: int
    successful_push_counter: int
    successful_update_counter: int
    suspended_delete_counter: int
    suspended_poll_counter: int
    suspended_push_counter: int
    suspended_update_counter: int
    throttled_poll_counter: int

    first_message_visibility_timeout_sec_gauge: int
    last_message_visibility_timeout_sec_gauge: int
    longest_unpolled_message_sec_gauge: int


@dataclass
class Message:
    id: int
    poll_tag: int


@dataclass
class PollItem:
    message: Message
    contents: Any


@dataclass
class PushItem:
    contents: Any
    visibility_timeout_secs: int


class QueuedQueueClient:
    def __init__(self, svc: "QueuedClient", queue_name: str):
        self.svc = svc
        self.queue_name = queue_name

    def metrics(self) -> QueueMetrics:
        res = self.svc.raw_request("GET", f"{qpp(self.queue_name)}/metrics", None)
        return QueueMetrics(**res)

    def poll_messages_raw(
        self,
        count: int,
        visibility_timeout_secs: int,
        ignore_existing_visibility_timeouts: bool = False,
    ) -> List[PollItem]:
        res = self.svc.raw_request(
            "POST",
            f"{qpp(self.queue_name)}/messages/poll",
            {
                "count": count,
                "visibility_timeout_secs": visibility_timeout_secs,
                "ignore_existing_visibility_timeouts": ignore_existing_visibility_timeouts,
            },
        )
        return [
            PollItem(
                message=Message(
                    id=msg["id"],
                    poll_tag=msg["poll_tag"],
                ),
                contents=msg["contents"],
            )
            for msg in res["messages"]
        ]

    def poll_messages(
        self,
        count: int,
        visibility_timeout_secs: int,
        ignore_existing_visibility_timeouts: bool = False,
    ) -> List[PollItem]:
        res = self.poll_messages_raw(
            count, visibility_timeout_secs, ignore_existing_visibility_timeouts
        )
        for msg in res:
            msg.contents = msgpack.unpackb(msg.contents, strict_map_key=True)
        return res

    def push_messages_raw(self, messages: List[PushItem]) -> List[int]:
        res = self.svc.raw_request(
            "POST",
            f"{qpp(self.queue_name)}/messages/push",
            {
                "messages": [
                    {
                        "contents": msg.contents,
                        "visibility_timeout_secs": msg.visibility_timeout_secs,
                    }
                    for msg in messages
                ]
            },
        )
        return res["ids"]

    def push_messages(self, messages: List[PushItem]) -> List[int]:
        return self.push_messages_raw(
            [
                PushItem(
                    contents=msgpack.packb(msg.contents),
                    visibility_timeout_secs=msg.visibility_timeout_secs,
                )
                for msg in messages
            ]
        )

    def update_message(self, message: Message, new_visibility_timeout_secs: int) -> int:
        res = self.svc.raw_request(
            "POST",
            f"{qpp(self.queue_name)}/messages/update",
            {
                "id": message.id,
                "poll_tag": message.poll_tag,
                "visibility_timeout_secs": new_visibility_timeout_secs,
            },
        )
        return res["new_poll_tag"]

    def delete_messages(self, messages: List[Message]):
        self.svc.raw_request(
            "POST",
            f"{qpp(self.queue_name)}/messages/delete",
            {"messages": [asdict(msg) for msg in messages]},
        )


@dataclass
class ApiKey:
    key: str
    prefix: str


class QueuedClient:
    def __init__(self, endpoint: str, api_key: Optional[str] = None):
        self.endpoint = endpoint
        self.api_key = api_key

    def queue(self, queue_name: str) -> QueuedQueueClient:
        return QueuedQueueClient(self, queue_name)

    def raw_request(
        self, method: str, path: str, body: Optional[Dict[str, Any]]
    ) -> Any:
        headers = {}
        headers["Accept"] = "application/msgpack"
        if self.api_key:
            headers["Authorization"] = self.api_key
        if body is not None:
            headers["Content-Type"] = "application/msgpack"
            body = msgpack.packb(body)
        res = requests.request(
            method=method,
            url=self.endpoint + path,
            headers=headers,
            data=body,
        )
        if res.status_code == 401:
            raise QueuedUnauthorizedError()
        if res.headers.get("content-type") in (
            "application/msgpack",
            "application/x-msgpack",
        ):
            res_body = msgpack.unpackb(res.content, strict_map_key=True)
        else:
            res_body = res.content.decode("utf-8", "replace")
        if not (200 <= res.status_code < 300):
            raise QueuedApiError(
                res.status_code,
                res_body.get("error") if type(res_body) is dict else res_body,
                res_body.get("error_details") if type(res_body) is dict else None,
            )
        return res_body

    def list_api_keys(self) -> List[ApiKey]:
        res = self.raw_request("GET", "/api-keys", None)
        return [ApiKey(**k) for k in res["keys"]]

    def delete_api_key(self, api_key: str):
        self.raw_request("DELETE", "/api-key/" + quote(api_key, safe=""), None)

    def set_api_key(self, api_key: str, queue_name_prefix: str):
        self.raw_request(
            "PUT",
            "/api-key/" + quote(api_key, safe=""),
            {
                "prefix": queue_name_prefix,
            },
        )

    def delete_queue(self, queue_name: str):
        self.raw_request("DELETE", qpp(queue_name), None)

    def create_queue(self, queue_name: str):
        self.raw_request("PUT", qpp(queue_name), None)

    def list_queues(self) -> List[str]:
        res = self.raw_request("GET", "/queues", None)
        return [q["name"] for q in res["queues"]]
