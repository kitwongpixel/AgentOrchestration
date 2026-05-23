"""Webhook subscription and delivery helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4


ALLOWED_FILTER_FIELDS: Set[str] = {
    "event_id",
    "event_type",
    "cursor",
    "workspace_id",
    "payload",
}

INTERNAL_ONLY_FIELDS: Set[str] = {
    "internal_only",
    "secret",
    "trace_id",
    "database_id",
}

ALLOWED_WORKSPACE_ROLES: Set[str] = {
    "workspace:owner",
    "workspace:operator",
}


class WebhookError(Exception):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


@dataclass
class WebhookDelivery:
    delivery_id: str
    endpoint: str
    payload: Dict[str, Any]


@dataclass
class WebhookSubscription:
    subscription_id: str
    workspace_id: str
    endpoint: str
    filters: List[str]
    active: bool = True
    rotated: bool = False
    deliveries: Dict[str, WebhookDelivery] = field(default_factory=dict)


class WebhookService:
    def __init__(self):
        self._subscriptions: Dict[str, WebhookSubscription] = {}

    def clear(self) -> None:
        self._subscriptions.clear()

    def _require_workspace_access(self, workspace_id: Optional[str], role: Optional[str]) -> None:
        if not workspace_id:
            raise WebhookError(401, "Missing workspace context")
        if role not in ALLOWED_WORKSPACE_ROLES:
            raise WebhookError(403, "Insufficient workspace role")

    def _validate_filters(self, filters: List[str]) -> None:
        invalid = [field for field in filters if field not in ALLOWED_FILTER_FIELDS]
        if invalid:
            raise WebhookError(400, f"Unsupported subscription filters: {', '.join(sorted(invalid))}")

    def _validate_endpoint(self, endpoint: str) -> None:
        if not endpoint or not endpoint.startswith("https://"):
            raise WebhookError(400, "Webhook endpoint must use https://")

    def register_subscription(
        self,
        *,
        workspace_id: str,
        role: str,
        endpoint: str,
        filters: Optional[List[str]] = None,
    ) -> WebhookSubscription:
        self._require_workspace_access(workspace_id, role)
        self._validate_endpoint(endpoint)
        filters = filters or []
        self._validate_filters(filters)

        subscription = WebhookSubscription(
            subscription_id=str(uuid4()),
            workspace_id=workspace_id,
            endpoint=endpoint,
            filters=list(dict.fromkeys(filters)),
        )
        self._subscriptions[subscription.subscription_id] = subscription
        return subscription

    def get_subscription(self, subscription_id: str) -> Optional[WebhookSubscription]:
        return self._subscriptions.get(subscription_id)

    def disable_subscription(
        self,
        *,
        workspace_id: str,
        role: str,
        subscription_id: str,
    ) -> WebhookSubscription:
        subscription = self._get_owned_subscription(subscription_id, workspace_id, role)
        subscription.active = False
        return subscription

    def rotate_subscription_endpoint(
        self,
        *,
        workspace_id: str,
        role: str,
        subscription_id: str,
        new_endpoint: str,
    ) -> WebhookSubscription:
        subscription = self._get_owned_subscription(subscription_id, workspace_id, role)
        self._validate_endpoint(new_endpoint)
        subscription.endpoint = new_endpoint
        subscription.rotated = True
        return subscription

    def deliver_event(
        self,
        *,
        workspace_id: str,
        role: str,
        subscription_id: str,
        endpoint: str,
        delivery_id: str,
        event: Dict[str, Any],
    ) -> Dict[str, Any]:
        subscription = self._get_owned_subscription(subscription_id, workspace_id, role)
        self._validate_endpoint(endpoint)

        if not subscription.active:
            raise WebhookError(410, "Webhook subscription is disabled")
        if subscription.rotated or endpoint != subscription.endpoint:
            raise WebhookError(410, "Webhook endpoint is stale")

        if delivery_id in subscription.deliveries:
            delivery = subscription.deliveries[delivery_id]
            return {
                "status": "ok",
                "idempotent": True,
                "delivery_id": delivery.delivery_id,
                "subscription_id": subscription.subscription_id,
                "endpoint": delivery.endpoint,
                "payload": delivery.payload,
            }

        payload = self._build_public_payload(subscription, event)
        delivery = WebhookDelivery(
            delivery_id=delivery_id,
            endpoint=endpoint,
            payload=payload,
        )
        subscription.deliveries[delivery_id] = delivery
        return {
            "status": "ok",
            "idempotent": False,
            "delivery_id": delivery.delivery_id,
            "subscription_id": subscription.subscription_id,
            "endpoint": endpoint,
            "payload": payload,
        }

    def _get_owned_subscription(
        self,
        subscription_id: str,
        workspace_id: str,
        role: Optional[str],
    ) -> WebhookSubscription:
        self._require_workspace_access(workspace_id, role)
        subscription = self._subscriptions.get(subscription_id)
        if subscription is None:
            raise WebhookError(404, "Webhook subscription not found")
        if subscription.workspace_id != workspace_id:
            raise WebhookError(403, "Workspace access denied")
        return subscription

    def _build_public_payload(
        self,
        subscription: WebhookSubscription,
        event: Dict[str, Any],
    ) -> Dict[str, Any]:
        public_payload: Dict[str, Any] = {
            "subscription_id": subscription.subscription_id,
            "workspace_id": subscription.workspace_id,
        }
        for field in subscription.filters:
            if field in event and field not in INTERNAL_ONLY_FIELDS:
                public_payload[field] = self._sanitize_event_value(event[field])

        for key, value in event.items():
            if key in {"subscription_id", "workspace_id"}:
                continue
            if key in INTERNAL_ONLY_FIELDS:
                continue
            if key in subscription.filters:
                continue
            if key == "payload" and isinstance(value, dict):
                public_payload[key] = {
                    inner_key: inner_value
                    for inner_key, inner_value in value.items()
                    if inner_key not in INTERNAL_ONLY_FIELDS
                }
            elif key == "payload":
                public_payload[key] = self._sanitize_event_value(value)

        return public_payload

    def _sanitize_event_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: self._sanitize_event_value(nested_value)
                for key, nested_value in value.items()
                if key not in INTERNAL_ONLY_FIELDS
            }
        if isinstance(value, list):
            return [self._sanitize_event_value(item) for item in value]
        return value


webhooks = WebhookService()
