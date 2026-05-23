from fastapi.testclient import TestClient

from src.api.server import create_app
from src.api.webhooks import webhooks


def setup_function():
    webhooks.clear()


def client():
    return TestClient(create_app())


def headers(workspace_id="workspace-a", role="workspace:operator"):
    return {
        "Authorization": "Bearer test-token",
        "X-Workspace-ID": workspace_id,
        "X-Workspace-Role": role,
    }


def register_subscription(client_instance, filters=None, endpoint="https://hooks.example.com/a"):
    response = client_instance.post(
        "/api/v2/webhooks/subscriptions",
        json={
            "endpoint": endpoint,
            "filters": filters or ["event_id", "event_type", "payload"],
        },
        headers=headers(),
    )
    assert response.status_code == 200
    return response.json()


def test_valid_delivery_filters_internal_fields_and_preserves_public_data():
    http = client()
    subscription = register_subscription(http)

    response = http.post(
        f"/api/v2/webhooks/subscriptions/{subscription['subscription_id']}/deliver",
        json={
            "endpoint": subscription["endpoint"],
            "delivery_id": "delivery-1",
            "event": {
                "event_id": "evt-1",
                "event_type": "task.completed",
                "payload": {
                    "visible": "yes",
                    "internal_only": "nope",
                },
                "internal_only": "secret",
                "trace_id": "trace-123",
            },
        },
        headers=headers(),
    )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["idempotent"] is False
    assert data["payload"]["subscription_id"] == subscription["subscription_id"]
    assert data["payload"]["workspace_id"] == "workspace-a"
    assert data["payload"]["event_id"] == "evt-1"
    assert data["payload"]["event_type"] == "task.completed"
    assert data["payload"]["payload"] == {"visible": "yes"}
    assert "internal_only" not in data["payload"]
    assert "trace_id" not in data["payload"]


def test_rejected_subscription_filters_block_unknown_fields():
    http = client()
    response = http.post(
        "/api/v2/webhooks/subscriptions",
        json={
            "endpoint": "https://hooks.example.com/a",
            "filters": ["event_id", "not-allowed"],
        },
        headers=headers(),
    )

    assert response.status_code == 400
    assert "Unsupported subscription filters" in response.json()["detail"]


def test_delivery_is_idempotent_for_duplicate_delivery_ids():
    http = client()
    subscription = register_subscription(http)

    first = http.post(
        f"/api/v2/webhooks/subscriptions/{subscription['subscription_id']}/deliver",
        json={
            "endpoint": subscription["endpoint"],
            "delivery_id": "delivery-1",
            "event": {
                "event_id": "evt-1",
                "event_type": "task.started",
                "payload": {"visible": "yes"},
            },
        },
        headers=headers(),
    )
    second = http.post(
        f"/api/v2/webhooks/subscriptions/{subscription['subscription_id']}/deliver",
        json={
            "endpoint": subscription["endpoint"],
            "delivery_id": "delivery-1",
            "event": {
                "event_id": "evt-2",
                "event_type": "task.failed",
                "payload": {"visible": "no"},
            },
        },
        headers=headers(),
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["delivery_id"] == second.json()["delivery_id"]
    assert first.json()["idempotent"] is False
    assert second.json()["idempotent"] is True
    assert second.json()["payload"]["event_id"] == "evt-1"
    assert second.json()["payload"]["event_type"] == "task.started"


def test_workspace_isolation_blocks_other_tenants():
    http = client()
    subscription = register_subscription(http)

    response = http.post(
        f"/api/v2/webhooks/subscriptions/{subscription['subscription_id']}/deliver",
        json={
            "endpoint": subscription["endpoint"],
            "delivery_id": "delivery-1",
            "event": {
                "event_id": "evt-1",
                "event_type": "task.completed",
            },
        },
        headers=headers(workspace_id="workspace-b"),
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Workspace access denied"


def test_disabled_or_rotated_endpoints_are_rejected():
    http = client()
    subscription = register_subscription(http)

    disable_response = http.post(
        f"/api/v2/webhooks/subscriptions/{subscription['subscription_id']}/disable",
        headers=headers(),
    )
    assert disable_response.status_code == 200

    disabled_delivery = http.post(
        f"/api/v2/webhooks/subscriptions/{subscription['subscription_id']}/deliver",
        json={
            "endpoint": subscription["endpoint"],
            "delivery_id": "delivery-1",
            "event": {"event_id": "evt-1", "event_type": "task.completed"},
        },
        headers=headers(),
    )
    assert disabled_delivery.status_code == 410

    webhooks.clear()
    subscription = register_subscription(http)
    rotate_response = http.post(
        f"/api/v2/webhooks/subscriptions/{subscription['subscription_id']}/rotate",
        json={"endpoint": "https://hooks.example.com/b"},
        headers=headers(),
    )
    assert rotate_response.status_code == 200

    stale_delivery = http.post(
        f"/api/v2/webhooks/subscriptions/{subscription['subscription_id']}/deliver",
        json={
            "endpoint": "https://hooks.example.com/a",
            "delivery_id": "delivery-2",
            "event": {"event_id": "evt-2", "event_type": "task.completed"},
        },
        headers=headers(),
    )
    assert stale_delivery.status_code == 410


def test_missing_workspace_context_is_denied():
    http = client()
    response = http.post(
        "/api/v2/webhooks/subscriptions",
        json={
            "endpoint": "https://hooks.example.com/a",
            "filters": ["event_id"],
        },
        headers={"Authorization": "Bearer test-token"},
    )

    assert response.status_code == 401
