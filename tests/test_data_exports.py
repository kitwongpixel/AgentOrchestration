from fastapi.testclient import TestClient

from src.api.server import create_app
from src.data.exports import export_jobs


def setup_function():
    export_jobs.clear()


def client():
    return TestClient(create_app())


def headers():
    return {"Authorization": "Bearer test-token"}


def test_valid_export_job_is_normalized_and_persisted():
    http = client()
    response = http.post(
        "/api/v2/exports",
        json={
            "date_from": "2026-05-01",
            "date_to": "2026-05-01",
            "workspace": "  workspace-a  ",
            "status_filters": ["Pending", "completed", "pending"],
        },
        headers=headers(),
    )

    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "queued"
    assert data["filters"] == {
        "date_from": "2026-05-01",
        "date_to": "2026-05-01",
        "workspace_id": "workspace-a",
        "statuses": ["pending", "completed"],
    }
    assert export_jobs.queue_size() == 1

    job = export_jobs.get_job(data["job_id"])
    assert job is not None
    assert job.filters.to_dict() == data["filters"]

    fetched = http.get(f"/api/v2/exports/{data['job_id']}", headers=headers())
    assert fetched.status_code == 200
    assert fetched.json() == data

    listed = http.get("/api/v2/exports", headers=headers())
    assert listed.status_code == 200
    assert listed.json()["jobs"] == [data]


def test_invalid_date_range_is_rejected_before_job_creation():
    http = client()
    response = http.post(
        "/api/v2/exports",
        json={
            "date_from": "2026-05-10",
            "date_to": "2026-05-01",
            "workspace_id": "workspace-a",
            "statuses": ["running"],
        },
        headers=headers(),
    )

    assert response.status_code == 422
    assert "date_from must be less than or equal to date_to" in response.json()["detail"]
    assert export_jobs.queue_size() == 0


def test_blank_workspace_id_is_rejected_before_job_creation():
    http = client()
    response = http.post(
        "/api/v2/exports",
        json={
            "date_from": "2026-05-01",
            "date_to": "2026-05-02",
            "workspace_id": "   ",
            "statuses": ["completed"],
        },
        headers=headers(),
    )

    assert response.status_code == 422
    assert "workspace_id must not be blank" in response.json()["detail"]
    assert export_jobs.queue_size() == 0


def test_unsupported_status_rejected_before_job_creation():
    http = client()
    response = http.post(
        "/api/v2/exports",
        json={
            "date_from": "2026-05-01",
            "date_to": "2026-05-02",
            "workspace_id": "workspace-a",
            "statuses": ["completed", "oops"],
        },
        headers=headers(),
    )

    assert response.status_code == 422
    assert "Unsupported export statuses" in response.json()["detail"]
    assert "oops" in response.json()["detail"]
    assert export_jobs.queue_size() == 0


def test_missing_auth_header_is_rejected_before_export_handling():
    http = client()
    response = http.post(
        "/api/v2/exports",
        json={
            "date_from": "2026-05-01",
            "workspace_id": "workspace-a",
            "statuses": ["completed"],
        },
        headers={},
    )

    assert response.status_code == 401

