"""Meaningful negative checks for the boundary shared by future adapters."""

import copy
import json

import pytest
from jsonschema import ValidationError

from contracts.api import ROOT, example_validator, load_api, parameter_validator, schema_validator, validate_api_contract, validate_permission_invariants


EXAMPLES = json.loads((ROOT / "examples" / "examples.json").read_text(encoding="utf-8"))


def payload(name):
    return copy.deepcopy(next(example["value"] for example in EXAMPLES if example["name"] == name))


def test_spec_and_published_examples_validate():
    report = validate_api_contract()
    assert report["operations"] >= 25
    assert report["examples"] >= 15


@pytest.mark.parametrize("example", [value for value in EXAMPLES if value["kind"] == "request"], ids=lambda item: item["name"])
def test_unknown_request_fields_are_rejected(example):
    value = copy.deepcopy(example["value"])
    value["unexpected"] = True
    with pytest.raises(ValidationError):
        example_validator(example).validate(value)


@pytest.mark.parametrize("value", ["", "   ", "../item", "item/id", "a" * 129, 42])
def test_external_ids_cannot_be_empty_paths_or_numbers(value):
    with pytest.raises(ValidationError):
        schema_validator("ExternalId").validate(value)


def test_external_ids_preserve_case_and_leading_zeroes():
    for value in ("0007", "CaseSensitive", "commons:1234"):
        schema_validator("ExternalId").validate(value)


def test_wrong_resource_kind_is_rejected():
    value = payload("start-training")
    value["dataset_id"] = "prj_" + "1" * 32
    with pytest.raises(ValidationError):
        schema_validator("TrainingCreateRequest").validate(value)


@pytest.mark.parametrize("name,prefix", [("project_id", "prj"), ("dataset_id", "dts"), ("job_id", "job"), ("model_version", "mdl"), ("deployment_id", "dep"), ("key_id", "key")])
def test_path_parameters_require_the_correct_resource_kind(name, prefix):
    parameter_validator(name).validate(prefix + "_" + "a" * 32)
    for value in ("other_" + "a" * 32, "../secret", "prj_short", 123):
        with pytest.raises(ValidationError):
            parameter_validator(name).validate(value)


def test_cursor_page_and_idempotency_bounds_are_executable():
    for name, values in (("Limit", [0, 101, 1.5]), ("Cursor", ["", "x" * 2049]), ("IdempotencyKey", ["short", "invalid/key", "x" * 129])):
        for value in values:
            with pytest.raises(ValidationError):
                parameter_validator(name).validate(value)


def test_async_job_response_cannot_return_another_resource_kind():
    example = copy.deepcopy(next(value for value in EXAMPLES if value["name"] == "training-accepted"))
    example["value"]["resource_id"] = "prj_" + "1" * 32
    with pytest.raises(ValidationError):
        example_validator(example).validate(example["value"])


@pytest.mark.parametrize("field", ["project_id", "user_id", "session_id"])
def test_demo_identity_cannot_be_selected_in_body(field):
    value = payload("demo-recommend-request")
    value[field] = "someone_else"
    with pytest.raises(ValidationError):
        schema_validator("DemoRecommendationRequest").validate(value)


def test_online_receipt_time_cannot_be_supplied_by_client():
    value = payload("linked-click")
    value["events"][0]["received_at"] = "2026-10-01T12:00:00Z"
    with pytest.raises(ValidationError):
        schema_validator("EventBatchRequest").validate(value)


def test_watch_requires_duration_and_click_cannot_smuggle_it():
    value = payload("linked-click")
    value["events"][0]["event_type"] = "watch"
    with pytest.raises(ValidationError):
        schema_validator("EventBatchRequest").validate(value)
    value["events"][0]["watch_ms"] = 1200
    schema_validator("EventBatchRequest").validate(value)
    value["events"][0]["event_type"] = "click"
    with pytest.raises(ValidationError):
        schema_validator("EventBatchRequest").validate(value)


def test_dataset_requires_exactly_one_items_and_events_file():
    value = payload("declare-dataset")
    value["files"][1]["name"] = "items.csv"
    with pytest.raises(ValidationError):
        schema_validator("DatasetCreateRequest").validate(value)


def test_dataset_paths_cannot_be_selected_by_client():
    value = payload("declare-dataset")
    value["files"][0]["s3_key"] = "another-project/items.csv"
    with pytest.raises(ValidationError):
        schema_validator("DatasetCreateRequest").validate(value)


def test_integration_credentials_cannot_get_training_permission():
    value = payload("issue-key")
    value["scopes"] = ["training:write"]
    with pytest.raises(ValidationError):
        schema_validator("KeyCreateRequest").validate(value)


def test_baseline_cannot_claim_model_probabilities():
    value = payload("baseline-recommendations")
    value["items"][0]["score"] = 0.99
    with pytest.raises(ValidationError):
        schema_validator("RecommendationResponse").validate(value)
    value["items"][0]["score"] = None
    value["model_version"] = "mdl_" + "4" * 32
    with pytest.raises(ValidationError):
        schema_validator("RecommendationResponse").validate(value)


def test_deepfm_response_requires_real_version_and_score():
    value = payload("deepfm-recommendations")
    value["model_version"] = None
    with pytest.raises(ValidationError):
        schema_validator("RecommendationResponse").validate(value)
    value = payload("deepfm-recommendations")
    value["items"][0]["score"] = None
    with pytest.raises(ValidationError):
        schema_validator("RecommendationResponse").validate(value)


def test_recommendations_identify_the_actual_baseline_version():
    value = payload("baseline-recommendations")
    assert value["baseline_version"] == "popularity-v1"
    for invalid in (None, "", "popularity-v1\n"):
        value["baseline_version"] = invalid
        with pytest.raises(ValidationError):
            schema_validator("RecommendationResponse").validate(value)
    value = payload("deepfm-recommendations")
    value["baseline_version"] = "popularity-v1"
    with pytest.raises(ValidationError):
        schema_validator("RecommendationResponse").validate(value)


def test_deepfm_cannot_claim_an_undeployed_generation():
    value = payload("deepfm-recommendations")
    value["deployment_generation"] = 0
    with pytest.raises(ValidationError):
        schema_validator("RecommendationResponse").validate(value)


def test_upload_target_refresh_accepts_only_declared_filename_shapes():
    value = payload("refresh-upload-targets")
    schema_validator("UploadTargetsRequest").validate(value)
    for invalid in ([], ["../items.csv"], ["items.csv", "items.csv"], ["media.json"]):
        value["file_names"] = invalid
        with pytest.raises(ValidationError):
            schema_validator("UploadTargetsRequest").validate(value)
    value = payload("refresh-upload-targets")
    value["sha256"] = "a" * 64
    with pytest.raises(ValidationError):
        schema_validator("UploadTargetsRequest").validate(value)


def test_upload_refresh_is_owner_only_idempotent_and_preserves_checksums():
    operation = load_api()["paths"]["/v1/projects/{project_id}/datasets/{dataset_id}/upload-targets"]["post"]
    assert operation["x-permissions"] == {"actors": ["owner"], "integration_scopes": []}
    assert operation["security"] == [{"DashboardJWT": []}]
    assert {"$ref": "#/components/parameters/IdempotencyKey"} in operation["parameters"]
    assert "original target URLs even after they expire" in operation["description"]
    assert "Expected byte lengths, full-file SHA-256 digests" in operation["description"]


def test_final_newlines_cannot_bypass_exact_patterns():
    with pytest.raises(ValidationError):
        parameter_validator("IdempotencyKey").validate("valid_key\n")
    value = payload("declare-dataset")
    value["files"][0]["sha256"] += "\n"
    with pytest.raises(ValidationError):
        schema_validator("DatasetCreateRequest").validate(value)
    value = payload("key-issued-once")
    value["key"]["masked_key"] += "\n"
    with pytest.raises(ValidationError):
        schema_validator("KeyIssued").validate(value)


@pytest.mark.parametrize("mutation", ["public_resource", "integration_training", "viewer_deployment", "missing_security", "foreign_disclosure"])
def test_contract_checker_rejects_permission_drift(mutation):
    document = load_api()
    project = document["paths"]["/v1/projects/{project_id}"]["get"]
    if mutation == "public_resource":
        project["x-permissions"]["actors"] = ["public"]
        project["security"] = []
    elif mutation == "integration_training":
        job = document["paths"]["/v1/projects/{project_id}/training-jobs"]["post"]
        job["x-permissions"]["actors"].append("integration")
        job["x-permissions"]["integration_scopes"] = ["events:write"]
        job["security"].append({"IntegrationKey": []})
    elif mutation == "viewer_deployment":
        document["paths"]["/v1/projects/{project_id}/deployments"]["post"]["x-permissions"]["actors"].append("viewer")
    elif mutation == "missing_security":
        project["security"] = []
    else:
        document["x-authorization-policy"]["foreign_project_status"] = 403
    with pytest.raises(ValueError):
        validate_permission_invariants(document)


def test_limits_prevent_unbounded_requests():
    value = payload("recommend")
    value["limit"] = 21
    with pytest.raises(ValidationError):
        schema_validator("RecommendationRequest").validate(value)
    value = payload("linked-click")
    value["events"] *= 101
    with pytest.raises(ValidationError):
        schema_validator("EventBatchRequest").validate(value)
    value = payload("recommend")
    value["context"]["feature_vector"] = [1, 2, 3]
    with pytest.raises(ValidationError):
        schema_validator("RecommendationRequest").validate(value)


def test_security_and_permission_matrix_cover_every_operation():
    document = load_api()
    public = set()
    integration = set()
    for path, item in document["paths"].items():
        for method, operation in item.items():
            permissions = operation["x-permissions"]
            actors = permissions["actors"]
            security = operation["security"]
            assert operation["x-reject-unknown-query-parameters"] is True
            assert set(operation["responses"]) >= {"400", "401", "403", "404", "409", "413", "429", "503", "default"}
            if "public" in actors:
                public.add((path, method))
                assert security == []
            else:
                assert security
            if "integration" in actors:
                integration.add(operation["operationId"])
                assert {"IntegrationKey": []} in security
                assert permissions["integration_scopes"]
            if "demo" in actors:
                assert path.startswith("/v1/demo/")
                assert security == [{"DemoSession": []}]
            if "training-jobs" in path or "/deployments" in path:
                assert "integration" not in actors and "demo" not in actors
                if method == "post":
                    assert actors == ["owner"]
    assert public == {("/health", "get"), ("/v1/demo/sessions", "post")}
    assert integration == {"upsert_items", "accept_events", "recommend"}


def test_owner_resource_mutations_require_idempotency_header():
    document = load_api()
    for item in document["paths"].values():
        for method, operation in item.items():
            if method == "post" and operation["x-permissions"]["actors"] == ["owner"]:
                assert {"$ref": "#/components/parameters/IdempotencyKey"} in operation["parameters"]


def test_error_and_request_id_contract_is_consistent():
    document = load_api()
    policy = document["x-authorization-policy"]
    assert policy["foreign_project_status"] == 404
    assert policy["insufficient_member_permission_status"] == 403
    assert policy["membership_required_for_dashboard_jwt"] is True
    for item in document["paths"].values():
        for operation in item.values():
            for status, response in operation["responses"].items():
                if status.isdigit() and int(status) < 400:
                    assert "X-Request-ID" in response["headers"]
