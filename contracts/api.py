"""Validate the checked-in API contract and examples without a running service."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from jsonschema import Draft202012Validator, FormatChecker
from openapi_spec_validator import validate
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012


ROOT = Path(__file__).resolve().parent
API_PATH = ROOT / "openapi.json"
DATA_PATH = ROOT / "schemas" / "data.schema.json"


def load_api() -> dict:
    return json.loads(API_PATH.read_text(encoding="utf-8"))


def _registry() -> Registry:
    """Register only local documents: missing references fail instead of fetching."""
    registry = Registry()
    for path in (API_PATH, DATA_PATH):
        document = json.loads(path.read_text(encoding="utf-8"))
        resource = Resource.from_contents(document, default_specification=DRAFT202012)
        registry = registry.with_resource(path.as_uri(), resource)
        if "$id" in document:
            registry = registry.with_resource(document["$id"], resource)
    return registry


def schema_validator(name: str) -> Draft202012Validator:
    """Return a validator for an OpenAPI component, with format checks enabled."""
    return Draft202012Validator(
        {"$ref": API_PATH.as_uri() + "#/components/schemas/" + name},
        registry=_registry(),
        format_checker=FormatChecker(),
    )


def parameter_validator(name: str) -> Draft202012Validator:
    """Validate a path, query or header value using the declared parameter."""
    return Draft202012Validator(
        {"$ref": API_PATH.as_uri() + "#/components/parameters/" + name + "/schema"},
        registry=_registry(),
        format_checker=FormatChecker(),
    )


def _local_reference(document: dict, value: dict) -> dict:
    while "$ref" in value:
        reference = value["$ref"]
        if not reference.startswith("#/"):
            raise ValueError("Only document-local operation references are supported")
        value = document
        for part in reference[2:].split("/"):
            value = value[part.replace("~1", "/").replace("~0", "~")]
    return value


def example_validator(example: dict, document: dict | None = None) -> Draft202012Validator:
    document = load_api() if document is None else document
    operation = document["paths"][example["path"]][example["method"]]
    if example["kind"] == "request":
        target = operation["requestBody"]
    elif example["kind"] == "response":
        target = operation["responses"][str(example["status"])]
    else:
        raise ValueError("Example kind must be request or response")
    target = _local_reference(document, target)["content"]["application/json"]["schema"]
    # Absolutize copied references instead of giving the wrapper the document's
    # identity (which would shadow its registered components).
    def with_absolute_references(value):
        if isinstance(value, list):
            return [with_absolute_references(item) for item in value]
        if isinstance(value, dict):
            return {
                key: urljoin(API_PATH.as_uri(), item) if key == "$ref" else with_absolute_references(item)
                for key, item in value.items()
            }
        return value

    return Draft202012Validator(
        with_absolute_references(target),
        registry=_registry(),
        format_checker=FormatChecker(),
    )


def validate_permission_invariants(document: dict) -> None:
    """Reject drift between declared actors, security schemes and key scopes."""
    public_routes = {("/health", "get"), ("/v1/demo/sessions", "post")}
    integration_scopes = {
        "upsert_items": ["catalog:write"],
        "accept_events": ["events:write"],
        "recommend": ["recommendations:read"],
    }
    policy = document["x-authorization-policy"]
    if (
        policy["unauthenticated_status"] != 401
        or policy["foreign_project_status"] != 404
        or policy["insufficient_member_permission_status"] != 403
        or policy["membership_required_for_dashboard_jwt"] is not True
    ):
        raise ValueError("Authorization status/membership policy changed")
    if document["components"]["parameters"]["IdempotencyKey"]["required"] is not True:
        raise ValueError("Idempotency-Key must be required when declared")
    for path, item in document["paths"].items():
        for method, operation in item.items():
            permissions = operation["x-permissions"]
            actors = permissions["actors"]
            if not actors or len(set(actors)) != len(actors) or set(actors) - {"owner", "viewer", "integration", "demo", "public"}:
                raise ValueError("Invalid actors for " + operation["operationId"])
            expected_security = []
            if set(actors) & {"owner", "viewer"}:
                expected_security.append({"DashboardJWT": []})
            if "integration" in actors:
                expected_security.append({"IntegrationKey": []})
                if permissions["integration_scopes"] != integration_scopes.get(operation["operationId"]):
                    raise ValueError("Unsupported integration route or scope")
            elif permissions["integration_scopes"]:
                raise ValueError("Integration scope without integration actor")
            if "demo" in actors:
                if actors != ["demo"] or not path.startswith("/v1/demo/"):
                    raise ValueError("Demo credentials escaped their route boundary")
                expected_security.append({"DemoSession": []})
            if "public" in actors and (actors != ["public"] or (path, method) not in public_routes):
                raise ValueError("Unexpected public route")
            if operation["security"] != expected_security:
                raise ValueError("Actors and security schemes disagree")
            if operation["x-reject-unknown-query-parameters"] is not True:
                raise ValueError("Unknown query parameters must be rejected")
            if "training-jobs" in path or "/deployments" in path:
                if set(actors) - {"owner", "viewer"} or (method == "post" and actors != ["owner"]):
                    raise ValueError("Training/deployment permission boundary changed")
            if method == "post" and actors == ["owner"] and {"$ref": "#/components/parameters/IdempotencyKey"} not in operation["parameters"]:
                raise ValueError("Owner resource mutation lacks idempotency")


def validate_api_contract() -> dict[str, Any]:
    """Check OpenAPI structure, schemas, and every published request/response."""
    document = load_api()
    validate(document, base_uri=API_PATH.as_uri())
    validate_permission_invariants(document)
    for schema in document["components"]["schemas"].values():
        Draft202012Validator.check_schema(schema)
    examples = json.loads((ROOT / "examples" / "examples.json").read_text(encoding="utf-8"))
    names = set()
    for example in examples:
        if example["name"] in names:
            raise ValueError("Duplicate example name: " + example["name"])
        names.add(example["name"])
        example_validator(example, document).validate(example["value"])
    return {
        "openapi_version": document["openapi"],
        "contract_version": document["x-contract-version"],
        "paths": len(document["paths"]),
        "operations": sum(len(item) for item in document["paths"].values()),
        "examples": len(examples),
        "permission_invariants": True,
    }
