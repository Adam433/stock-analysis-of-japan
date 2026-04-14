from __future__ import annotations


# Keep this in sync with `_bmad-output/planning-artifacts/rps-semantics-contract.md`.
APPROVED_RPS_DEFINITION_VERSION = "rps-v1-2026-04-14"
LEGACY_UNRECORDED_RPS_DEFINITION_VERSION = "legacy-unrecorded-pre-rps-versioning"


def normalize_rps_definition_version(version: str | None) -> str:
    return version or LEGACY_UNRECORDED_RPS_DEFINITION_VERSION
