from __future__ import annotations

import logging
import os
from pathlib import Path

from dagster_dbt import DbtProject
from dagster_dbt.dbt_project import DagsterDbtProjectPreparer
from dagster_dbt.errors import DagsterDbtManifestNotFoundError

from oddsfox_pipeline.config._env import _env_bool
from oddsfox_pipeline.config.settings import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    resolve_dbt_executable,
)

logger = logging.getLogger(__name__)

_INPUT_SUFFIXES = {".sql", ".yml", ".yaml", ".csv", ".md"}
_INPUT_ROOT_FILES = (
    "dbt_project.yml",
    "packages.yml",
    "dependencies.yml",
    "package-lock.yml",
)
_INPUT_DIRS = ("models", "macros", "seeds", "tests")
_SKIP_DIR_NAMES = {"target", "logs", "dbt_packages", "__pycache__"}


def resolve_dbt_target_path() -> Path:
    """Align Dagster manifest prep with Make/dbt ``DBT_TARGET_PATH`` when set."""
    raw = os.getenv("DBT_TARGET_PATH")
    if not raw:
        return Path("target")
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (DBT_PROJECT_DIR / path).resolve()


def dbt_manifest_inputs_stale(project_dir: Path, manifest_path: Path) -> bool:
    """True when ``manifest.json`` is missing, empty, or older than dbt inputs."""
    if not manifest_path.is_file() or manifest_path.stat().st_size < 1:
        return True
    manifest_mtime = manifest_path.stat().st_mtime
    project_dir = project_dir.resolve()
    target_resolved = resolve_dbt_target_path()
    newest = 0.0
    for name in _INPUT_ROOT_FILES:
        path = project_dir / name
        if path.is_file():
            newest = max(newest, path.stat().st_mtime)
    for dirname in _INPUT_DIRS:
        root = project_dir / dirname
        if not root.is_dir():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in _INPUT_SUFFIXES:
                continue
            if any(part in _SKIP_DIR_NAMES for part in path.parts):
                continue
            try:
                resolved = path.resolve()
            except OSError:
                continue
            if resolved.is_relative_to(target_resolved):
                continue
            newest = max(newest, path.stat().st_mtime)
    return newest > manifest_mtime


class OddsfoxDbtProjectPreparer(DagsterDbtProjectPreparer):
    """Use the repo-resolved dbt executable during Dagster dev manifest prep."""

    def prepare_if_dev(self, project: DbtProject) -> None:
        if not self.using_dagster_dev():
            return
        force = _env_bool("ODDSFOX_DBT_FORCE_PREPARE", False)
        if not force and not dbt_manifest_inputs_stale(
            Path(project.project_dir), project.manifest_path
        ):
            return
        self.prepare(project)
        if not project.manifest_path.exists():
            raise DagsterDbtManifestNotFoundError(
                f"Did not find manifest.json at expected path {project.manifest_path} "
                f"after running '{self.prepare.__qualname__}'. Ensure the implementation respects "
                "all DbtProject properties."
            )

    def _dbt_cli(self, project: DbtProject):
        from dagster_dbt.core.resource import DbtCliResource

        return DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROFILES_DIR),
            profile="oddsfox",
            target="dev",
            dbt_executable=resolve_dbt_executable(),
        )

    def _prepare_packages(self, project: DbtProject) -> None:
        self._dbt_cli(project).cli(
            ["deps", "--quiet"], target_path=project.target_path
        ).wait()

    def _prepare_manifest(self, project: DbtProject) -> None:
        self._dbt_cli(project).cli(
            [
                "parse",
                "--quiet",
                "--profiles-dir",
                str(DBT_PROFILES_DIR),
                "--profile",
                "oddsfox",
                "--target",
                "dev",
            ],
            target_path=project.target_path,
        ).wait()
        # ponytail: skip seed checksum poison; ceiling = relocated project_dir
        # with stale seed root_path; upgrade = wipe DBT_TARGET_PATH / make dbt-prepare.


_ODDSFOX_DBT_PREPARER = OddsfoxDbtProjectPreparer()


def prepare_dbt_project(
    project: DbtProject,
    *,
    preparer: DagsterDbtProjectPreparer | None = None,
) -> None:
    active_preparer = preparer or _ODDSFOX_DBT_PREPARER
    if active_preparer.using_dagster_dev():
        try:
            active_preparer.prepare_if_dev(project)
        except Exception:
            if not project.manifest_path.exists():
                raise
            logger.warning(
                "Using existing dbt manifest at %s after prepare_if_dev() failed",
                project.manifest_path,
                exc_info=True,
            )
    elif not project.manifest_path.exists():
        active_preparer.prepare(project)


DBT_PROJECT = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    profile="oddsfox",
    target="dev",
    target_path=resolve_dbt_target_path(),
    prepare_project_cli_args=[
        "parse",
        "--quiet",
        "--profiles-dir",
        str(DBT_PROFILES_DIR),
    ],
)
prepare_dbt_project(DBT_PROJECT)

DBT_DAGSTER_GROUP_NAME = "analytics"


__all__ = [
    "DBT_DAGSTER_GROUP_NAME",
    "DBT_PROJECT",
    "OddsfoxDbtProjectPreparer",
    "dbt_manifest_inputs_stale",
    "prepare_dbt_project",
    "resolve_dbt_target_path",
]
