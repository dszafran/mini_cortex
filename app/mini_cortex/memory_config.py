"""The packet taxonomy and pipeline knobs from `memory_types.yaml` (override the path with MINI_CORTEX_TYPES)."""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

APP_DIR = Path(__file__).resolve().parents[1]


class TypeDef(BaseModel):
    name: str
    definition: str
    example: str = ""
    enabled: bool = True


class PipelineKnobs(BaseModel):
    theta: float = Field(0.86, ge=0, le=1)
    neighbours: int = 5
    buffer_messages: int = 5
    buffer_seconds: float = 20
    window_gap_minutes: float = 240
    window_max_items: int = 50
    context_items: int = 10
    chunk_chars: int = 24000
    chunk_overlap: int = 2000
    parallel_windows: int = 6


class MemoryConfig(BaseModel):
    types: list[TypeDef]
    pipeline: PipelineKnobs = PipelineKnobs()

    @property
    def enabled(self) -> list[TypeDef]:
        return [t for t in self.types if t.enabled]

    def type_names(self) -> list[str]:
        return [t.name for t in self.enabled]


def types_path() -> Path:
    return Path(os.environ.get("MINI_CORTEX_TYPES") or APP_DIR / "memory_types.yaml")


def load_memory_config(path: Path | None = None) -> MemoryConfig:
    return MemoryConfig.model_validate(yaml.safe_load((path or types_path()).read_text()))


def save_memory_config(cfg: MemoryConfig, path: Path | None = None) -> None:
    """Rewrites only the values, keeping the file's comments where the structure allows it: the header comment is
    preserved, the rest is regenerated."""
    path = path or types_path()
    header = []
    for line in path.read_text().splitlines() if path.is_file() else []:
        if not line.startswith("#"):
            break
        header.append(line)
    body = yaml.safe_dump(cfg.model_dump(), sort_keys=False, allow_unicode=True, width=120)
    path.write_text("\n".join(header) + ("\n\n" if header else "") + body)
