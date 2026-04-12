from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class PipelineFile:
    file_key: str
    snapshot: str
    entity: str
    staged_file_path: Path