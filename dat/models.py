from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel

OUT_ROOT = Path('./out/reader_tests/generated/')


class TableMetadata(BaseModel):
    name: str
    description: str
    check_versions: List[int]

    @property
    def root(self) -> Path:
        return OUT_ROOT / self.name

    @property
    def delta_root(self) -> str:
        return str(self.root / 'delta')

    def expected_root(self, version: Optional[int] = None) -> Path:
        version_path = 'latest' if version is None else f'v{version}'
        return self.root / 'expected' / version_path

    def expected_path(self, version: Optional[int] = None) -> str:
        return str(self.expected_root(version) / 'table_content.parquet')


class ExpectedMetadata(BaseModel):
    config: Dict[str, str]
    min_reader_version: int
    min_writer_version: int
    table_schema_json: str
