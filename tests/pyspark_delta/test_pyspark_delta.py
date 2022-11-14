import json
from pathlib import Path
from typing import List, NamedTuple, Optional

import chispa
import pytest

TEST_ROOT = Path('../out/reader_tests/')


class ReadCase(NamedTuple):
    delta_root: Path
    version: Optional[int]
    parquet_root: Path
    name: str
    description: str
    min_reader_version: int
    min_writer_version: int


cases: List[ReadCase] = []

for path in (TEST_ROOT / 'generated').iterdir():
    if path.is_dir():
        with open(path / 'table_metadata.json') as f:
            case_metadata = json.loads(f)

        for version_path in (path / 'expected').iterdir():
            if version_path.is_dir():
                if version_path.name[0] == 'v':
                    version = int(version_path.name[1:])
                elif version_path.name == 'latest':
                    version = None
                else:
                    continue

                with open(version_path / 'expected_metadata.json') as f:
                    expected_metadata = json.loads(f)

                case = ReadCase(
                    delta_root=path / 'delta',
                    version=version,
                    parquet_root=version_path,
                    name=case_metadata['name'],
                    description=case_metadata['description'],
                    min_reader_version=expected_metadata['min_reader_version'],
                    min_writer_version=expected_metadata['min_writer_version'],
                )


@pytest.mark.parameterize('case', cases)
def test_readers_dat(spark, case: ReadCase):
    # TODO: is there a way to get the protocol versions of a specific version
    # of the table?
    query = spark.read.format('delta')
    if case.version:
        query = query.option('versionAsOf', case.version)
    actual_df = query.load(str(case.delta_root))
    expected_df = spark.read.format('parquet').load(str(case.parquet_root))

    chispa.assert_df_equality(actual_df, expected_df)
