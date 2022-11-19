import json
from pathlib import Path
from typing import List, NamedTuple, Optional

import chispa
import pytest

TEST_ROOT = Path('out/reader_tests/')

MAX_SUPPORTED_READER_VERSION = 2


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
        with open(path / 'test_case_info.json') as f:
            case_metadata = json.load(f)

        for version_path in (path / 'expected').iterdir():
            if version_path.is_dir():
                if version_path.name[0] == 'v':
                    version = int(version_path.name[1:])
                elif version_path.name == 'latest':
                    version = None
                else:
                    continue

                with open(version_path / 'table_version_metadata.json') as f:
                    expected_metadata = json.load(f)

                case = ReadCase(
                    delta_root=path / 'delta',
                    version=version,
                    parquet_root=version_path,
                    name=case_metadata['name'],
                    description=case_metadata['description'],
                    min_reader_version=expected_metadata['min_reader_version'],
                    min_writer_version=expected_metadata['min_writer_version'],
                )
                cases.append(case)


@pytest.mark.parametrize('case', cases,
                         ids=lambda
                         case: f'{case.name} (version={case.version})')
def test_readers_dat(spark_session, case: ReadCase):
    query = spark_session.read.format('delta')
    if case.version is not None:
        query = query.option('versionAsOf', case.version)

    if case.min_reader_version > MAX_SUPPORTED_READER_VERSION:
        # If it's a reader version we don't support, assert failure
        with pytest.raises(Exception):
            query.load(str(case.delta_root))
    else:
        actual_df = query.load(str(case.delta_root))

        expected_df = spark_session.read.format('parquet').load(
            str(case.parquet_root) + '/*.parquet')

        chispa.assert_df_equality(actual_df, expected_df)
