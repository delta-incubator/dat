from enum import Enum
from typing import List, Tuple

from pydantic import BaseModel


class WriteMode(str, Enum):  # noqa: WPS600
    overwrite = 'overwrite'
    append = 'append'


class RowCollection(BaseModel):
    write_mode: WriteMode
    rows: List[Tuple]
