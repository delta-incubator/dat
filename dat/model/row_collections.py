from enum import Enum
from typing import List, Tuple

from pydantic import BaseModel


class WriteMode(str, Enum):
    overwrite = 'overwrite'
    append = 'append'


class RowCollection(BaseModel):
    write_mode: WriteMode
    data: List[Tuple]
