from typing import List, Optional

from pydantic import BaseModel


class TableMetadata(BaseModel):
    features: Optional[List[str]]
    writer_protocol_version: int
    reader_protocol_version: int
