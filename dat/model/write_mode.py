from enum import Enum


class WriteMode(str, Enum):  # noqa: WPS600
    overwrite = 'overwrite'
    append = 'append'
