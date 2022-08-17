from enum import Enum


class WriteMode(str, Enum):
    overwrite = 'overwrite'
    append = 'append'
