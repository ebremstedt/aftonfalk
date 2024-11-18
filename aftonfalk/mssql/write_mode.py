from enum import Enum, auto


class WriteMode(Enum):
    TRUNCATE_WRITE = auto()
    APPEND = auto()
    MERGE = auto()
