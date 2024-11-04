from enum import Enum, auto
from typing import Optional


class SqlServerIndexType(Enum):
    CLUSTERED = auto()
    NONCLUSTERED = auto()
    UNIQUE = auto()
    FULLTEXT = auto()
    XML = auto()
    SPATIAL = auto()
    FILTERED = auto()


class SortDirection(Enum):
    ASC = "ASC"
    DESC = "DESC"


class WriteMode(Enum):
    TRUNCATE_WRITE = auto()
    APPEND = auto()
    MERGE = auto()


class SqlServerDataType(Enum):
    """
    SqlServerDataType.VARCHAR.with_length(255) # Output: VARCHAR(255)
    SqlServerDataType.DECIMAL.with_precision(10, 2) # Output: DECIMAL(10, 2)
    SqlServerDataType.DATETIME2.with_precision(3) # Output: DATETIME2(3)
    """

    BIGINT = auto()
    BINARY = auto()
    CHAR = auto()
    CURSOR = auto()
    DATE = auto()
    DATETIME = auto()
    DATETIME2 = auto()
    DATETIMEOFFSET = auto()
    DECIMAL = auto()
    FLOAT = auto()
    GEOGRAPHY = auto()
    GEOMETRY = auto()
    IMAGE = auto()
    INT = auto()
    MONEY = auto()
    NCHAR = auto()
    NUMERIC = auto()
    NVARCHAR = auto()
    REAL = auto()
    SMALLDATETIME = auto()
    SMALLINT = auto()
    SMALLMONEY = auto()
    TABLE = auto()
    TIME = auto()
    TINYINT = auto()
    UNIQUEIDENTIFIER = auto()
    VARBINARY = auto()
    VARCHAR = auto()
    XML = auto()

    def with_length(self, length: int) -> str:
        """Return the data type with a specified length."""
        return f"{self.name}({length})"

    def with_precision(self, precision: int, scale: Optional[int] = None) -> str:
        """Return the data type with precision and optionally scale."""
        if scale is not None:
            return f"{self.name}({precision}, {scale})"
        return f"{self.name}({precision})"


class SqlServerTimeZone(Enum):
    UTC = "UTC"
    CENTRAL_EUROPEAN_STANDARD_TIME = "Central European Standard Time"
