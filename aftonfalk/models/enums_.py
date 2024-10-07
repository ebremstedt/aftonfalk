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


class SqlServerDataType(Enum):
    """
    SqlServerDataType.VARCHAR.with_length(255) # Output: VARCHAR(255)
    SqlServerDataType.DECIMAL.with_precision(10, 2) # Output: DECIMAL(10, 2)
    SqlServerDataType.DATETIME2.with_precision(3) # Output: DATETIME2(3)
    """

    TINYINT = auto()
    SMALLINT = auto()
    INT = auto()
    BIGINT = auto()
    FLOAT = auto()
    REAL = auto()
    DECIMAL = auto()
    NUMERIC = auto()
    MONEY = auto()
    SMALLMONEY = auto()
    CHAR = auto()
    VARCHAR = auto()
    TEXT = auto()
    NCHAR = auto()
    NVARCHAR = auto()
    NTEXT = auto()
    BINARY = auto()
    VARBINARY = auto()
    IMAGE = auto()
    DATE = auto()
    DATETIME = auto()
    DATETIME2 = auto()
    DATETIMEOFFSET = auto()
    SMALLDATETIME = auto()
    TIME = auto()
    UNIQUEIDENTIFIER = auto()
    XML = auto()
    CURSOR = auto()
    TABLE = auto()
    GEOMETRY = auto()
    GEOGRAPHY = auto()

    def with_length(self, length: int) -> str:
        """Return the data type with a specified length."""
        return f"{self.name}({length})"

    def with_precision(self, precision: int, scale: Optional[int] = None) -> str:
        """Return the data type with precision and optionally scale."""
        if scale is not None:
            return f"{self.name}({precision}, {scale})"
        return f"{self.name}({precision})"
