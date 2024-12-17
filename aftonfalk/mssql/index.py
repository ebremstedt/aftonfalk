from dataclasses import dataclass
from enum import Enum, auto
from typing import List
from aftonfalk.mssql.column import Column
from aftonfalk.mssql.path import Path


class SortDirection(Enum):
    ASC = "ASC"
    DESC = "DESC"


class SqlServerIndexType(Enum):
    CLUSTERED = auto()
    NONCLUSTERED = auto()
    UNIQUE = auto()
    FULLTEXT = auto()
    XML = auto()
    SPATIAL = auto()
    FILTERED = auto()


@dataclass
class Index:
    """
    Represents an SQL Server index definition.

    This class is used to generate SQL statements for creating indexes on SQL Server tables. It supports various
    SQL Server index types, allows configuration of columns, uniqueness, and sorting order.

    Attributes:
        index_type (SqlServerIndexType): The type of the index (e.g., CLUSTERED, NONCLUSTERED, UNIQUE, etc.).
        columns (List[Column]): A list of `Column` objects representing the columns included in the index.
        is_unique (bool): Indicates whether the index is unique. Defaults to False.
        sort_direction (SortDirection): Specifies the sort direction (ASC or DESC) for all columns in the index.
                                         Defaults to ASC.

    Raises:
        ValueError: If any attributes are invalid during initialization (e.g., empty columns list, invalid index_type).

    """

    index_type: SqlServerIndexType
    columns: List[Column]
    is_unique: bool = False
    sort_direction: SortDirection = SortDirection.ASC

    def __post_init__(self):
        self.validate_index_type()
        self.validate_columns()
        self.validate_sort_direction()

    def validate_index_type(self):
        """Ensure index_type is a valid SqlServerIndexType."""
        if not isinstance(self.index_type, SqlServerIndexType):
            raise ValueError(
                f"Invalid index_type: {self.index_type}. Must be a SqlServerIndexType."
            )

    def validate_columns(self):
        """Ensure columns is a non-empty list of Column objects."""
        if not self.columns or not isinstance(self.columns, list):
            raise ValueError("Columns must be a non-empty list of Column objects.")
        if not all(isinstance(col, Column) for col in self.columns):
            raise ValueError("All elements in columns must be of type Column.")

    def validate_sort_direction(self):
        """Ensure sort_direction is a valid SortDirection."""
        if not isinstance(self.sort_direction, SortDirection):
            raise ValueError(
                f"Invalid sort_direction: {self.sort_direction}. Must be a SortDirection."
            )

    def index_name(self, path: Path) -> str:
        """Generate the name for the index."""
        index_columns_snake = "_".join(f"{col.name}" for col in self.columns)
        return f"{path.table}_{index_columns_snake}"

    def to_sql(self, path: Path) -> str:
        """Generate the SQL statement for creating the index."""
        unique_clause = "UNIQUE " if self.is_unique else ""
        index_columns = ", ".join(
            f"{col.name} {self.sort_direction.value}" for col in self.columns
        )
        return (
            f"CREATE {unique_clause}{self.index_type.name} INDEX "
            f"{self.index_name(path=path)} ON {path.to_str()} ({index_columns})"
        )
