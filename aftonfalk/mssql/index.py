from dataclasses import dataclass
from enum import Enum, auto
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
    index_type: SqlServerIndexType
    columns: list[Column]
    is_unique: bool = False
    sort_direction: SortDirection = SortDirection.ASC

    def index_name(self, path: Path) -> str:
        index_columns_snake = "_".join(f"{col.name}" for col in self.columns)
        return f"{path.table}_{index_columns_snake}"

    def to_sql(self, path: Path) -> str:
        unique_clause = "UNIQUE " if self.is_unique else ""
        index_columns = ", ".join(
            f"{col.name} {self.sort_direction.value}" for col in self.columns
        )

        # Index names are unique so have to use table prefix here
        index_str = f"CREATE {unique_clause}{self.index_type.name} INDEX {self.index_name(path=path)} ON {path.to_str()} ({index_columns})"
        return index_str
