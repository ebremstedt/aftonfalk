from dataclasses import dataclass, field
from typing import Optional
from aftonfalk.models.enums_ import SqlServerIndexType, SortDirection


@dataclass
class Column:
    name: str
    data_type: str
    constraints: str = ""
    description: str = ""
    sensitive: bool = False

    def column_definition(self) -> str:
        return f"{self.name} {self.data_type} {self.constraints}".strip()


@dataclass
class Index:
    name: str
    index_type: SqlServerIndexType
    columns: list[Column]
    is_unique: bool = False
    sort_direction: SortDirection = SortDirection.ASC

    def to_sql(self, path: str) -> str:
        unique_clause = "UNIQUE " if self.is_unique else ""
        index_columns = ", ".join(
            f"{col.name} {self.sort_direction.value}" for col in self.columns
        )
        index_columns_snake = "_".join(f"{col.name}" for col in self.columns)

        return f"CREATE {unique_clause}{self.index_type.name} INDEX {index_columns_snake} ON {path} ({index_columns});"


@dataclass
class Table:
    default_columns: Optional[list[Column]] = field(default_factory=list)
    unique_columns: Optional[list[Column]] = field(default_factory=list)
    non_unique_columns: Optional[list[Column]] = field(default_factory=list)
    sensitive_columns: Optional[list[str]] = field(default_factory=list)
    indexes: Optional[list[Index]] = field(default_factory=list)

    _columns: list[Column] = None
    default_columns_str_comma: Optional[str] = None
    unique_columns_str_comma: Optional[str] = None
    non_unique_columns_str_comma: Optional[str] = None
    sensitive_columns_str_comma: Optional[str] = None
    default_columns_str_underscore: Optional[str] = None
    unique_columns_str_underscore: Optional[str] = None
    non_unique_columns_str_underscore: Optional[str] = None
    sensitive_columns_str_comma: Optional[str] = None

    def create_column_list(self):
        non_default_columns = self.unique_columns + self.non_unique_columns
        self._columns = self.default_columns + non_default_columns

    def str_comma(self, input_list: list[Column]):
        if len(input_list) == 0:
            return ""
        return ",".join([col.name for col in input_list])

    def str_underscore(self, input_list: list[Column]):
        if len(input_list) == 0:
            return ""
        return "_".join([col.name for col in input_list])

    def set_default_attributes(self):
        self.create_column_list()
        self.default_columns_str_comma

    def __post_init__(self):
        self.set_default_attributes()
