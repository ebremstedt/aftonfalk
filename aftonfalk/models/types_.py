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
    source_path: str
    destination_path: str
    metadata_modified_field_enabled: bool = True
    data_modified_field_enabled: bool = False
    source_data_modified_column_name: str = None
    source_data_modified_column_format: str = "YYYY-MM-DD HH:mm:ss.SSS"
    default_columns: Optional[list[Column]] = field(default_factory=list)
    unique_columns: Optional[list[Column]] = field(default_factory=list)
    non_unique_columns: Optional[list[Column]] = field(default_factory=list)
    sensitive_columns: Optional[list[str]] = field(default_factory=list)
    indexes: Optional[list[Index]] = field(default_factory=list)

    _columns: list[Column] = None

    def create_column_list(self):
        non_default_columns = self.unique_columns + self.non_unique_columns
        self._columns = self.default_columns + non_default_columns

    def __post_init__(self):
        data_modified = Column(
            name="data_modified", data_type="DATETIMEOFFSET", constraints="NOT NULL"
        )
        metadata_modified = Column(
                name="metadata_modified",
                data_type="DATETIMEOFFSET",
                constraints="NOT NULL",
            )

        if self.metadata_modified_field_enabled:
            self.default_columns.append(
                metadata_modified
            )

        if self.data_modified_field_enabled:
            self.default_columns.append(
                data_modified
            )
            self.indexes = [
                Index(
                    name="data_modified_nc",
                    index_type=SqlServerIndexType.NONCLUSTERED,
                    columns=[data_modified],
                )
            ]

        self.create_column_list()

    def table_ddl(self) -> str:
        columns_def = [col.column_definition() for col in self._columns]
        indexes_sql = "\n".join(index.to_sql(self.destination_path) for index in self.indexes)

        ddl = (
            f"CREATE TABLE {self.destination_path} (\n  " + ",\n  ".join(columns_def) + ","
            "\n);\n" + indexes_sql
        )

        print("The ddl that will run:\n")
        print(ddl + "\n")

        return ddl

    def insert_sql(self) -> str:
        column_names = ", ".join([col.name for col in self._columns])
        placeholders = ", ".join(["?"] * len(self._columns))
        return f"INSERT INTO {self.destination_path} ({column_names}) VALUES ({placeholders});"


    def read_sql(
        self,
        incremental: bool = False,
        since: str = "",
        until: str = ""
    ) -> str:
        """
        Consider overwriting this function to fit your needs.

        Params:
            incremental: adds where clause to get a subset of rows
            since: format needs to match source
            until: format needs to match source


        """
        sql = []
        select = "SELECT"
        sql.append(select)

        fields = []

        if self.metadata_modified_field_enabled:
            fields.append("SYSDATETIMEOFFSET() as metadata_modified")

        if self.data_modified_field_enabled:
            fields.append(f"{self.source_data_modified_column_name} as data_modified")

        fields.append("*")

        sql.append(",\n".join(fields))

        sql.append(f"FROM {self.source_path}")

        if incremental:
            sql.append(f"WHERE '{since}' < {self.source_data_modified_column_name} AND {self.source_data_modified_column_name} > '{until}'")

        sql_string = "\n".join(sql)

        print("The query that will run:\n")
        print(sql_string + "\n")

        return sql_string
