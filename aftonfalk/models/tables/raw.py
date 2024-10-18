from aftonfalk.models.types_ import Table, Column, Index
from aftonfalk.models.enums_ import SqlServerIndexType, SqlServerDataType
from dataclasses import dataclass
from typing import Optional

varchar_fifty = SqlServerDataType.VARCHAR.with_length(50)


@dataclass
class RawTable(Table):
    source_modified_column: Optional[str] = None

    def __post_init__(self):
        data_modified_column = Column(
            name="data_modified", data_type="DATETIMEOFFSET", constraints="NOT NULL"
        )
        self.default_columns = [
            data_modified_column,
            Column(
                name="metadata_modified",
                data_type="DATETIMEOFFSET",
                constraints="NOT NULL",
            ),
            Column(name="data", data_type="NVARCHAR(MAX)", constraints="NOT NULL"),
        ]
        self.indexes = [
            Index(
                name="woo",
                index_type=SqlServerIndexType.NONCLUSTERED,
                columns=[data_modified_column],
            )
        ]
        super().set_default_attributes()

    def table_ddl(self, path: str) -> str:
        columns_def = [col.column_definition() for col in self._columns]
        indexes_sql = "\n".join(index.to_sql(path) for index in self.indexes)

        return (
            f"CREATE TABLE {path} (\n  " + ",\n  ".join(columns_def) + ","
            "\n);\n" + indexes_sql
        )

    def insert_sql(self, path: str) -> str:
        column_names = ", ".join([col.name for col in self._columns])
        placeholders = ", ".join(["?"] * len(self._columns))
        return f"INSERT INTO {path} ({column_names}) VALUES ({placeholders});"
