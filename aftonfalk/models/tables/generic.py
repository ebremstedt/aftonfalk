from dataclasses import dataclass
from aftonfalk.models.types_ import Table, Column


@dataclass
class Generic(Table):
    destination_path: str = None
    source_sql: str = None

    def __post_init__(self):
        self.default_columns = [
            Column(
                name="metadata_modified",
                data_type="DATETIMEOFFSET",
                constraints="NOT NULL",
            )
        ]

        super().set_default_attributes()

    def table_ddl(self) -> str:
        columns_def = [col.column_definition() for col in self._columns]
        indexes_sql = "\n".join(index.to_sql(self.destination_path) for index in self.indexes)

        return (
            f"CREATE TABLE {self.destination_path} (\n  " + ",\n  ".join(columns_def) + ","
            "\n);\n" + indexes_sql
        )

    def insert_sql(self) -> str:
        column_names = ", ".join([col.name for col in self._columns])
        placeholders = ", ".join(["?"] * len(self._columns))
        return f"INSERT INTO {self.destination_path} ({column_names}) VALUES ({placeholders});"
