from dataclasses import dataclass
from aftonfalk.models.types_ import Table, Column


@dataclass
class Generic(Table):
    source_path: str
    destination_path: str

    def __post_init__(self):
        self.default_columns = [
            Column(
                name="metadata_modified",
                data_type="DATETIMEOFFSET",
                constraints="NOT NULL",
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


    def read_sql() -> str:
        """ This is empty by design and should be replaced """
        pass
