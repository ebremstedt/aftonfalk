from aftonfalk.models.types_ import Table, Column, Index
from aftonfalk.models.enums_ import SqlServerIndexType
from dataclasses import dataclass
from pendulum import now


@dataclass
class CleanTable(Table):
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
        ]
        self.indexes = [
            Index(
                name="woo",
                index_type=SqlServerIndexType.NONCLUSTERED,
                columns=[data_modified_column],
            )
        ]
        super().set_default_attributes()

    def table_ddl(self, path: str, primary_key: bool = True) -> str:
        default_columns_def = [col.column_definition() for col in self.default_columns]
        unique_columns_def = [col.column_definition() for col in self.unique_columns]
        non_unique_columns_def = [
            col.column_definition()
            for col in self.non_unique_columns
            if col.name not in [col.name for col in self.unique_columns]
        ]
        indexes_sql = "\n".join(index.to_sql(path) for index in self.indexes)
        pk_name = "_".join(col.name for col in self.unique_columns)
        pk_definition = ", ".join(col.name for col in self.unique_columns)
        right_now_str = now().format("YYMMDDHHmmss")

        sql = (
            f"CREATE TABLE {path} (\n"
            + ",\n".join(default_columns_def)
            + ",\n"
            + ",\n".join(unique_columns_def)
            + ",\n"
            + ",\n".join(non_unique_columns_def)
            + ",\n"
            + f"CONSTRAINT PK_{pk_name}_{right_now_str} PRIMARY KEY ({pk_definition})"
            + "\n);\n"
            + indexes_sql
        )

        if not primary_key:
            sql = (
                f"CREATE TABLE {path} (\n"
                + ",\n".join(default_columns_def)
                + ",\n"
                + ",\n".join(unique_columns_def)
                + ",\n"
                + ",\n".join(non_unique_columns_def)
                + "\n);\n"
                + indexes_sql
            )

        return sql

    def insert_sql(self, path: str) -> str:
        column_names = ", ".join([col.name for col in self._columns])
        placeholders = ", ".join(["?" for column in self._columns])
        return f"INSERT INTO {path} ({column_names}) VALUES ({placeholders});"

    def read_raw(self, path: str) -> str:
        return f"""SELECT *
        FROM (
            SELECT
                CAST(data_modified AS VARCHAR(50)) AS data_modified,
                CAST(metadata_modified AS VARCHAR(50)) AS metadata_modified,
                data,
                ROW_NUMBER() OVER (PARTITION BY {self.str_comma(input_list=self.unique_columns)} ORDER BY data_modified DESC) AS rn
            FROM {path}
            -- Remove line below for real data
            WHERE {self.str_comma(input_list=self.unique_columns)} <> 'NULL'
        ) AS THE_TABLE_WHO_MUST_BE_NAMED
        WHERE rn = 1;
        """

    def read_raw_incremental(self, path: str) -> str:
        self.str_comma(input_list=self.unique_columns)

        return f"""SELECT *
        FROM (
            SELECT
                CAST(data_modified AS VARCHAR(50)) AS data_modified,
                CAST(metadata_modified AS VARCHAR(50)) AS metadata_modified,
                data,
                ROW_NUMBER() OVER (PARTITION BY {self.str_comma(input_list=self.unique_columns)} ORDER BY data_modified DESC) AS rn

            FROM {path}
            WHERE data_modified > ? and ? < data_modified
                 -- Remove line below for real data
                AND {self.str_comma(input_list=self.unique_columns)} <> 'NULL'
        ) AS THE_TABLE_WHO_MUST_BE_NAMED
        WHERE rn = 1;
        """

    def has_sensitive_columns(self) -> bool:
        for column in self._columns:
            if column.sensitive:
                return True
        return False
