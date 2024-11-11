from dataclasses import dataclass, field
from typing import Optional
from aftonfalk.mssql.enums_ import (
    SqlServerIndexType,
    SortDirection,
    SqlServerTimeZone,
    WriteMode,
)
import re
from pendulum import now


class InvalidPathException(Exception):
    pass


@dataclass
class Path:
    database: str
    schema: str
    table: str

    def to_str(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"


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

@dataclass
class Table:
    """
    Parameters
        source_path: Source table location.
        destination_path: Desired destination table location.
        source_data_modified_column_name: The name of the field that indicates when a row was modified
        destination_data_modified_column_name: self explanatory
        temp_table_path: Location of temp table, only applicable with WriteMode.MERGE
        enforce_primary_key: Should uniqueness be enforced or not via primary key
        timezone: Timezone to use for timestamps
        write_mode: How you want to write to the table. Available modes:
            TRUNCATE_WRITE
            APPEND
            MERGE
        batch_size: The number of rows to insert

        default_columns: Columns that you want to be default for the table
        unique_columns: Columns which make a row unique in the table
        non_unique_columns: The rest of the columns
        indexes: Any indexes you want the table to use
    """

    source_path: Path
    destination_path: Path
    source_data_modified_column_name: str = None
    destination_data_modified_column_name: str = "data_modified"
    temp_table_schema: str = "INTERNAL"
    enforce_primary_key: bool = False
    timezone: SqlServerTimeZone = SqlServerTimeZone.UTC
    write_mode: WriteMode = WriteMode.APPEND
    batch_size: int = 1000

    temp_table_path: Path = None
    default_columns: Optional[list[Column]] = field(default_factory=list)
    unique_columns: Optional[list[Column]] = field(default_factory=list)
    non_unique_columns: Optional[list[Column]] = field(default_factory=list)
    indexes: Optional[list[Index]] = field(default_factory=list)

    _columns: list[Column] = None

    def create_column_list(self):
        non_default_columns = self.unique_columns + self.non_unique_columns
        self._columns = self.default_columns + non_default_columns

    def path_is_valid(self, string: str) -> bool:
        pattern = r"^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+){2}$"

        if re.match(pattern, string):
            return True
        return False

    def valid_batch_size(self) -> bool:
        return 0 < self.batch_size < 50001

    def __post_init__(self):
        self.create_column_list()
        self.temp_table_path=Path(
            database=self.destination_path.database,
            schema=self.temp_table_schema,
            table=f"{self.destination_path.table}_{now().format('YYMMDDHHmmss')}"
        )
        if not self.valid_batch_size():
            raise ValueError("Batch size needs to be between (including) 1 and 50000")

    def join_columns_by(self, columns: list[Column], separator: str = ","):
        if len(columns) == 0:
            return ""
        return separator.join([col.name for col in columns])

    def table_ddl(self, path: Path) -> str:

        ddl = [f"CREATE TABLE {path.to_str()} ("]

        columns_def = [col.column_definition() for col in self._columns]

        ddl_parts = columns_def

        if self.enforce_primary_key:
            pk_name = "_".join(col.name for col in self.unique_columns)
            pk_definition = ", ".join(col.name for col in self.unique_columns)
            ddl_parts.append(
                f"CONSTRAINT PK_{pk_name}_{now().format("YYMMDDHHmmss")} PRIMARY KEY ({pk_definition})"
            )

        ddl.append(",\n".join(ddl_parts))

        ddl.append(");")

        table_ddl_str = "\n".join(ddl)

        return table_ddl_str

    def insert_sql(self, path: Path) -> str:
        column_names = ", ".join([col.name for col in self._columns])
        placeholders = ", ".join(["?"] * len(self._columns))
        return f"INSERT INTO {path.to_str()} ({column_names}) VALUES ({placeholders});"

    def read_sql(self, since: Optional[str] = None, until: Optional[str] = None) -> str:
        """
        Construct a read sql statement.
        Consider overwriting this function to fit your needs.

        Params:
            since: format needs to match source
            until: format needs to match source

        Returns:
            str
        """
        sql = ["SELECT"]

        fields = []
        tz_info = f"AT TIME ZONE '{self.timezone.value}'"
        fields.append(f"SYSDATETIMEOFFSET() {tz_info} as metadata_modified")
        if self.source_data_modified_column_name:
            fields.append(
                f"""CAST({self.source_data_modified_column_name} AS DATETIME) {tz_info} AS data_modified"""
            )
        fields.append("*")
        sql.append(",\n".join(fields))

        sql.append(f"FROM {self.source_path.to_str()}")

        if since and until:
            sql.append(
                f"WHERE '{since}' <= {self.source_data_modified_column_name} AND {self.source_data_modified_column_name} < '{until}'"
            )

        sql_string = "\n".join(sql)

        return sql_string

    def has_sensitive_columns(self) -> bool:
        for column in self._columns:
            if column.sensitive:
                return True
        return False
