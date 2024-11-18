from dataclasses import dataclass, field
from pendulum import now
import re
from typing import Optional
from aftonfalk.mssql.timezone import SqlServerTimeZone
from aftonfalk.mssql.data_type import DataType
from aftonfalk.mssql.index import Index
from aftonfalk.mssql.path import Path
from aftonfalk.mssql.column import Column
from aftonfalk.mssql.write_mode import WriteMode


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
        fast_executemany: pyodbc setting for bulk inserts, defaults to False
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
    fast_executemany: bool = False
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
        self.temp_table_path = Path(
            database=self.destination_path.database,
            schema=self.temp_table_schema,
            table=f"{self.destination_path.table}_{now().format('YYMMDDHHmmss')}",
        )
        if not self.valid_batch_size():
            raise ValueError("Batch size needs to be between (including) 1 and 50000")

    def join_columns_by(self, columns: list[Column], separator: str = ","):
        if len(columns) == 0:
            return ""
        return separator.join([col.name for col in columns])

    def table_ddl(self, path: Path) -> str:
        ddl = [f"CREATE TABLE {path.to_str()} ("]

        columns_def = [col.column_sql_definition() for col in self._columns]

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
        elif not self.source_data_modified_column_name:
            fields.append(f"""SYSDATETIMEOFFSET() {tz_info} AS data_modified""")

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

    def python_code(self) -> str:
        """Save the object as Python code"""

        def generate_column_python_code(col: Column, dt_string: str) -> str:
            constraints = f", constraints='{col.constraints}'"
            if not col.constraints:
                constraints=""

            description = f", description='{col.description}'"
            if not col.description:
                description=""

            sensitive = f", sensitive='{col.sensitive}'"
            if not col.sensitive:
                sensitive=""


            return f"Column(name='{col.name}', data_type={dt_string}{constraints}{description}{sensitive})"

        def generate_path_python_code(prefix: str, path: Path) -> str:
            return f"{prefix}=Path(database='{path.database}', schema='{path.schema}', table='{path.table}')"

        def generate_data_type_python_code(dt: DataType) -> str:
            args = [f"type=SqlServerDataType.{dt.type.name}"]
            if dt.length is not None:
                args.append(f"length={dt.length}")
            if dt.precision is not None:
                args.append(f"precision={dt.precision}")
            if dt.scale is not None:
                args.append(f"scale={dt.scale}")
            return f"DataType({', '.join(args)})"

        def create_columns_list(key: str, value: list[Column]) -> str:
            columns_list = []
            for col in value:
                for column_attribute_key, column_attribute_value in vars(col).items():
                    if column_attribute_key == "data_type" and type(column_attribute_value) == DataType:
                        dt_string = generate_data_type_python_code(dt=column_attribute_value)
                        column_string = generate_column_python_code(col=col, dt_string=dt_string)
                        columns_list.append(f"{column_string}")
            return f"{key}=[\n{",\n".join(columns_list)}\n]"

        def create_index_list() -> str:
            index_list = []
            for index in self.indexes:
                index_list.append(f"Index(\nSqlServerIndexType.{index.index_type.name}")
                index_list.append(f"{create_columns_list(key="columns", value=index.columns)}")
                index_list.append(f"is_unique={index.is_unique}")
                index_list.append(f"sort_direction={index.sort_direction}\n)")
            return ",\n".join(index_list)

        # Get class name and attributes
        class_name = self.__class__.__name__
        excluded_attributes = ["_columns", "temp_table_path"]
        attributes = vars(self)  # Get the object's attributes as a dictionary
        attributes = {
            key: value
            for key, value in attributes.items()
            if key not in excluded_attributes
        }

        paths = []
        paths.append(generate_path_python_code(prefix="source_path", path=self.source_path))
        paths.append(generate_path_python_code(prefix="destination_path", path=self.destination_path))

        all_columns = []
        for key, value in attributes.items():
            if isinstance(value, list):
                if key == "default_columns":
                    column_list = create_columns_list(key=key, value=value)
                    all_columns.append(column_list)
                if key == "unique_columns":
                    column_list = create_columns_list(key=key, value=value)
                    all_columns.append(column_list)
                if key == "non_unique_columns":
                    column_list = create_columns_list(key=key, value=value)
                    all_columns.append(column_list)

        # Putting it all together (turn off formatting for types & columns)
        code_lines = [
            f"# ruff: noqa: F401",
            f"# Automatically generated from: {class_name}.python_code(). Do not remove noqa",
            f"from aftonfalk.mssql import Table, Path, Column, Index, SqlServerTimeZone, WriteMode, SqlServerIndexType, DataType, SortDirection, SqlServerDataType\n\n",
            f"{class_name.lower()} = {class_name}(",
        ]

        properties_lines = []
        properties_lines.extend(all_columns)
        properties_lines.append(",\n".join(paths))
        properties_lines.append(f"source_data_modified_column_name='{self.source_data_modified_column_name}'")
        properties_lines.append(f"destination_data_modified_column_name='{self.destination_data_modified_column_name}'")
        properties_lines.append(f"temp_table_schema='{self.temp_table_schema}'")
        properties_lines.append(f"enforce_primary_key={self.enforce_primary_key}")
        properties_lines.append(f"timezone={self.timezone}")
        properties_lines.append(f"write_mode={self.write_mode}")
        properties_lines.append(f"fast_executemany={self.fast_executemany}")
        properties_lines.append(f"batch_size={self.batch_size}")
        properties_lines.append(f"indexes=[{create_index_list()}]")

        code_lines.append(",\n".join(properties_lines))
        code_lines.append(")")

        return "\n".join(code_lines)
