from itertools import batched
from typing import Any, Iterable, Optional
from datetime import datetime, timedelta, timezone
import pyodbc
import struct
from urllib.parse import urlparse, unquote
from aftonfalk.mssql.table import Table
from aftonfalk.mssql.path import Path
from aftonfalk.mssql.write_mode import WriteMode


class MssqlDriver:
    def __init__(
        self,
        dsn: str,
        driver: Optional[str] = "ODBC Driver 18 for SQL Server",
        trust_server_certificate: bool = True,
        encrypt: bool = False,
    ):
        self.dsn = dsn
        self.driver = driver
        self.trust_server_certificate = trust_server_certificate
        self.encrypt = encrypt
        self.connection_string = self._connection_string()

    def _connection_string(self) -> str:
        parsed = urlparse(self.dsn)
        technology = parsed.scheme # noqa # ignore
        user = unquote(parsed.username) if parsed.username else None
        password = unquote(parsed.password) if parsed.password else None
        hostname = parsed.hostname
        port = parsed.port

        trust_server_certificate_str = ""
        if self.trust_server_certificate:
            trust_server_certificate_str = "TrustServerCertificate=yes;"

        encrypt_str = ""
        if not self.encrypt:
            encrypt_str = "Encrypt=no;"

            return f"DRIVER={self.driver};SERVER={hostname},{port};UID={user};PWD={password};{trust_server_certificate_str}{encrypt_str}"
        else:
            raise ValueError("Invalid DSN format")

    def handle_datetimeoffset(self, dto_value):
        # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
        tup = struct.unpack(
            "<6hI2h", dto_value
        )  # e.g., (2017, 3, 16, 10, 35, 18, 500000000, -6, 0)
        return datetime(
            tup[0],
            tup[1],
            tup[2],
            tup[3],
            tup[4],
            tup[5],
            tup[6] // 1000,
            timezone(timedelta(hours=tup[7], minutes=tup[8])),
        )

    def read(
        self,
        query: str,
        params: Optional[tuple] = None,
        batch_size: Optional[int] = 1000,
        database: Optional[str] = None,
    ) -> Iterable[dict]:
        """Read data from database

        Parameters
            query: the query to run
            params: any params you might wish to use in the query
            batch_size: divide total read into smaller batches
            database: Useful when queries need a database context, such as when querying the INFORMATION_SCHEMA tables

        returns:
            Generator of dicts
        """
        with pyodbc.connect(self.connection_string) as conn:
            conn.add_output_converter(-155, self.handle_datetimeoffset)
            with conn.cursor() as cursor:
                if database is not None:
                    cursor.execute(f"USE {database};")
                if params is not None:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                columns = [column[0] for column in cursor.description]

                while True:
                    rows = cursor.fetchmany(batch_size)
                    if len(rows) == 0:
                        break
                    for row in rows:
                        yield dict(zip(columns, row))

    def execute(self, sql: str, *params: Any):
        """Internal function used to execute sql queries without parameters

        Parameters
            sql: the sql to run
        """
        with pyodbc.connect(self.connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, *params)
                cursor.commit()

    def write(
        self,
        sql: str,
        data: Iterable[dict],
        batch_size: int = 1000,
        fast_executemany: bool = False,
    ):
        """Write to table from a generator of dicts

        Good to know: Pyodbc limitation for batch size: number_of_rows * number_of_columns < 2100

        Parameters:
            sql: the sql to run
            data: generator of dicts with the data itself
            batch_size: batches the data into manageable chunks for sql server
        """
        with pyodbc.connect(self.connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.fast_executemany = fast_executemany
                for rows in batched((tuple(row.values()) for row in data), batch_size):
                    cursor.executemany(sql, rows)

    def create_schema_in_one_go(self, path: Path):
        """Pyodbc cant have these two statements in one go, so we have to execute them to the cursor separately"""
        with pyodbc.connect(self.connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"USE {path.database};")
                cursor.execute(f"CREATE SCHEMA {path.schema};")

    def merge_ddl(
        self,
        table: Table,
    ) -> str:
        update_columns = table.non_unique_columns + table.default_columns

        if not table.unique_columns or not update_columns:
            raise ValueError("Unique columns and update columns cannot be empty.")

        on_conditions = (
            " AND ".join(
                [
                    f"target.{col.name} = source.{col.name}"
                    for col in table.unique_columns
                ]
            )
            + f" AND source.{table.destination_data_modified_column_name} >= target.{table.destination_data_modified_column_name}"
        )
        update_clause = ", ".join(
            [f"target.{col.name} = source.{col.name}" for col in update_columns]
        )
        insert_columns = ", ".join([col.name for col in table._columns])
        insert_values = ", ".join([f"source.{col.name}" for col in table._columns])

        merge_ddl = f"""
            MERGE INTO {table.destination_path.to_str()} AS target
            USING {table.temp_table_path.to_str()} AS source
            ON {on_conditions}
            WHEN MATCHED THEN
                UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values});
        """

        return merge_ddl

    def _schema_exists(self, path: Path) -> bool:
        """Create ddl to check if anything exists"""
        sql = f"""SELECT
            top 1 CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM {path.database}.sys.schemas
                    WHERE name = '{path.schema}'
                )
                THEN 1
                ELSE 0
            END AS thing_exists;
            """

        schema_exists = False
        for row in self.read(query=sql):
            if row.get("thing_exists") == 1:
                return True

        return schema_exists

    def _index_exists(self, path: Path, index_name: str) -> bool:
        """Create ddl to check if anything exists"""
        sql = f"SELECT i.name as index_name FROM {path.database}.sys.indexes i WHERE i.name = '{index_name}'"
        for row in self.read(query=sql):
            if row.get("index_name") == index_name:
                return True
        return False

    def _table_exists(self, path: Path) -> bool:
        """Create ddl to check if anything exists"""
        sql = f"""SELECT
            top 1 CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM [{path.database}].sys.tables t
                    LEFT JOIN [{path.database}].sys.schemas s on t.schema_id  = s.schema_id
                    WHERE t.name = '{path.table}'
                    AND s.name = '{path.schema}'
                )
                THEN 1
                ELSE 0
            END AS thing_exists;
            """

        table_exists = False
        for row in self.read(query=sql):
            if row.get("thing_exists") == 1:
                return True

        return table_exists

    def _create_schema(self, path: Path):
        """Create schema if it does not already exist"""
        if not self._schema_exists(path=path):
            self.create_schema_in_one_go(path=path)

    def create_table(self, path: Path, ddl: str, drop_first: Optional[bool] = False):
        """
        Parameters:
            Path: where the table would be located
            ddl: the ddl to create the table
            drop_first: do you want to drop the table before creating it (default: False)
        """

        if self._table_exists(path=path):
            if not drop_first:
                return
            self.execute(sql=f"DROP TABLE {path.to_str()};")

        self._create_schema(path=path)

        self.execute(sql=ddl)

    def apply_indexes(self, table: Table, path: Path):
        for index in table.indexes:
            if not self._index_exists(
                path=path, index_name=index.index_name(path=path)
            ):
                self.execute(sql=index.to_sql(path=path))

    def truncate_write(self, table: Table, data: Iterable[dict]):
        path = table.destination_path
        self.create_table(path=path, ddl=table.table_ddl(path=path), drop_first=True)

        self.apply_indexes(table=table, path=path)

        self.write(
            sql=table.insert_sql(path=path),
            data=data,
            batch_size=table.batch_size,
            fast_executemany=table.fast_executemany,
        )

    def append(self, table: Table, data: Iterable[dict]):
        path = table.destination_path

        self.create_table(path=path, ddl=table.table_ddl(path=path))

        self.apply_indexes(table=table, path=path)

        self.write(
            sql=table.insert_sql(path=path),
            data=data,
            batch_size=table.batch_size,
            fast_executemany=table.fast_executemany,
        )

    def merge(self, table: Table, data: Iterable[list]):
        """
        Creates destination schema + table if it does not already exist.
        Creates temporary and equivalent table to which data is inserted to.
        Data is then merged to destination table, and the temporary table is deleted.

        Parameters:
            table: the table to merge to
            data: the data itself
            drop_destination_first: whether you want to drop the destination before creating table
        """

        self.create_table(
            ddl=table.table_ddl(path=table.destination_path),
            path=table.destination_path,
        )
        self.apply_indexes(table=table, path=table.destination_path)

        self.create_table(
            ddl=table.table_ddl(path=table.temp_table_path),
            path=table.temp_table_path,
        )
        self.apply_indexes(table=table, path=table.temp_table_path)

        self.write(
            sql=table.insert_sql(path=table.temp_table_path),
            data=data,
            batch_size=table.batch_size,
            fast_executemany=table.fast_executemany,
        )

        merge_sql = self.merge_ddl(
            table=table,
        )

        self.execute(sql=merge_sql)

        self.execute(sql=f"DROP TABLE {table.temp_table_path.to_str()};")

    def write_using_modes(self, table: Table, data: Iterable[dict]):
        if table.write_mode == WriteMode.APPEND:
            self.append(table=table, data=data)
        elif table.write_mode == WriteMode.TRUNCATE_WRITE:
            self.truncate_write(table=table, data=data)
        elif table.write_mode == WriteMode.MERGE:
            self.merge(table=table, data=data)
