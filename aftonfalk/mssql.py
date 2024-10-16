from itertools import batched
from typing import Any, Iterable, Optional
from datetime import datetime, timedelta, timezone
import pyodbc
import re
import struct


class MssqlDriver:
    def __init__(
        self, dsn: str, driver: Optional[str] = "ODBC Driver 18 for SQL Server"
    ):
        self.dsn = dsn
        self.driver = driver
        self.connection_string = self._connection_string()

    def _connection_string(self) -> str:
        pattern = re.compile(
            r"^(?P<technology>[a-zA-Z]+)://"
            r"(?P<user>[^\:]+):"
            r"(?P<password>[^\@]+)@"
            r"(?P<hostname>[^:]+):"
            r"(?P<port>[0-9]+)$"
        )

        match = pattern.match(self.dsn)
        if match:
            user = match.group("user")
            password = match.group("password")
            hostname = match.group("hostname")
            port = match.group("port")
            return f"DRIVER={self.driver};SERVER={hostname},{port};UID={user};PWD={password};TrustServerCertificate=yes;"
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
        batch_size: Optional[int] = 100,
    ) -> Iterable[dict]:
        """Read data from database

        Parameters
            query: the query to run
            params: any params you might wish to use in the query
            batch_size: divide total read into smaller batches

        returns:
            Generator of dicts
        """
        with pyodbc.connect(self.connection_string) as conn:
            conn.add_output_converter(-155, self.handle_datetimeoffset)

            with conn.cursor() as cursor:
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

    def write(self, sql: str, data: Iterable[dict], batch_size: int = 100):
        """Write to table from a generator of dicts

        Good to know: Pyodbc limitation for batch size: number_of_rows * number_of_columns < 2100

        Parameters:
            sql: the sql to run
            data: generator of dicts with the data itself
            batch_size: batches the data into manageable chunks for sql server
        """
        with pyodbc.connect(self.connection_string) as conn:
            with conn.cursor() as cursor:
                for rows in batched((tuple(row.values()) for row in data), batch_size):
                    cursor.executemany(sql, rows)

    def create_schema_in_one_go(self, catalog: str, schema: str):
        """Pyodbc cant have these two statements in one go, so we have to execute them to the cursor separately"""
        with pyodbc.connect(self.connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"USE {catalog};")
                cursor.execute(f"CREATE SCHEMA {schema};")

    def merge_ddl(
        self,
        source_path: str,
        destination_path: str,
        unique_columns: list[str],
        update_columns: list[str],
        modified_column: str,
    ) -> str:
        if not unique_columns or not update_columns:
            raise ValueError("Unique columns and update columns cannot be empty.")

        on_conditions = (
            " AND ".join([f"target.{col} = source.{col}" for col in unique_columns])
            + f" AND source.{modified_column} >= target.{modified_column}"
        )
        update_clause = ", ".join(
            [f"target.{col} = source.{col}" for col in update_columns]
        )
        insert_columns = ", ".join(unique_columns + update_columns)
        insert_values = ", ".join(
            [f"source.{col}" for col in unique_columns + update_columns]
        )

        # temporary fix because test data
        source_path = (
            f"(SELECT * FROM {source_path} where {unique_columns[0]} <> 'NULL')"
        )

        return f"""
            MERGE INTO {destination_path} AS target
            USING {source_path} AS source
            ON {on_conditions}
            WHEN MATCHED THEN
                UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values});
        """


    def _schema_exists(self, catalog: str, schema: str) -> bool:
        """Create ddl to check if anything exists"""
        sql = f"""SELECT
            top 1 CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM {catalog}.sys.schemas
                    WHERE name = '{schema}'
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

    def _table_exists(self, catalog: str, schema: str, table_name: str) -> bool:
        """Create ddl to check if anything exists"""
        sql = f"""SELECT
            top 1 CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM [{catalog}].sys.tables t
                    LEFT JOIN [{catalog}].sys.schemas s on t.schema_id  = s.schema_id
                    WHERE t.name = '{table_name}'
                    AND s.name = '{schema}'
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

    def _create_schema(self, catalog: str, schema: str):
        """Create schema if it does not already exist"""
        if not self._schema_exists(catalog=catalog, schema=schema):
            self.create_schema_in_one_go(catalog=catalog, schema=schema)

    def create_table(
        self,
        path: str,
        ddl: str,
    ):
        """Create table. An effort to standardize our landing area.

        Parameters:
            Path: where the table would be located
            ddl: the ddl to create the table
        """
        catalog, schema, table = path.split(".")

        if self._table_exists(catalog=catalog, schema=schema, table_name=table):
            return

        self._create_schema(catalog=catalog, schema=schema)

        self.execute(sql=ddl)

    def read_from_source_table(
        self,
        path: str,
        params: Optional[tuple] = (),
        where_clause: Optional[str] = "WHERE 1=1",
    ) -> Iterable[dict]:
        catalog, schema, table = path.split(".")

        source_query = f"SELECT * FROM [{catalog}].[{schema}].[{table}] {where_clause}"

        return self.read(query=source_query, params=params)
