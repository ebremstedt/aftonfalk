from dataclasses import dataclass


@dataclass
class Generic:
    source_path: str
    destination_path: str

    def table_ddl():
        pass

    def insert_sql():
        pass

    def read_sql():
        pass
