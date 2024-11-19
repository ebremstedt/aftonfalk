from dataclasses import dataclass


class InvalidPathException(Exception):
    pass


@dataclass
class Path:
    database: str
    schema: str
    table: str

    def to_str(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"
