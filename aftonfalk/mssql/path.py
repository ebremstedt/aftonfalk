from dataclasses import dataclass
import re


class InvalidPathException(Exception):
    pass


@dataclass
class Path:
    database: str
    schema: str
    table: str

    def to_str(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

    @staticmethod
    def validate_part(part: str):
        if not part:
            raise ValueError("Name cannot be empty")

        pattern = r"^[a-zA-Z0-9_]+$"
        if not re.match(pattern, part):
            raise ValueError(
                f"Name '{part}' must contain only letters, numbers, or underscores"
            )

    @staticmethod
    def validate_table(table: str):
        if not table:
            raise ValueError("Name cannot be empty")

        pattern = r"^#?[a-zA-Z0-9_]+$"
        if not re.match(pattern, table):
            raise ValueError(
                f"Table '{table}' must contain only letters, numbers, or underscores. It can however, start with #"
            )

    def __post_init__(self):
        Path.validate_part(self.database)
        Path.validate_part(self.schema)
        Path.validate_table(self.table)
