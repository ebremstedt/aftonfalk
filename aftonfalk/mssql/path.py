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

        pattern = r'^[a-zA-Z0-9_]+$'
        if not re.match(pattern, part):
            raise ValueError(f"Name '{part}' must contain only letters, numbers, or underscores")

    def __post_init__(self):
        # Static method can be called without `self`
        Path.validate_part(self.database)
        Path.validate_part(self.schema)
        Path.validate_part(self.table)
