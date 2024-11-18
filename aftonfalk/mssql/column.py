from aftonfalk.mssql.data_type import DataType
from dataclasses import dataclass


@dataclass
class Column:
    name: str
    data_type: DataType
    constraints: str = ""
    description: str = ""
    sensitive: bool = False

    def column_sql_definition(self) -> str:
        return f"{self.name} {self.data_type.definition.replace("(-1)", "(MAX)")} {self.constraints}".strip()
