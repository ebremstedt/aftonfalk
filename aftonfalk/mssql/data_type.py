from enum import Enum, auto
from dataclasses import dataclass


class SqlServerDataType(Enum):
    BIT = auto()
    BIGINT = auto()
    BINARY = auto()
    CHAR = auto()
    CURSOR = auto()
    DATE = auto()
    DATETIME = auto()
    DATETIME2 = auto()
    DATETIMEOFFSET = auto()
    DECIMAL = auto()
    FLOAT = auto()
    GEOGRAPHY = auto()
    GEOMETRY = auto()
    IMAGE = auto()
    INT = auto()
    MONEY = auto()
    NCHAR = auto()
    NUMERIC = auto()
    NVARCHAR = auto()
    REAL = auto()
    SMALLDATETIME = auto()
    SMALLINT = auto()
    SMALLMONEY = auto()
    TABLE = auto()
    TIME = auto()
    TINYINT = auto()
    UNIQUEIDENTIFIER = auto()
    VARBINARY = auto()
    VARCHAR = auto()
    XML = auto()


LENGTH_TYPES = [
    SqlServerDataType.CHAR,
    SqlServerDataType.NCHAR,
    SqlServerDataType.VARCHAR,
    SqlServerDataType.NVARCHAR,
    SqlServerDataType.BINARY,
    SqlServerDataType.VARBINARY,
]

LENGTH_MAX_TYPES = [
    SqlServerDataType.VARCHAR,
    SqlServerDataType.NVARCHAR,
    SqlServerDataType.VARBINARY,
]

LENGTH_MAX: int = 8000
LENGTH_MIN: int = -1

PRECISION_SCALE_TYPES = [SqlServerDataType.DECIMAL, SqlServerDataType.NUMERIC]

PRECISION_ONLY_TYPES = [SqlServerDataType.FLOAT]


@dataclass
class DataType:
    type: SqlServerDataType
    length: int = None
    precision: int = None
    scale: int = None
    definition: str = None
    python_definition: str = None

    def validate_length_datatypes(self):
        if not self.length:
            return

        if not (LENGTH_MIN <= self.length <= LENGTH_MAX) and self.length != 0:
            raise ValueError(
                f"{self.type} length must either be -1 (translates to MAX) or be between 1 and {LENGTH_MAX}."
            )

        return

    def validate_datatypes(self):
        if self.type in LENGTH_TYPES:
            self.validate_length_datatypes()

        if self.type not in LENGTH_TYPES:
            if self.length is not None:
                raise ValueError(f"{self.type} type can't have length!.")

        if self.type in PRECISION_SCALE_TYPES and not self.precision:
            raise ValueError(f"{self.type} type requires a non-empty precision.")

        if self.type in PRECISION_SCALE_TYPES and self.precision and not self.scale:
            raise ValueError(
                f"{self.type} type requires a non-empty scale along with precision."
            )

        if self.type in PRECISION_ONLY_TYPES and not self.precision:
            raise ValueError(f"{self.type} type requires a non-empty precision.")

    def datatype_definition(self) -> str:

        if self.type in LENGTH_TYPES:
            if self.length:
                return f"{self.type.name}({self.length})"
            else:
                return f"{self.type.name}(255)"

        elif self.type in PRECISION_SCALE_TYPES:
            if self.precision and self.scale:
                return f"{self.type.name}({self.precision}, {self.scale})"
            elif self.precision:
                return f"{self.type.name}({self.precision})"
            else:
                return f"{self.type.name}(18, 0)"

        elif self.type in PRECISION_ONLY_TYPES:
            if self.precision:
                return f"{self.type.name}({self.precision})"
            else:
                return f"{self.type.name}(53)"

        else:
            return self.type.name

    def generate_python_code(self) -> str:
        args = [f"type=SqlServerDataType.{self.type.name}"]

        if self.length is not None:
            args.append(f"length={self.length}")
        if self.precision is not None:
            args.append(f"precision={self.precision}")
        if self.scale is not None:
            args.append(f"scale={self.scale}")

        return f"DataType({', '.join(args)})"


    def __post_init__(self):
        self.validate_datatypes()
        self.definition = self.datatype_definition()
        self.python_definition = self.generate_python_code()
