from enum import Enum, auto
from dataclasses import dataclass
import re


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
    TIMESTAMP = auto() # note: this is like ROWVERSION
    ROWVERSION = auto()
    TEXT = auto()
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
    SqlServerDataType.TEXT
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

        if self.type not in PRECISION_SCALE_TYPES and self.type not in PRECISION_ONLY_TYPES:
            if self.precision is not None or self.scale is not None:
                raise ValueError(f"{self.type} type can't have precision or scale!")

        if self.type in PRECISION_ONLY_TYPES and self.scale is not None:
            raise ValueError(f"{self.type} type can't have scale!")

        if self.type in PRECISION_SCALE_TYPES:
            if self.precision is None or self.scale is None:
                raise ValueError(f"{self.type} type requires both precision and scale!")


    def datatype_definition(self) -> str:

        if self.type in LENGTH_TYPES:
            if self.length:
                return f"{self.type.name}({self.length})".replace("(-1)", "(MAX)")
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


RESERVED_KEYWORDS = {
    "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "BACKUP", "BEGIN",
    "BETWEEN", "BREAK", "BROWSE", "BULK", "BY", "CASCADE", "CASE", "CHECK", "CHECKPOINT",
    "CLOSE", "CLUSTERED", "COALESCE", "COLUMN", "COMMIT", "COMPUTE", "CONSTRAINT", "CONTAINS",
    "CONTAINSTABLE", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE",
    "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DBCC",
    "DEALLOCATE", "DECLARE", "DEFAULT", "DELETE", "DENY", "DESC", "DISK", "DISTINCT", "DISTRIBUTED",
    "DOUBLE", "DROP", "DUMP", "ELSE", "END", "ERRLVL", "ESCAPE", "EXCEPT", "EXEC", "EXECUTE",
    "EXISTS", "EXIT", "EXTERNAL", "FETCH", "FILE", "FILLFACTOR", "FOR", "FOREIGN", "FREETEXT",
    "FREETEXTTABLE", "FROM", "FULL", "FUNCTION", "GOTO", "GRANT", "GROUP", "HAVING", "HOLDLOCK",
    "IDENTITY", "IDENTITY_INSERT", "IDENTITYCOL", "IF", "IN", "INDEX", "INNER", "INSERT", "INTERSECT",
    "INTO", "IS", "JOIN", "KEY", "KILL", "LEFT", "LIKE", "LINENO", "LOAD", "MERGE", "NATIONAL", "NOCHECK",
    "NONCLUSTERED", "NOT", "NULL", "NULLIF", "OF", "OFF", "OFFSETS", "ON", "OPEN", "OPENDATASOURCE",
    "OPENQUERY", "OPENROWSET", "OPENXML", "OPTION", "OR", "ORDER", "OUTER", "OVER", "PERCENT", "PIVOT",
    "PLAN", "PRECISION", "PRIMARY", "PRINT", "PROC", "PROCEDURE", "PUBLIC", "RAISERROR", "READ",
    "READTEXT", "RECONFIGURE", "REFERENCES", "REPLICATION", "RESTORE", "RESTRICT", "RETURN", "REVERT",
    "REVOKE", "RIGHT", "ROLLBACK", "ROWCOUNT", "ROWGUIDCOL", "RULE", "SAVE", "SCHEMA", "SECURITYAUDIT",
    "SELECT", "SEMANTICKEYPHRASETABLE", "SEMANTICSIMILARITYDETAILSTABLE", "SEMANTICSIMILARITYTABLE",
    "SESSION_USER", "SET", "SETUSER", "SHUTDOWN", "SOME", "STATISTICS", "SYSTEM_USER", "TABLE", "TABLESAMPLE",
    "TEXTSIZE", "THEN", "TO", "TOP", "TRAN", "TRANSACTION", "TRIGGER", "TRUNCATE", "TRY_CONVERT",
    "TSEQUAL", "UNION", "UNIQUE", "UNPIVOT", "UPDATE", "UPDATETEXT", "USE", "USER", "VALUES", "VARYING",
    "VIEW", "WAITFOR", "WHEN", "WHERE", "WHILE", "WITH", "WITHIN GROUP", "WRITETEXT"
}

@dataclass
class Column:
    name: str
    data_type: DataType
    constraints: str = ""
    description: str = ""
    sensitive: bool = False

    def column_sql_definition(self) -> str:
        return f"{self.name} {self.data_type.definition} {self.constraints}".strip()

    def validate_sql_column_name(self):
        if not (1 <= len(self.name) <= 128):
            raise ValueError(f'Name {self.name} must be between 1 and 128')

        def is_valid_string(s: str) -> bool:
            pattern = r"^[a-zA-ZåäöÅÄÖ0-9_]+$"
            return bool(re.match(pattern, s))

        if not is_valid_string(s=self.name):
            raise ValueError(f'Name {self.name} must match: r"^[a-zA-ZåäöÅÄÖ0-9_]+$"')
        if self.name.upper() in RESERVED_KEYWORDS:
            error_strings = [
                "1. Must be between 1 and 128 characters.",
                "2. Must be letters or underscores.",
                "3. Cannot be a reserved keyword."
            ]
            raise ValueError(f"Column name must fill criteria:\n{'\n'.join(error_strings)}\nThe selected name {self.name} is unfortunately one of {RESERVED_KEYWORDS} ")

    def validate_types(self):
        if not isinstance(self.data_type, DataType):
            raise TypeError(f"Column attribute data_type must be of type DataType")

    def __post_init__(self):
        self.validate_types()
        self.validate_sql_column_name()
