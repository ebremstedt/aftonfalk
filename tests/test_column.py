import unittest
from aftonfalk.mssql import Column, DataType, SqlServerDataType, RESERVED_KEYWORDS


class TestColumn(unittest.TestCase):
    def setUp(self):
        self.default_data_type = DataType(type=SqlServerDataType.TEXT)

    def test_valid_column_creation(self):
        data_type = DataType(type=SqlServerDataType.INT)
        column = Column(name="user_id", data_type=data_type)
        self.assertEqual(column.name, "user_id")
        self.assertEqual(column.data_type, data_type)
        self.assertEqual(column.constraints, "")
        self.assertEqual(column.description, "")
        self.assertFalse(column.sensitive)

    def test_column_sql_definition(self):
        data_type_int = DataType(type=SqlServerDataType.INT)
        column = Column(name="age", data_type=data_type_int, constraints="NOT NULL")
        self.assertEqual(column.column_sql_definition(), "age INT NOT NULL")

        data_type_varchar = DataType(type=SqlServerDataType.VARCHAR, length=50)
        column_with_length = Column(name="email", data_type=data_type_varchar)
        self.assertEqual(
            column_with_length.column_sql_definition(), "email VARCHAR(50)"
        )

    def test_column_name_validation(self):
        valid_names = [
            "user_id",
            "first_name",
            "age2",
            "åårsmöte",
            "UPPERCASE_NAME",
            "mixed_Case_123",
        ]
        for name in valid_names:
            Column(name=name, data_type=self.default_data_type)

    def test_column_name_invalid_length(self):
        with self.assertRaisesRegex(ValueError, "must be between 1 and 128"):
            Column(name="", data_type=self.default_data_type)

        long_name = "a" * 129
        with self.assertRaisesRegex(ValueError, "must be between 1 and 128"):
            Column(name=long_name, data_type=self.default_data_type)

    def test_column_name_invalid_characters(self):
        invalid_names = [
            "user-name",
            "first name",
            "email@test",
            "user$name",
            "special!char",
        ]
        for name in invalid_names:
            with self.assertRaisesRegex(ValueError, "must match"):
                Column(name=name, data_type=self.default_data_type)

    def test_reserved_keyword_validation(self):
        for keyword in RESERVED_KEYWORDS:
            with self.assertRaises(ValueError):
                Column(name=keyword, data_type=self.default_data_type)

    def test_invalid_data_type(self):
        with self.assertRaisesRegex(TypeError, "data_type must be of type DataType"):
            Column(name="test", data_type="not a DataType")

    def test_optional_parameters(self):
        data_type = DataType(type=SqlServerDataType.NVARCHAR, length=100)
        column = Column(
            name="email",
            data_type=data_type,
            constraints="UNIQUE",
            description="User email address",
            sensitive=True,
        )
        self.assertEqual(column.constraints, "UNIQUE")
        self.assertEqual(column.description, "User email address")
        self.assertTrue(column.sensitive)


if __name__ == "__main__":
    unittest.main()
