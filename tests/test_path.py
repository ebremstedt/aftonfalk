import unittest
from aftonfalk.mssql import Path, InvalidPathException


class TestPath(unittest.TestCase):
    def test_valid_path(self):
        """Test that a valid Path is created successfully."""
        path = Path(database="my_db", schema="public", table="users")
        self.assertEqual(path.to_str(), "my_db.public.users")

    def test_empty_part(self):
        """Test that an empty part raises a ValueError."""
        with self.assertRaises(ValueError) as context:
            Path(database="", schema="public", table="users")
        self.assertIn("Name cannot be empty", str(context.exception))

    def test_invalid_characters_in_part(self):
        """Test that invalid characters in a part raise a ValueError."""
        invalid_names = ["my-db", "public!", "us@ers"]
        for part in invalid_names:
            with self.assertRaises(ValueError) as context:
                Path(database="my_db", schema=part, table="users")
            self.assertIn(
                "must contain only letters, numbers, or underscores",
                str(context.exception),
            )

    def test_to_str_format(self):
        """Test that the to_str method returns the correct format."""
        path = Path(database="my_database", schema="my_schema", table="my_table")
        self.assertEqual(path.to_str(), "my_database.my_schema.my_table")

    def test_validate_part_as_static_method(self):
        """Test validate_part as a standalone static method."""
        # Valid part
        Path.validate_part(part="valid_name")  # Should not raise any exceptions
        # Invalid part
        with self.assertRaises(ValueError) as context:
            Path.validate_part("invalid-name!")
        self.assertIn(
            "must contain only letters, numbers, or underscores", str(context.exception)
        )

    def test_valid_table_names(self):
        """Test valid table names."""
        valid_table_names = [
            "table123",
            "table_name",
            "_underscore_table",
            "Table",
            "table123_name",
            "#tempTable",
            "#_temp123",
        ]
        for name in valid_table_names:
            with self.subTest(name=name):
                Path.validate_table(name)

    def test_invalid_table_names(self):
        """Test invalid table names."""
        invalid_table_names = [
            "",  # Empty string
            "table-name",  # Dash is not allowed
            "table name",  # Space is not allowed
            "table!",  # Special character
            "##temp",  # Double #
            "#temp@name",  # Special character after #
        ]

        for name in invalid_table_names:
            try:
                Path.validate_table(name)
                self.fail(
                    f"Expected ValueError for table name '{name}', but no exception was raised."
                )
            except ValueError:
                pass

    def test_empty_table_name(self):
        """Test that an empty table name raises an exception."""
        with self.assertRaises(ValueError) as context:
            Path.validate_table("")
        self.assertEqual(str(context.exception), "Name cannot be empty")

    def test_table_with_special_characters(self):
        """Test table names with special characters."""
        with self.assertRaises(ValueError) as context:
            Path.validate_table("table#name")
        self.assertIn(
            "must contain only letters, numbers, or underscores", str(context.exception)
        )


if __name__ == "__main__":
    unittest.main()
