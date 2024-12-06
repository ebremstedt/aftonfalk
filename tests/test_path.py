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
            self.assertIn("must contain only letters, numbers, or underscores", str(context.exception))

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
        self.assertIn("must contain only letters, numbers, or underscores", str(context.exception))

if __name__ == "__main__":
    unittest.main()
