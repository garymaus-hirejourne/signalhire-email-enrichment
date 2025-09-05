import unittest
from .south_detroit_phase1_team_scraper_debug import ContactScraper


class TestNameValidation(unittest.TestCase):
    def setUp(self):
        self.scraper = ContactScraper("", "")  # Empty API keys for testing

    def test_standard_names(self):
        valid_names = ["John Smith", "Jane Doe", "Robert Johnson", "Sarah Williams"]
        for name in valid_names:
            self.assertTrue(self.scraper.validate_name(name), f"Failed for {name}")

    def test_names_with_special_characters(self):
        valid_names = [
            "John O'Malley-Smith",
            "Jean-Luc Picard",
            "Mary-Jane Smith",
            "Robert Downey Jr.",
        ]
        for name in valid_names:
            self.assertTrue(self.scraper.validate_name(name), f"Failed for {name}")

    def test_names_with_titles(self):
        valid_names = [
            "Dr. John Smith",
            "Mr. John Smith",
            "Mrs. Jane Doe",
            "Ms. Sarah Williams",
        ]
        for name in valid_names:
            self.assertTrue(self.scraper.validate_name(name), f"Failed for {name}")

    def test_names_with_suffixes(self):
        valid_names = [
            "John Smith, MD",
            "Jane Doe, PhD",
            "Robert Johnson, Esq.",
            "Sarah Williams, DVM",
        ]
        for name in valid_names:
            self.assertTrue(self.scraper.validate_name(name), f"Failed for {name}")

    def test_invalid_names(self):
        invalid_names = [
            "",
            "A",
            "John",
            "johnsmith",
            "Team Overview",
            "Contact Us",
            "John Smith and Associates",
            "The Smith Company",
            "Smith & Sons",
            "Smith, Inc.",
        ]
        for name in invalid_names:
            self.assertFalse(self.scraper.validate_name(name), f"Failed for {name}")

    def test_long_names(self):
        long_name = "John Smith " * 20
        self.assertFalse(
            self.scraper.validate_name(long_name), "Long name should be invalid"
        )


if __name__ == "__main__":
    unittest.main()
