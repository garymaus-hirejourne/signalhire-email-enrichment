import unittest
import os

from SouthDetroit.scripts.south_detroit_phase1_team_scraper_debug import ContactScraper


class TestInternationalNames(unittest.TestCase):
    def setUp(self):
        # Use test API keys or empty strings for testing
        self.scraper = ContactScraper("", "")

    def test_two_part_names(self):
        valid_names = [
            "Li Wei",  # Chinese name
            "Wang Mei",  # Chinese name
            "Zhang Hua",  # Chinese name
            "Liu Ying",  # Chinese name
            "Chen Li",  # Chinese name
            "Yang Wei",  # Chinese name
            "Zhao Hua",  # Chinese name
            "Sun Mei",  # Chinese name
            "Zhou Li",  # Chinese name
            "Wu Wei",  # Chinese name
            "Ravi Kumar",  # Indian name
            "Priya Sharma",  # Indian name
            "Vijay Singh",  # Indian name
            "Meera Patel",  # Indian name
            "Rajesh Gupta",  # Indian name
            "Sita Desai",  # Indian name
            "Amit Joshi",  # Indian name
            "Neha Chaudhary",  # Indian name
            "Rajiv Sharma",  # Indian name
            "Priyanka Patel",  # Indian name
            "Jean-Luc Picard",  # French name
            "Carlos García",  # Spanish name
            "Maria Silva",  # Portuguese name
            "Olga Ivanova",  # Russian name
            "Maria Rodriguez",  # Spanish name
            "Anna Müller",  # German name
            "Giuseppe Rossi",  # Italian name
            "Ahmed Hassan",  # Arabic name
            "Mohammed Ali",  # Arabic name
            "Pierre Dubois",  # French name
            "Maria Fernandes",  # Portuguese name
            "Olga Petrova",  # Russian name
            "Anna Schmidt",  # German name
            "Giuseppe Bianchi",  # Italian name
            "Takahashi Kenji",  # Japanese name
            "Sato Yuki",  # Japanese name
            "Tanaka Takeshi",  # Japanese name
            "Suzuki Akira",  # Japanese name
            "Watanabe Yuki",  # Japanese name
            "Kato Takashi",  # Japanese name
            "Yamamoto Yoko",  # Japanese name
            "Ito Akira",  # Japanese name
            "Nakamura Yuki",  # Japanese name
            "Kobayashi Yuki",  # Japanese name
        ]
        for name in valid_names:
            self.assertTrue(self.scraper.validate_name(name), f"Failed for {name}")

    def test_invalid_names(self):
        invalid_names = [
            "Cher",  # Single name
            "Prince",  # Single name
            "Madonna",  # Single name
            "Beyoncé",  # Single name
            "John",  # Single name
            "Smith",  # Single name
            "Team Lead",  # Non-name pattern
            "Contact Us",  # Non-name pattern
            "About Us",  # Non-name pattern
            "Company Inc",  # Company name
            "LLC",  # Company name
            "Group",  # Company name
            "and",  # Common word
            "the",  # Common word
            "12345",  # Numbers only
            "",  # Empty string
            " ",  # Whitespace
            "a b c",  # Too many parts
            "John",  # Too few parts
            "Smith",  # Too few parts
            "John Smith Smith",  # Too many parts
            "John-Smith-Smith",  # Too many parts
            "John Smith, MD, PhD",  # Too many parts
            "Dr. Dr. John Smith"  # Too many parts
            "Li Wei, PhD",  # Suffix makes too many parts
            "Dr. Wang Mei"  # Prefix makes too many parts
            "Ahmed Hassan, MD",  # Suffix makes too many parts
            "Dr. Pierre Dubois",  # Prefix makes too many parts
        ]
        for name in invalid_names:
            self.assertFalse(self.scraper.validate_name(name), f"Failed for {name}")


if __name__ == "__main__":
    unittest.main()
