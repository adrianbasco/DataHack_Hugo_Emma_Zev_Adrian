from __future__ import annotations

import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATES_PATH = REPO_ROOT / "config" / "date_templates.yaml"

ALCOHOL_FIRST_STOP_TYPES = {
    "bar",
    "brewery",
    "brewery_or_bar",
    "cocktail_bar",
    "pub",
    "rooftop_bar",
    "wine_bar",
}


class DateTemplatesConfigTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        raw_config = yaml.safe_load(TEMPLATES_PATH.read_text(encoding="utf-8"))
        cls.templates = raw_config["templates"]

    def test_template_ids_are_unique(self) -> None:
        template_ids = [template["id"] for template in self.templates]
        self.assertEqual(len(template_ids), len(set(template_ids)))

    def test_templates_do_not_outsource_the_date_to_home(self) -> None:
        for template in self.templates:
            description = template["description"].lower()
            self.assertNotIn("at home", description, template["id"])
            self.assertNotIn("happens at home", description, template["id"])

    def test_templates_have_meaningful_variation_count(self) -> None:
        for template in self.templates:
            value = template.get("meaningful_variations")
            self.assertIs(
                type(value),
                int,
                f"{template['id']} must define an integer meaningful_variations value.",
            )
            self.assertGreater(
                value,
                0,
                f"{template['id']} meaningful_variations must be positive.",
            )

    def test_templates_do_not_repeat_the_same_stop_type_three_times(self) -> None:
        for template in self.templates:
            stop_types = [stop["type"] for stop in template["stops"]]
            for index in range(len(stop_types) - 2):
                window = stop_types[index : index + 3]
                self.assertFalse(
                    window[0] == window[1] == window[2],
                    f"{template['id']} repeats stop type {window[0]!r} three times.",
                )

    def test_templates_are_not_just_drinking_venues(self) -> None:
        for template in self.templates:
            stop_types = {stop["type"] for stop in template["stops"]}
            self.assertFalse(
                stop_types and stop_types.issubset(ALCOHOL_FIRST_STOP_TYPES),
                f"{template['id']} is just a drinking crawl.",
            )


if __name__ == "__main__":
    unittest.main()
