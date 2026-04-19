"""Tests for lenient JSON extraction in the date-idea agent's output parser."""

from __future__ import annotations

import unittest

from back_end.agents.date_idea_agent import (
    DateIdeaAgentOutputError,
    _extract_first_json_object,
    _parse_final_json,
)


class ExtractFirstJsonObjectTests(unittest.TestCase):
    def test_returns_none_when_no_open_brace(self) -> None:
        self.assertIsNone(_extract_first_json_object("no json here"))

    def test_extracts_object_from_prose_preamble(self) -> None:
        text = (
            "**All feasibility checks pass.**\n\nHere is the final JSON:\n\n"
            '{"date_ideas":[{"title":"X"}]}'
        )
        self.assertEqual(
            '{"date_ideas":[{"title":"X"}]}',
            _extract_first_json_object(text),
        )

    def test_stops_at_first_balanced_object_and_ignores_trailing(self) -> None:
        text = '{"a":1,"b":{"c":2}} \nfollow-up notes here {"leftover": true}'
        self.assertEqual(
            '{"a":1,"b":{"c":2}}',
            _extract_first_json_object(text),
        )

    def test_respects_braces_inside_strings(self) -> None:
        text = 'intro {"title":"with } brace","stops":[]} trailing'
        self.assertEqual(
            '{"title":"with } brace","stops":[]}',
            _extract_first_json_object(text),
        )

    def test_respects_escaped_quotes_inside_strings(self) -> None:
        text = r'{"title":"has \"escaped\" quotes and a } brace"}'
        self.assertEqual(text, _extract_first_json_object(text))

    def test_returns_none_for_unbalanced_braces(self) -> None:
        self.assertIsNone(_extract_first_json_object('{"unterminated": 1'))


class ParseFinalJsonTests(unittest.TestCase):
    def test_rejects_empty_text(self) -> None:
        with self.assertRaises(DateIdeaAgentOutputError):
            _parse_final_json("")

    def test_rejects_none(self) -> None:
        with self.assertRaises(DateIdeaAgentOutputError):
            _parse_final_json(None)

    def test_parses_strict_json(self) -> None:
        self.assertEqual({"a": 1}, _parse_final_json('{"a":1}'))

    def test_strips_markdown_fence(self) -> None:
        text = '```json\n{"a":1}\n```'
        self.assertEqual({"a": 1}, _parse_final_json(text))

    def test_recovers_from_prose_preamble(self) -> None:
        text = (
            "Checklist:\n- all good\n\nHere is the final JSON:\n\n"
            '{"date_ideas":[{"title":"X"}]}'
        )
        self.assertEqual(
            {"date_ideas": [{"title": "X"}]},
            _parse_final_json(text),
        )

    def test_recovers_from_trailing_commentary(self) -> None:
        text = '{"date_ideas":[]} \n\nLet me know if you want more.'
        self.assertEqual({"date_ideas": []}, _parse_final_json(text))

    def test_rejects_when_object_content_is_not_json(self) -> None:
        with self.assertRaises(DateIdeaAgentOutputError):
            _parse_final_json("intro { this is not valid json } trailing")

    def test_rejects_non_object_top_level_value(self) -> None:
        with self.assertRaises(DateIdeaAgentOutputError):
            _parse_final_json("[1,2,3]")


if __name__ == "__main__":
    unittest.main()
