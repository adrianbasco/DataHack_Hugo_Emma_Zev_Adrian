from __future__ import annotations

import unittest

from back_end.precache.cards import build_plan_card_payload


class PlanCardPayloadTests(unittest.TestCase):
    def test_build_plan_card_payload_includes_api_card_fields(self) -> None:
        payload = build_plan_card_payload(
            plan_title="Cocktails, Dinner & Dessert",
            plan_hook="A Maps-verified date night plan.",
            plan_time_iso="2026-04-24T19:00:00+10:00",
            bucket_id="sydney_cbd",
            bucket_label="Sydney CBD",
            template_id="drinks_dinner_dessert",
            template_title="Drinks, dinner, dessert",
            template_description="Start with drinks and end somewhere sweet.",
            vibe=("romantic", "foodie"),
            transport_mode="WALK",
            model="anthropic/test-model",
            stops=[
                {
                    "kind": "venue",
                    "stop_type": "cocktail_bar",
                    "fsq_place_id": "bar-1",
                    "name": "Bar One",
                    "description": "Kick things off with cocktails.",
                    "why_it_fits": "It sets the tone.",
                }
            ],
            verification={
                "stops_verification": [
                    {
                        "matched": True,
                        "google_place_id": "google-bar-1",
                        "google_maps_uri": "https://maps.google.com/?cid=bar-1",
                        "website_uri": "https://bar.example.com",
                        "formatted_address": "1 Date St, Sydney NSW 2000",
                        "business_status": "OPERATIONAL",
                        "match_kind": "address_match",
                        "open_at_plan_time": True,
                        "rating": 4.7,
                        "user_rating_count": 321,
                        "weekday_descriptions": ["Friday: 5:00 PM – 12:00 AM"],
                        "location": {"latitude": -33.86, "longitude": 151.20},
                        "primary_photo": {
                            "name": "places/google-bar-1/photos/photo-1",
                            "width_px": 1200,
                            "height_px": 900,
                            "author_attributions": [
                                {
                                    "display_name": "Photographer",
                                    "uri": "https://maps.google.com/contrib/1",
                                    "photo_uri": "https://lh3.googleusercontent.com/a-/photo",
                                }
                            ],
                        },
                        "photos": [
                            {
                                "name": "places/google-bar-1/photos/photo-1",
                                "width_px": 1200,
                                "height_px": 900,
                                "author_attributions": [],
                            }
                        ],
                    }
                ],
                "legs": [],
                "feasibility": {
                    "all_venues_matched": True,
                    "all_open_at_plan_time": True,
                    "all_legs_under_threshold": True,
                    "summary_reasons": [],
                },
            },
            rag_documents={
                "bar-1": {
                    "fsq_place_id": "bar-1",
                    "address": "1 Date St",
                    "locality": "Sydney",
                    "region": "NSW",
                    "postcode": "2000",
                    "fsq_category_labels": ["Dining and Drinking > Bar"],
                    "crawl4ai_quality_score": 8,
                    "crawl4ai_template_stop_tags": ["cocktail_bar"],
                    "crawl4ai_ambience_tags": ["moody"],
                    "crawl4ai_setting_tags": ["rooftop"],
                    "crawl4ai_activity_tags": ["drinks"],
                    "crawl4ai_drink_tags": ["cocktails"],
                    "crawl4ai_booking_signals": ["walk-ins"],
                    "crawl4ai_evidence_snippets": [
                        "[ Discover ](https://bar.example.com/discover)",
                        "Late-night cocktails with city views.",
                    ],
                }
            },
        )

        self.assertEqual("Cocktails, Dinner & Dessert", payload["plan_title"])
        self.assertIn("Sydney CBD", payload["search_text"])
        self.assertEqual(1, len(payload["stops"]))
        stop = payload["stops"][0]
        self.assertEqual("https://bar.example.com", stop["website_uri"])
        self.assertEqual("https://maps.google.com/?cid=bar-1", stop["google_maps_uri"])
        self.assertTrue(stop["open_at_plan_time"])
        self.assertEqual("places/google-bar-1/photos/photo-1", stop["primary_photo"]["name"])
        self.assertEqual(["Dining and Drinking > Bar"], stop["categories"])
        self.assertEqual("Discover", stop["evidence_snippets"][0])
        self.assertEqual("Discover", stop["source_summary"])


if __name__ == "__main__":
    unittest.main()
