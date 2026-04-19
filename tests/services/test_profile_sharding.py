from __future__ import annotations

import unittest

import pandas as pd

from back_end.services.profile_sharding import (
    build_shard_keys,
    shard_places_dataframe,
    shard_suffix,
    stable_shard_index,
)


class ProfileShardingTestCase(unittest.TestCase):
    def test_build_shard_keys_can_use_place_ids_without_websites(self) -> None:
        places = pd.DataFrame(
            [
                {"fsq_place_id": "a", "website": None},
                {"fsq_place_id": "b", "website": None},
            ]
        )

        keys = build_shard_keys(places, shard_key="fsq_place_id")

        self.assertEqual(keys.tolist(), ["a", "b"])

    def test_build_shard_keys_keeps_duplicate_websites_together(self) -> None:
        places = pd.DataFrame(
            [
                {"fsq_place_id": "a", "website": "example.com"},
                {"fsq_place_id": "b", "website": "https://example.com/"},
                {"fsq_place_id": "c", "website": "https://other.com/menu"},
            ]
        )

        keys = build_shard_keys(places, shard_key="website")

        self.assertEqual(keys.iloc[0], keys.iloc[1])
        self.assertNotEqual(keys.iloc[0], keys.iloc[2])

    def test_domain_sharding_uses_host_identity(self) -> None:
        places = pd.DataFrame(
            [
                {"fsq_place_id": "a", "website": "https://example.com/one"},
                {"fsq_place_id": "b", "website": "https://example.com/two"},
                {"fsq_place_id": "c", "website": "https://other.com/two"},
            ]
        )

        keys = build_shard_keys(places, shard_key="domain")

        self.assertEqual(keys.iloc[0], keys.iloc[1])
        self.assertNotEqual(keys.iloc[0], keys.iloc[2])

    def test_shard_places_dataframe_partitions_without_loss(self) -> None:
        places = pd.DataFrame(
            [
                {"fsq_place_id": "a", "website": "https://example.com"},
                {"fsq_place_id": "b", "website": "https://example.com"},
                {"fsq_place_id": "c", "website": "https://other.com"},
                {"fsq_place_id": "d", "website": "https://third.com"},
                {"fsq_place_id": "e", "website": None},
            ]
        )

        shards = [
            shard_places_dataframe(
                places,
                shard_count=3,
                shard_index=index,
                shard_key="website",
            )
            for index in range(3)
        ]
        combined = pd.concat(shards, ignore_index=True)

        self.assertEqual(sorted(combined["fsq_place_id"].tolist()), sorted(places["fsq_place_id"].tolist()))
        self.assertEqual(int(combined["fsq_place_id"].duplicated().sum()), 0)

    def test_stable_shard_index_is_deterministic(self) -> None:
        first = stable_shard_index("https://example.com", shard_count=8)
        second = stable_shard_index("https://example.com", shard_count=8)

        self.assertEqual(first, second)

    def test_shard_suffix_zero_pads_outputs(self) -> None:
        self.assertEqual(shard_suffix(shard_count=12, shard_index=3), "_shard-03-of-12")


if __name__ == "__main__":
    unittest.main()
