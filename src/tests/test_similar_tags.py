from typing import Dict, List

import pandas as pd
import pytest

from features.related_profile_tags import find_similar_users


@pytest.fixture(scope="module")
def profiles():
    return pd.DataFrame(
        {
            'signer_id': [
                "user_1",
                "user_2",
                "user_3",
                "user_4",
                "user_5",
                "user_6",
                "user_7",
                "user_8",
                "user_9",
                "user_10",
            ],
            'block_timestamp': [
                1675109895737903584,
                1675109939228073527,
                1675124033638930781,
                1675199846173309175,
                1675167659888643164,
                1667619560148378089,
                1668633217817337126,
                1668636583644156265,
                1681424140795202125,
                1681424084929441284,
            ],
            'tags': [
                ['liquid-staking', 'near-protocol', 'near', 'staking'],
                ['aurora'],
                [
                    'web3',
                    'crypto',
                    'privacy',
                    'community',
                    'decentralized',
                    'engineering',
                ],
                ['community', 'dao', 'education', 'onboarding', 'events'],
                [
                    'rust',
                    'developer',
                    'dao',
                    'developer-governance',
                    'diversity',
                    'equity',
                    'inclusion',
                ],
                ['learner', 'near', 'tester'],
                ['web3', 'nft', 'defi', 'writer'],
                ['non-artist', 'chaotic', 'popcorn', 'maker'],
                ['blockchain', 'developer'],
                ['nft'],
            ],
        }
    )


def test_find_similar_users(profiles):
    result = find_similar_users(profiles, 'tags', 0, 2)

    assert len(result) == 2
    assert result[0]['score'] >= result[1]['score']

    for r in result:
        assert isinstance(r, dict)
        assert 'score' in r
        assert 'similar_profile' in r

        assert isinstance(r['score'], float)
        assert isinstance(r['similar_profile'], dict)

        assert set(['signer_id', 'block_timestamp', 'tags']).issubset(
            set(r['similar_profile'].keys())
        )

    # Test invalid index
    with pytest.raises(ValueError):
        find_similar_users(profiles, 'tags', 100, 2)

    # Test no similar users found
    with pytest.raises(ValueError):
        find_similar_users(profiles, 'tags', 0, 0)
