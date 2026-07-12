# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from oc_meta.lib.agent_matching import (
    AlignmentPair,
    AlignmentResult,
    PersonName,
    align_names,
    name_score,
    normalize_name,
)


def test_normalize_and_score_names() -> None:
    assert normalize_name("  García, José-Luís ") == "garcia jose luis"
    assert name_score(PersonName(name="Doe Jane"), PersonName(name="Jane Doe")) == 0.95
    assert (
        name_score(
            PersonName(given="J.", family="Doe"),
            PersonName(given="Jane", family="Doe"),
        )
        == 0.9
    )
    assert (
        name_score(PersonName(name="Иван Иванов"), PersonName(name="Ivan Ivanov"))
        == 0.0
    )
    assert (
        name_score(
            PersonName(given="John", family="Smith"),
            PersonName(given="Jane", family="Smith"),
        )
        == 0.8
    )
    assert (
        name_score(
            PersonName(family="Smith"),
            PersonName(given="Jane", family="Smith"),
        )
        == 0.6666666666666667
    )


def test_align_names_preserves_order_and_skips_unmatched_agent() -> None:
    result = align_names(
        [
            PersonName(name="Jane Doe"),
            PersonName(name="Unrelated Person"),
            PersonName(name="John Smith"),
        ],
        [PersonName(name="Jane Doe"), PersonName(name="John Smith")],
    )
    assert result == AlignmentResult(
        pairs=(
            AlignmentPair(local_index=0, external_index=0, score=1.0),
            AlignmentPair(local_index=2, external_index=1, score=1.0),
        ),
        unmatched_local=(1,),
        unmatched_external=(),
        ambiguous=False,
    )
