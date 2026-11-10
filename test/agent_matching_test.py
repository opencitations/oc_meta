# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import pytest

from oc_meta.lib.agent_matching import (
    PersonName,
    name_identifies,
    names_compatible,
    names_share_word,
    normalize_name,
)


def test_normalize_name() -> None:
    assert normalize_name("  García, José-Luís ") == "garcia jose luis"


@pytest.mark.parametrize(
    "left, right",
    [
        (PersonName(given="J.", family="Smith"), PersonName("", "John", "Smith")),
        (PersonName(given="Tom", family="Doe"), PersonName("", "Thomas", "Doe")),
        (PersonName(given="Ivan", family="Иванов"), PersonName("", "Ivan", "Ivanov")),
        (PersonName(given="Tn", family="Doe"), PersonName("", "Thomas Nikolai", "Doe")),
        (PersonName(given="Jean-Luc", family="Doe"), PersonName("", "Jeanluc", "Doe")),
        (PersonName(given="Sean", family="O'Brien"), PersonName("", "Sean", "OBrien")),
        (PersonName(name="Smith John"), PersonName("", "John", "Smith")),
    ],
)
def test_names_compatible_accepts_variants(left: PersonName, right: PersonName) -> None:
    assert names_compatible(left, right) is True


@pytest.mark.parametrize(
    "left, right",
    [
        (PersonName(given="John", family="Smith"), PersonName("", "Jane", "Smith")),
        (PersonName(given="J.", family="S."), PersonName("", "John", "Smith")),
        (PersonName(given="Mary", family="Jones"), PersonName("", "Mary", "Smith")),
        (PersonName(given="Guolei", family="Liu"), PersonName("", "Gaolei", "Li")),
    ],
)
def test_names_compatible_rejects_different_people(
    left: PersonName, right: PersonName
) -> None:
    assert names_compatible(left, right) is False


@pytest.mark.parametrize(
    "left, right, expected",
    [
        (PersonName(name="S. P. Mc Kee"), PersonName(name="Shawn McKee"), True),
        (PersonName(name="Avi Denburg"), PersonName(name="Avram Denburg"), True),
        (PersonName(name="Wei Li"), PersonName(name="Bo Li"), False),
        (PersonName(name="John Smith"), PersonName(name="Ada Rossi"), False),
    ],
)
def test_names_share_word(left: PersonName, right: PersonName, expected: bool) -> None:
    assert names_share_word(left, right) is expected


@pytest.mark.parametrize(
    "candidate, reference, expected",
    [
        (PersonName(name="O. Arteaga"), PersonName(name="Oriol Arteaga"), True),
        (PersonName(name="Viet Hung Pham"), PersonName(name="Hung Pham"), True),
        (PersonName(name="He Li"), PersonName(name="He Li"), True),
        (
            PersonName(name="Mohamed M. Awad"),
            PersonName(name="Yasser Mahmoud A. Mohamed"),
            False,
        ),
        (PersonName(name="Ahmed Hossam Abozamel"), PersonName(name="Ahmed"), False),
        (PersonName(name="John Smith"), PersonName(name="Jane Smith"), False),
        (
            PersonName(name="Ying-Jun Angela Zhang"),
            PersonName(name="Ning Huang"),
            False,
        ),
        (PersonName(name="Long-Cheng Huang"), PersonName(name="Cheng Zhang"), False),
    ],
)
def test_name_identifies(
    candidate: PersonName, reference: PersonName, expected: bool
) -> None:
    assert name_identifies(candidate, reference) is expected
