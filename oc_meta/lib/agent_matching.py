# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from itertools import product

from anyascii import anyascii
from nicknames import NickNamer
from rapidfuzz import fuzz

NICKNAMES = NickNamer()
APOSTROPHES = str.maketrans("", "", "'’ʹʼ`")
VOWELS = set("aeiou")


@dataclass(frozen=True, slots=True)
class PersonName:
    name: str = ""
    given: str = ""
    family: str = ""

    @property
    def display(self) -> str:
        return self.name or " ".join(part for part in (self.given, self.family) if part)


@dataclass(frozen=True, slots=True)
class AlignmentPair:
    local_index: int
    external_index: int
    score: float


@dataclass(frozen=True, slots=True)
class AlignmentResult:
    pairs: tuple[AlignmentPair, ...]
    unmatched_local: tuple[int, ...]
    unmatched_external: tuple[int, ...]
    ambiguous: bool


def normalize_name(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value).casefold()
    characters = (
        character for character in decomposed if unicodedata.category(character) != "Mn"
    )
    return " ".join(
        "".join(
            character if character.isalnum() else " " for character in characters
        ).split()
    )


def script_family(value: str) -> str:
    families = set()
    for character in value:
        if not character.isalpha():
            continue
        name = unicodedata.name(character, "")
        if "CYRILLIC" in name:
            families.add("cyrillic")
        elif "GREEK" in name:
            families.add("greek")
        elif "LATIN" in name:
            families.add("latin")
        elif "CJK" in name or "HIRAGANA" in name or "KATAKANA" in name:
            families.add("cjk")
        else:
            families.add("other")
    return next(iter(families)) if len(families) == 1 else "mixed"


def _initials_compatible(left: str, right: str) -> bool:
    left_tokens = normalize_name(left).split()
    right_tokens = normalize_name(right).split()
    if not left_tokens or not right_tokens:
        return False
    limit = min(len(left_tokens), len(right_tokens))
    return all(
        left_tokens[index] == right_tokens[index]
        or (
            left_tokens[index][0] == right_tokens[index][0]
            and (len(left_tokens[index]) == 1 or len(right_tokens[index]) == 1)
        )
        for index in range(limit)
    )


def name_score(left: PersonName, right: PersonName) -> float:
    left_display = normalize_name(left.display)
    right_display = normalize_name(right.display)
    if not left_display or not right_display:
        return 0.0
    if left_display == right_display:
        return 1.0
    if set(left_display.split()) == set(right_display.split()):
        return 0.95

    left_family = normalize_name(left.family)
    right_family = normalize_name(right.family)
    if (
        left_family
        and right_family
        and left_family == right_family
        and _initials_compatible(left.given, right.given)
    ):
        return 0.9

    left_script = script_family(left.display)
    right_script = script_family(right.display)
    if left_script != right_script or "mixed" in {left_script, right_script}:
        return 0.0
    return fuzz.ratio(left_display, right_display) / 100


def _token_variants(value: str) -> set[tuple[str, ...]]:
    ascii_value = anyascii(value).translate(APOSTROPHES)
    return {
        tuple(normalize_name(ascii_value).split()),
        tuple(normalize_name(ascii_value.replace("-", "")).split()),
    }


def _name_variants(name: PersonName) -> set[tuple[str, ...]]:
    if not name.family:
        return _token_variants(name.display)
    return {
        given + family
        for given, family in product(
            _token_variants(name.given), _token_variants(name.family)
        )
    }


def _same_token(left: str, right: str) -> bool:
    return (
        left == right
        or (len(left) >= 4 and len(right) >= 4 and fuzz.ratio(left, right) >= 75)
        or left in NICKNAMES.nicknames_of(right)
        or right in NICKNAMES.nicknames_of(left)
        or bool(NICKNAMES.canonicals_of(left) & NICKNAMES.canonicals_of(right))
    )


def _token_fits(token: str, other: str) -> bool:
    return _same_token(token, other) or (
        token[0] == other[0] and (len(token) == 1 or len(other) == 1)
    )


def _split_initials(tokens: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(
        part
        for token in tokens
        for part in (
            tuple(token)
            if 1 < len(token) <= 3 and not VOWELS & set(token)
            else (token,)
        )
    )


def _tokens_fit(tokens: tuple[str, ...], other: tuple[str, ...]) -> bool:
    used: set[int] = set()
    for token in tokens:
        index = next(
            (
                index
                for index, candidate in enumerate(other)
                if index not in used and _token_fits(token, candidate)
            ),
            None,
        )
        if index is None:
            return False
        used.add(index)
    return True


def _shorter_fits(left: tuple[str, ...], right: tuple[str, ...]) -> bool:
    shorter, longer = sorted((left, right), key=len)
    return _tokens_fit(shorter, longer)


def _token_sets_compatible(left: tuple[str, ...], right: tuple[str, ...]) -> bool:
    anchored = any(
        _same_token(token, other)
        for token in left
        for other in right
        if len(token) > 1 and len(other) > 1
    )
    return anchored and (
        _shorter_fits(left, right)
        or _shorter_fits(_split_initials(left), _split_initials(right))
    )


def names_compatible(left: PersonName, right: PersonName) -> bool:
    return any(
        _token_sets_compatible(left_tokens, right_tokens)
        for left_tokens, right_tokens in product(
            _name_variants(left), _name_variants(right)
        )
    )


def align_names(
    local: list[PersonName], external: list[PersonName], gap_penalty: float = 0.45
) -> AlignmentResult:
    rows = len(local) + 1
    columns = len(external) + 1
    scores = [[0.0] * columns for _ in range(rows)]
    paths = [[1] * columns for _ in range(rows)]
    moves = [[""] * columns for _ in range(rows)]

    for row in range(1, rows):
        scores[row][0] = scores[row - 1][0] - gap_penalty
        moves[row][0] = "local"
    for column in range(1, columns):
        scores[0][column] = scores[0][column - 1] - gap_penalty
        moves[0][column] = "external"

    for row in range(1, rows):
        for column in range(1, columns):
            candidates = {
                "match": scores[row - 1][column - 1]
                + name_score(local[row - 1], external[column - 1]),
                "local": scores[row - 1][column] - gap_penalty,
                "external": scores[row][column - 1] - gap_penalty,
            }
            best = max(candidates.values())
            best_moves = [
                move for move, score in candidates.items() if abs(score - best) < 1e-9
            ]
            scores[row][column] = best
            moves[row][column] = best_moves[0]
            paths[row][column] = min(
                2,
                sum(
                    paths[row - 1][column - 1]
                    if move == "match"
                    else paths[row - 1][column]
                    if move == "local"
                    else paths[row][column - 1]
                    for move in best_moves
                ),
            )

    pairs = []
    unmatched_local = []
    unmatched_external = []
    row = len(local)
    column = len(external)
    while row or column:
        move = moves[row][column]
        if move == "match":
            pairs.append(
                AlignmentPair(
                    local_index=row - 1,
                    external_index=column - 1,
                    score=name_score(local[row - 1], external[column - 1]),
                )
            )
            row -= 1
            column -= 1
        elif move == "local":
            unmatched_local.append(row - 1)
            row -= 1
        else:
            unmatched_external.append(column - 1)
            column -= 1

    return AlignmentResult(
        pairs=tuple(reversed(pairs)),
        unmatched_local=tuple(reversed(unmatched_local)),
        unmatched_external=tuple(reversed(unmatched_external)),
        ambiguous=paths[-1][-1] > 1,
    )
