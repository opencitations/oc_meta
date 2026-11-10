# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import unicodedata
from collections.abc import Iterator
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


def _variant_pairs(
    left: PersonName, right: PersonName
) -> Iterator[tuple[tuple[str, ...], tuple[str, ...]]]:
    return product(_name_variants(left), _name_variants(right))


def names_compatible(left: PersonName, right: PersonName) -> bool:
    return any(
        _token_sets_compatible(left_tokens, right_tokens)
        for left_tokens, right_tokens in _variant_pairs(left, right)
    )


def _joined_tokens(tokens: tuple[str, ...]) -> set[str]:
    return {
        "".join(tokens[start:end])
        for start in range(len(tokens))
        for end in range(start + 2, len(tokens) + 1)
    }


def _tokens_share_word(left: tuple[str, ...], right: tuple[str, ...]) -> bool:
    return (
        any(
            _same_token(token, other)
            for token in left
            for other in right
            if len(token) > 2 and len(other) > 2
        )
        or bool(_joined_tokens(left) & set(right))
        or bool(set(left) & _joined_tokens(right))
    )


def names_share_word(left: PersonName, right: PersonName) -> bool:
    return any(
        _tokens_share_word(left_tokens, right_tokens)
        for left_tokens, right_tokens in _variant_pairs(left, right)
    )


def _enough_words_match(candidate: tuple[str, ...], reference: tuple[str, ...]) -> bool:
    words = [token for token in candidate if len(token) > 1]
    matched = sum(word in reference for word in words)
    return matched >= min(2, len(words))


def name_identifies(candidate: PersonName, reference: PersonName) -> bool:
    return names_compatible(candidate, reference) and any(
        _enough_words_match(candidate_tokens, reference_tokens)
        for candidate_tokens, reference_tokens in _variant_pairs(candidate, reference)
    )
