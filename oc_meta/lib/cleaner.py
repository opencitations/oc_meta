# SPDX-FileCopyrightText: 2019 Silvio Peroni <silvio.peroni@unibo.it>
# SPDX-FileCopyrightText: 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# SPDX-FileCopyrightText: 2021 Simone Persiani <iosonopersia@gmail.com>
# SPDX-FileCopyrightText: 2021-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import html
import re
from collections import OrderedDict
from datetime import datetime
from typing import Tuple, Union

from dateutil.parser import parse
from oc_ds_converter.oc_idmanager import (DOIManager, ISBNManager, ISSNManager,
                                          ORCIDManager)

from oc_meta.lib.master_of_regex import (
    RE_COMMA_AND_SPACES,
    RE_INVALID_VI_PATTERNS,
    RE_ISSUES_VALID_PATTERNS,
    RE_VOLUMES_VALID_PATTERNS,
    split_name_and_ids,
)

_HYPHEN_TRANS = str.maketrans({
    '\u00AD': '\u002D',  # Soft hyphen
    '\u06D4': '\u002D',  # Arabic Full Stop
    '\u2010': '\u002D',  # Hyphen
    '\u2011': '\u002D',  # Non-breaking Hyphen
    '\u2012': '\u002D',  # Figure Dash
    '\u2013': '\u002D',  # En-Dash
    '\u2014': '\u002D',  # Em-Dash
    '\u2043': '\u002D',  # Hyphen Bullet
    '\u2212': '\u002D',  # Minus Sign
    '\u2796': '\u002D',  # Heavy Minus Sign
    '\u2CBA': '\u002D',  # Coptic Capital Letter Dialect-p Ni
    '\uFE58': '\u002D',  # Small Em Dash
})

_SPACE_TRANS = str.maketrans({
    '\u0009': '\u0020',  # Character Tabulation
    '\u00A0': '\u0020',  # No-break space
    '\u200B': '\u0020',  # Zero width space
    '\u202F': '\u0020',  # Narrow no-break space
    '\u2003': '\u0020',  # Em Space
    '\u2005': '\u0020',  # Four-Per-Em Space
    '\u2009': '\u0020',  # Thin Space
})

# Translation table for control characters and extended ASCII to space
# Covers: 0x00-0x1F (control chars), 0x7F (DEL), 0x80-0xFF (extended ASCII)
_ASCII_CONTROL_TRANS = str.maketrans(
    {chr(i): ' ' for i in range(0x00, 0x20)}
    | {chr(0x7F): ' '}
    | {chr(i): ' ' for i in range(0x80, 0x100)}
)

_DOI_MANAGER = DOIManager(use_api_service=False, storage_manager=None)
_ISBN_MANAGER = ISBNManager()
_ISSN_MANAGER = ISSNManager()
_ORCID_MANAGER = ORCIDManager(use_api_service=False, storage_manager=None)


def normalize_hyphens(string: str) -> str:
    '''
    It replaces any hyphen, dash and minus sign with a hyphen-minus character.
    This is done for pages, IDs and dates.

    .. list-table:: Comparison between the various characters similar to hyphen-minus
        :widths: 25 25 50
        :header-rows: 1

        * - UTF-8
            - SIGN
            - NAME
        * - U+002D
            - -
            - Hyphen-minus
        * - U+00AD
            - ­
            - Soft hyphen
        * - U+06D4
            - ۔
            - Arabic Full Stop
        * - U+2010
            - ‐
            - Hyphen
        * - U+2011
            - −
            - Non-breaking Hyphen
        * - U+2012
            - –
            - Figure Dash
        * - U+2013
            - –
            - En-Dash
        * - U+2014
            - —
            - Em-Dash
        * - U+2043
            - ⁃
            - Hyphen Bullet
        * - U+2212
            - −
            - Minus Sign
        * - U+2796
            - ➖
            - Heavy Minus Sign
        * - U+2CBA
            - Ⲻ
            - Coptic Capital Letter Dialect-p Ni
        * - U+FE58
            - ﹘
            - Small Em Dash

    :returns: str -- the string with normalized hyphens
    '''
    return string.translate(_HYPHEN_TRANS)


def normalize_spaces(string: str) -> str:
    '''
    It replaces any ambiguous spaces with a space.

    .. list-table:: List of the various characters similar to the space
        :widths: 25 25 50
        :header-rows: 1

        * - UTF-8
            - NAME
        * - U+0020
            - Space
        * - U+0009
            - Character Tabulation
        * - U+00A0
            - No-break space
        * - U+200B
            - Zero width space
        * - U+202F
            - Narrow no-break space
        * - U+2003
            - Em Space
        * - U+2005
            - Four-Per-Em Space
        * - U+2009
            - Thin Space

    :returns: str -- the string with normalized spaces
    '''
    return string.translate(_SPACE_TRANS).replace('&nbsp;', '\u0020')


def clean_title(string: str, normalize: bool = True) -> str:
    '''
    Concerning titles of bibliographic resources ('venue' and 'title' columns),
    every word in the title is capitalized except for those that have capitals within them
    (probably acronyms, e.g. 'FaBiO and CiTO'). This exception, however, does not include entirely capitalized titles.
    Finally, null characters and spaces are removed.

    :returns: str -- The cleaned title
    '''
    title = string
    if normalize:
        if title.isupper():
            title = title.lower()
        words = title.split()
        for i, w in enumerate(words):
            if not any(x.isupper() for x in w):
                words[i] = w.title()
        return ' '.join(words)
    return title


def _date_parse_hack(date: str) -> str:
    dt = parse(date, default=datetime(2001, 1, 1))
    dt2 = parse(date, default=datetime(2002, 2, 2))

    if dt.year == dt2.year and dt.month == dt2.month and dt.day == dt2.day:
        clean_date = parse(date).strftime('%Y-%m-%d')
    elif dt.year == dt2.year and dt.month == dt2.month:
        clean_date = parse(date).strftime('%Y-%m')
    elif dt.year == dt2.year:
        clean_date = parse(date).strftime('%Y')
    else:
        clean_date = ''
    return clean_date


def clean_date(string: str) -> str:
    '''
    It tries to parse a date-string into a datetime object,
    considering both the validity of the format (YYYYY-MM-DD) and the value (e.g. 30 February is not a valid date).
    For example, a date 2020-02-30 will become 2020-02, because the day is invalid.
    On the other hand, 2020-27-12 will become 2020 since the day
    and month are invalid.
    If the year is not valid (e.g.year >9999) data would be totally discarded.

    :returns: str -- The cleaned date or an empty string
    '''
    date = string
    try:
        date = _date_parse_hack(date)
    except ValueError:
        try:
            # e.g. 2021-12-17
            if len(date) == 10:
                try:
                    # Maybe only the day is invalid, try year-month
                    new_date = date[:-3]
                    date = _date_parse_hack(new_date)
                except ValueError:
                    try:
                        # Maybe only the month is invalid, try year
                        new_date = date[:-6]
                        date = _date_parse_hack(new_date)
                    except ValueError:
                        date = ''
            # e.g. 2021-12
            elif len(date) == 7:
                # Maybe only the month is invalid, try year
                try:
                    new_date = date[:-3]
                    date = _date_parse_hack(new_date)
                except ValueError:
                    date = ''
            else:
                date = ''
        except ValueError:
            date = ''
    return date


def clean_name(string: str) -> str:
    '''
    The first letter of each element of the name is capitalized and superfluous spaces are removed.

    :returns: str -- The cleaned name
    '''
    name = string
    if ',' in name:
        split_name = RE_COMMA_AND_SPACES.split(name)
        first_name = split_name[1].split()
        for i, w in enumerate(first_name):
            first_name[i] = clean_title(w)
        new_first_name = ' '.join(first_name)
        surname = split_name[0].split()
        for i, w in enumerate(surname):
            surname[i] = clean_title(w)
        new_surname = ' '.join(surname)
        if new_surname:
            return new_surname + ', ' + new_first_name
        return ''
    split_name = name.split()
    for i, w in enumerate(split_name):
        split_name[i] = clean_title(w)
    return ' '.join(split_name)


def clean_agent_name(string: str) -> str:
    '''
    Clean a responsible agent name (author, editor, publisher).

    Removes unwanted characters while preserving letters, numbers, spaces,
    '&', apostrophes, and dots preceded by letters. Numbers and '&' are
    kept for organization names (e.g., "3M", "Smith & Sons").
    Normalizes hyphens, decodes HTML entities, and removes extra spaces.

    :returns: str -- The cleaned agent name.
    '''
    unwanted_characters = {'[', ']', ';', '?'}
    chars = []
    for i, c in enumerate(string):
        if c == '.':
            if i > 0 and string[i-1].isalpha():
                chars.append(c)
        elif c not in unwanted_characters:
            chars.append(c)
    clean_string = ' '.join(''.join(chars).split())
    clean_string = html.unescape(clean_string)
    clean_string = clean_string.translate(_HYPHEN_TRANS)
    return clean_string


def _normalize_ra_name(raw_name: str) -> str:
    """Normalize a RA name into one of: '', 'Full Name', 'Last, First', 'Last, '.

    Returns '' when the name is absent, literally 'Not Available', or a
    comma-separated pair whose surname is missing. Bare names are run
    through :func:`clean_agent_name` to drop bracket / punctuation junk.
    """
    name = raw_name.strip()
    if not name:
        return ''
    if ',' in name:
        last, _, first = name.partition(',')
        last = last.strip()
        first = first.strip()
        if last.lower() == 'not available':
            last = ''
        if first.lower() == 'not available':
            first = ''
        if not last:
            return ''
        return f'{last}, {first}' if first else f'{last}, '
    cleaned = clean_agent_name(name)
    if cleaned.lower() == 'not available':
        return ''
    return cleaned


def clean_ra_list(ra_list: list) -> list:
    '''
    Clean a list of responsible agents: normalize names, drop 'Not Available'
    entries, remove duplicates that share a name and at least one id, and
    strip identifiers that appear under more than one agent.

    :returns: list -- The cleaned responsible agents' list
    '''

    # Phase 1: parse each entry into (key, name, ids). The key groups entries
    # that belong to the same id bucket: named entries by their normalized
    # name, nameless (ids-only) entries by the raw input so each stays
    # distinct.
    parsed: list[tuple[str, str, list[str]]] = []
    agents_ids: OrderedDict[str, OrderedDict[str, None]] = OrderedDict()
    for ra in ra_list:
        raw_name, ids_str = split_name_and_ids(ra)
        name = _normalize_ra_name(raw_name)
        ids = ids_str.split()
        if not name and not ids:
            continue
        key = name or ra
        parsed.append((key, name, ids))
        if ids:
            agents_ids.setdefault(key, OrderedDict()).update(
                OrderedDict.fromkeys(ids)
            )

    # Phase 2: identifiers bucketed under more than one key are shared and
    # must be dropped — they cannot unambiguously identify a single agent.
    id_occurrences: dict[str, int] = {}
    for bucket in agents_ids.values():
        for identifier in bucket:
            id_occurrences[identifier] = id_occurrences.get(identifier, 0) + 1
    shared_ids = {i for i, count in id_occurrences.items() if count > 1}

    # Phase 3: emit cleaned entries in input order, dropping later duplicates
    # that share at least one surviving id with a previous entry of the same
    # name.
    output: list[str] = []
    seen_ids_by_name: OrderedDict[str, set[str]] = OrderedDict()
    for _, name, ids in parsed:
        kept_ids = [i for i in ids if i not in shared_ids]
        kept_ids_str = ' '.join(kept_ids)
        if not name:
            output.append(f'[{kept_ids_str}]')
            continue
        kept_set = set(kept_ids)
        if name in seen_ids_by_name and seen_ids_by_name[name] & kept_set:
            continue
        seen_ids_by_name.setdefault(name, set()).update(kept_set)
        output.append(f'{name} [{kept_ids_str}]' if kept_ids else name)
    return output


def normalize_id(string: str) -> Union[str, None]:
    '''
    This function verifies and normalizes identifiers whose schema corresponds to a DOI, an ISSN, an ISBN or an ORCID.

    :returns: Union[str, None] -- The normalized identifier if it is valid, None otherwise
    '''
    identifier = string.split(':', 1)
    schema = identifier[0].lower()
    value = identifier[1]
    if schema == 'doi':
        valid_id = _DOI_MANAGER.normalise(value, include_prefix=True) if _DOI_MANAGER.syntax_ok(value) else None
    elif schema == 'isbn':
        valid_id = _ISBN_MANAGER.normalise(value, include_prefix=True) if _ISBN_MANAGER.is_valid(value, get_extra_info=False) else None
    elif schema == 'issn':
        if value == '0000-0000':
            valid_id = None
        else:
            try:
                valid_id = _ISSN_MANAGER.normalise(value, include_prefix=True) if _ISSN_MANAGER.is_valid(value, get_extra_info=False) else None
            except ValueError:
                print(value)
                raise(ValueError)
    elif schema == 'orcid':
        valid_id = _ORCID_MANAGER.normalise(value, include_prefix=True) if _ORCID_MANAGER.is_valid(value, get_extra_info=False) else None
    else:
        valid_id = f'{schema}:{value}'
    return valid_id


def clean_volume_and_issue(row: dict) -> None:
    output = {'volume': '', 'issue': '', 'pub_date': ''}
    for field in {'volume', 'issue'}:
        vi = row[field]
        vi = normalize_hyphens(vi)
        vi = normalize_spaces(vi).strip()
        vi = html.unescape(vi)
        for compiled_pattern, strategy in RE_INVALID_VI_PATTERNS.items():
            capturing_groups = compiled_pattern.search(vi)
            if capturing_groups:
                if strategy == 'del':
                    row[field] = ''
                elif strategy == 'do_nothing':
                    row[field] = vi
                elif strategy == 's)':
                    row[field] = f'{vi}s)'
                else:
                    row[field] = ''
                    whatever, volume, issue, pub_date = _fix_invalid_vi(capturing_groups, strategy)
                    row[field] = whatever if whatever else row[field]
                    output['volume'] = volume if volume else ''
                    output['issue'] = issue if issue else ''
                    output['pub_date'] = pub_date if pub_date else ''
    row['volume'] = output['volume'] if not row['volume'] else row['volume']
    row['issue'] = output['issue'] if not row['issue'] else row['issue']
    row['pub_date'] = output['pub_date'] if not row['pub_date'] else row['pub_date']
    switch_vi = {'volume': '', 'issue': ''}
    for field in {'volume', 'issue'}:
        vi = row[field]
        for compiled_pattern in RE_VOLUMES_VALID_PATTERNS:
            if compiled_pattern.search(vi):
                if field == 'issue':
                    switch_vi['volume'] = vi
        for compiled_pattern in RE_ISSUES_VALID_PATTERNS:
            if compiled_pattern.search(vi):
                if field == 'volume':
                    switch_vi['issue'] = vi
    if switch_vi['volume'] and switch_vi['issue']:
        row['volume'] = switch_vi['volume']
        row['issue'] = switch_vi['issue']
    elif switch_vi['volume'] and not row['volume']:
        row['volume'] = switch_vi['volume']
        row['issue'] = ''
        row['type'] = 'journal volume' if row['type'] == 'journal issue' else row['type']
    elif switch_vi['issue'] and not row['issue']:
        row['issue'] = switch_vi['issue']
        row['volume'] = ''
        row['type'] = 'journal issue' if row['type'] == 'journal volume' else row['type']


def _fix_invalid_vi(capturing_groups: re.Match, strategy: str) -> Tuple[str | None, str | None, str | None, str | None]:
    vol_group = 1 if 'vol_iss' in strategy else 2
    iss_group = 1 if 'iss_vol' in strategy else 2
    whatever = None
    volume = None
    issue = None
    pub_date = None
    if 'vol' in strategy and 'iss' in strategy:
        volume = capturing_groups.group(vol_group)
        issue = capturing_groups.group(iss_group)
        if 'year' in strategy:
            pub_date = capturing_groups.group(3)
    elif strategy == 'all':
        whatever = capturing_groups.group(1)
    elif strategy == 'sep':
        first = capturing_groups.group(1)
        second = capturing_groups.group(2)
        whatever = f'{first}-{second}'
    return whatever, volume, issue, pub_date


def remove_ascii(string: str) -> str:
    clean_string = string.translate(_ASCII_CONTROL_TRANS)
    return ' '.join(clean_string.split())
