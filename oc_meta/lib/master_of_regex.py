# SPDX-FileCopyrightText: 2022-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import re

# Split by ';' outside '[]' (any spaces before and after ';').
semicolon_in_people_field = r'\s*;\s*(?=[^\]]*(?:\[|$))'

# A single id token "schema:value". Square brackets are excluded from both
# sides of the colon so a stray '[' that leaks into the name (e.g.
# '[Labour Party[ [omid:ra/123]') cannot be absorbed into the captured id.
_ID_TOKEN = r'[^\s\[\]]+:[^\s\[\]]+'

# It captures a colon preceded and followed by zero or more spaces.
colon_and_spaces = r'\s*:\s*'

# It captures a comma preceded and followed by zero or more spaces.
comma_and_spaces = r'\s*,\s*'

# It captures one or more spaces.
one_or_more_spaces = r'\s+'

RE_ENTITY_URI = re.compile(r'^(?P<base>https://w3id\.org/oc/meta)/(?P<short_name>br|ra|ar|re|id)/(?P<supplier_prefix>06[1-9]*0)(?P<entity_number>[1-9]\d*)$')
RE_SEMICOLON_IN_PEOPLE_FIELD = re.compile(semicolon_in_people_field)

# Parses a responsible-agent / venue cell into a name and an optional list of
# ids inside square brackets. Always matches any input: when the '[ids]' block
# is present the engine captures it (preferring the first occurrence in a
# multi-RA string), otherwise the whole (trimmed) cell lands in 'name' and
# 'ids' is None. Downstream code can therefore rely on .groups() always
# succeeding; a regex failure indicates a real bug and should crash.
RE_NAME_AND_IDS = re.compile(
    rf'''
    \s*
    (?P<name> .*? )                     # name, possibly empty, possibly with junk
    (?:
          \s* \[ \s*
          (?P<ids>
              (?: {_ID_TOKEN} )?        # optional first id ...
              (?: \s+ {_ID_TOKEN} )*    # ... followed by any number of space-separated ids
          )
          \s* \]
        | \s* \Z                        # or no '[ids]' block at all
    )
    ''',
    re.VERBOSE,
)


def split_name_and_ids(text: str) -> tuple[str, str]:
    """Parse a responsible-agent / venue cell into ``(name, ids_str)``.

    ``RE_NAME_AND_IDS`` is built to match any input, so a ``None`` here
    signals a regression in the pattern itself and must be raised loudly
    rather than silently fallen back to.
    """
    match = RE_NAME_AND_IDS.match(text)
    if match is None:
        raise RuntimeError(f"RE_NAME_AND_IDS failed to match {text!r}")
    return match["name"], match["ids"] or ""


RE_COLON_AND_SPACES = re.compile(colon_and_spaces)
RE_COMMA_AND_SPACES = re.compile(comma_and_spaces)
RE_ONE_OR_MORE_SPACES = re.compile(one_or_more_spaces)
RE_MULTIPLE_SPACES = re.compile(r'\s+')

# It captures any pages range separator.
pages_separator = r'[^A-Za-z\d]+(?=[A-Za-z\d]+)'

# It captures an ORCID
orcid_pattern = r'([0-9]{4}-){3}[0-9]{3}[0-9X]'

# A series of patterns useful to clean invalid "volume" and "issue" fields
good_sep = r'\-'
bad_sep = r'&\/_,\.:+;\(\[\|'
separators = good_sep + bad_sep
alphabets = r'a-zà-öø-ÿ\u0430-я\u0391-ω'  # basic latin, latin-1 supplement, cyrillic, greek
vi_pattern = fr'((?:[{alphabets}]*\d[{alphabets}\d]*|[ivxlcdm]+)(?:\s?(?:[{separators}]|and|\()\s?[{alphabets}\d]+\)?)*?)'
numero = r'(?:№|no?(?!v)\.?|n[º°]\.?|n[uú]m(?:[eé]ro)?|number)'
year_pattern = r'(\d{4})'
valid_vi_patterns = [
    vi_pattern,
    fr'[‹<\()]?[{alphabets}]+?([{separators}\s]?[{alphabets}])*[\)›>]?',
    fr'[{alphabets}{separators}\s]+{vi_pattern}',
    fr"[{alphabets}\d\-'/]*\d[{alphabets}\d\-'/]*(,?\s[{alphabets}\d\-'/]+)+",
    fr'\(?s(uppl([eéi]ment(ary|um)?))?\)?\s?(part)?\s?(s?{vi_pattern})?',
    fr'({vi_pattern}_)?({vi_pattern}\s)?[\(_]?supp?(plement|pl)?l?[\s\._]*({vi_pattern}|[{alphabets}])?\)?\.?',
    fr'{vi_pattern}*,?\s?part[\s_]{vi_pattern}(\sof\s{vi_pattern})?(,\sno?\.\s?{vi_pattern})?',
    fr'{vi_pattern}*[_\s]?pt?[_\s\.]{vi_pattern}',
    r'(ed|pt|d)\sinside(d|r)',
    r'p(ublish\s)?a(head\sof\s)?p(rint)?',
    '預刊文章',
    '[\u0621-\u064A]+',
    fr'\[{year_pattern}\]\s(\d\s)?[{alphabets}]+',
    fr'{vi_pattern}\s\[\+CDROM\]',
    fr'{vi_pattern}[{separators}\s]?\({vi_pattern}\)(\s{vi_pattern})?',
    fr'([{alphabets}]+\.)?[{alphabets}]+\.?',
    fr'[{alphabets}]+-\d+',
    fr'[{alphabets}]+(_[{alphabets}]+)+',
    fr'{numero}:?\s?{vi_pattern}(,?\s({year_pattern}|\({vi_pattern}\)))?',
    r'historica\svol\.\s\d+(,\d+(-\d+)?)?',
    r'\d+\(\d+\)\d{2,4}',
    fr'(\[{year_pattern}\]\s)?(\d+\s)?vl?r(\s\([a-z]+\))?',
    fr'\({vi_pattern}\/{vi_pattern}\)\s[{alphabets}]+(-[{alphabets}]+)?'
]
volumes_valid_patterns = [
    r'original\sseries,\svolume\s\d+',
    fr'(vol(ume)?|tome|cilt)\s?[{separators}]?\s?{vi_pattern}'
]
issues_valid_patterns = [
    fr'issue[\.,]?\s{vi_pattern}',
    fr'({vi_pattern}\s)?e?sp?e?(ecial)?[\s_\-\.](issue)?(_number_)?[\s_-]?({vi_pattern})?(["“][{alphabets}\s]+?["”])?',
    fr'ö(zel)?(\ss(ayı)?|\(special\))?(\s?{vi_pattern})?',
    fr'({numero}[{separators}\s]?)?hors[{separators}\s]série[{separators}\s]{vi_pattern}',
    '특별호',
    fr'([{alphabets}]+\s{year_pattern}\s)?\(?(jan(uary)?|feb(ruary)?|mar(ch)?|apr(il)?|may|jun(e)?|jul(y)?|aug(ust)?|sep(tember)?|oct(ober)?|(nov|dec)(ember)?|spring|summer|autumn|winter)(\s{year_pattern})?\)?',
    fr'{vi_pattern},\spart\s{vi_pattern}\sof\s{vi_pattern}',
    fr'sayı[{separators}\s]\s?{vi_pattern}',
    fr'issues?\s{vi_pattern},\s(supplement|part)\s{vi_pattern}',
    fr'issues?\s{vi_pattern}\.?\spp\.\s[a-z\d]+[^a-z\d]+[a-z\d]+'
]
invalid_vi_patterns = {
    fr'.*?(?:vol\.?(?:ume)?|tome)(?:[{separators}]?\s?){vi_pattern}[\-&\/_,\.:+;\(\)\[\]|\s]*(?:{numero}|issues?)[{separators}|\s]*(?:sp[eé]cial\s)?{vi_pattern}': 'vol_iss',
    fr'{vi_pattern},\s?{numero}\s?{vi_pattern}': 'vol_iss',
    fr'tập\s?{vi_pattern},?\s?số\s?{vi_pattern}': 'vol_iss',
    fr'issues?\s{vi_pattern}\svol\.?(?:ume)?\s{vi_pattern}(?:.*?{year_pattern}.*?)?': 'iss_vol_year',
    fr"{vi_pattern}\s?\({vi_pattern}'{year_pattern}\)": 'vol_iss_year',
    fr'cilt[{separators}\s]\s?{vi_pattern}[{separators}\s]sayı[{separators}\s]\s?{vi_pattern}(?:[{separators}\s]\s?temmuz\s{year_pattern})?': 'vol_iss_year',
    r'&na;|n\/a|not\savailable': 'del',
    r'[\:\-\.`ё/]': 'del',
    fr'\${{[{alphabets}]+(\.[{alphabets}]+)?}}': 'del',
    fr"[&\/_,:+;\|`'#]\s*{vi_pattern}": 'all',
    fr'[\->+]{vi_pattern}': 'do_nothing',
    fr"{vi_pattern}[\.+]": "do_nothing",
    fr"{numero}?[{separators}]?\s?{vi_pattern}[&\/_,:;\|`'\(\[\{{]": 'all',
    fr'{vi_pattern}\(\)': 'all',
    fr'n[�?]+{vi_pattern}': 'all',
    fr'{vi_pattern}(?:â\x80[\x92\x93\x94]|�+|â|\?+){vi_pattern}': 'sep',
    fr'{vi_pattern}\s?\(first\sserie': 's)'
}

RE_INVALID_VI_PATTERNS = {
    re.compile(f'^{pattern}$', re.IGNORECASE): strategy
    for pattern, strategy in invalid_vi_patterns.items()
}
RE_VOLUMES_VALID_PATTERNS = [
    re.compile(f'^{pattern}$', re.IGNORECASE)
    for pattern in volumes_valid_patterns
]
RE_ISSUES_VALID_PATTERNS = [
    re.compile(f'^{pattern}$', re.IGNORECASE)
    for pattern in issues_valid_patterns
]