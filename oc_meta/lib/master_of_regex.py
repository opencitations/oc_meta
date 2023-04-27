'''
Split by ';' outside '[]' (any spaces before and after ';').
'''
semicolon_in_people_field = '\s*;\s*(?=[^\]]*(?:\[|$))'

'''
It gets string inside '[]' ignoring any space between (ex: [ TARGET  ] --> TARGET).
An id schema must be present, followed by a colon.
Before the colon, there must be any character that is not a square bracket 
to prevent that in strings like 'Boezaart, Andr[eacute] [omid:123]' the id captured is '[eacute] [omid:123]'.
Alternatively, empty square brackets containing one or more spaces also represent a valid match.
'''
ids_inside_square_brackets = '\[\s*((?:[^\s]+:[^\s]+)?(?:\s+[^\s]+:[^\s]+)*)\s*\]'

'''
It gets the name and ids in two capturing groups.
As for ids, it gets the string inside '[]' ignoring any space between (ex: [ TARGET  ] --> TARGET).
An id schema must be present, followed by a colon.
'''
name_and_ids = f'\s*(.*?)\s*{ids_inside_square_brackets}'

'''
It captures a colon preceded and followed by zero or more spaces.
'''
colon_and_spaces = '\s*:\s*'

'''
It captures a colon preceded and followed by zero or more spaces.
'''
comma_and_spaces = '\s*,\s*'

'''
It captures one or more spaces.
'''
one_or_more_spaces = '\s+'

'''
It captures any pages range separator.
'''
pages_separator = '[^A-Za-z\d]+(?=[A-Za-z\d]+)'

'''
It captures an ORCID
'''
orcid_pattern = '([0-9]{4}-){3}[0-9]{3}[0-9X]'

'''
A series of patterns useful to clean invalid "volume" and "issue" fields
'''
good_sep = '\-'
bad_sep = '&\/_,\.:+;\(\[\|'
separators = good_sep + bad_sep
alphabets = 'a-zà-öø-ÿ\u0430-я\u0391-ω' # basic latin, latin-1 supplement, cyrillic, greek
vi_pattern = f'((?:[{alphabets}]*\d[{alphabets}\d]*|[ivxlcdm]+)(?:\s?(?:[{separators}]|and|\()\s?[{alphabets}\d]+\)?)*?)'
numero = '(?:№|no?(?!v)\.?|n[º°]\.?|n[uú]m(?:[eé]ro)?|number)'
year_pattern = '(\d{4})'
valid_vi_patterns = [
    vi_pattern,
    f'[‹<\()]?[{alphabets}]+?([{separators}\s]?[{alphabets}])*[\)›>]?',
    f'[{alphabets}{separators}\s]+{vi_pattern}',
    f"[{alphabets}\d\-'/]*\d[{alphabets}\d\-'/]*(,?\s[{alphabets}\d\-'/]+)+",
    f'\(?s(uppl([eéi]ment(ary|um)?))?\)?\s?(part)?\s?(s?{vi_pattern})?',
    f'({vi_pattern}_)?({vi_pattern}\s)?[\(_]?supp?(plement|pl)?l?[\s\._]*({vi_pattern}|[{alphabets}])?\)?\.?',
    f'{vi_pattern}*,?\s?part[\s_]{vi_pattern}(\sof\s{vi_pattern})?(,\sno?\.\s?{vi_pattern})?',
    f'{vi_pattern}*[_\s]?pt?[_\s\.]{vi_pattern}',
    '(ed|pt|d)\sinside(d|r)',
    'p(ublish\s)?a(head\sof\s)?p(rint)?',
    '預刊文章',
    '[\u0621-\u064A]+',
    f'\[{year_pattern}\]\s(\d\s)?[{alphabets}]+',
    f'{vi_pattern}\s\[\+CDROM\]',
    f'{vi_pattern}[{separators}\s]?\({vi_pattern}\)(\s{vi_pattern})?',
    f'([{alphabets}]+\.)?[{alphabets}]+\.?',
    f'[{alphabets}]+-\d+',
    f'[{alphabets}]+(_[{alphabets}]+)+',
    f'{numero}:?\s?{vi_pattern}(,?\s({year_pattern}|\({vi_pattern}\)))?',
    'historica\svol\.\s\d+(,\d+(-\d+)?)?',
    '\d+\(\d+\)\d{2,4}',
    f'(\[{year_pattern}\]\s)?(\d+\s)?vl?r(\s\([a-z]+\))?',
    f'\({vi_pattern}\/{vi_pattern}\)\s[{alphabets}]+(-[{alphabets}]+)?'
]
volumes_valid_patterns = [
    'original\sseries,\svolume\s\d+',
    f'(vol(ume)?|tome|cilt)\s?[{separators}]?\s?{vi_pattern}'
]
issues_valid_patterns = [
    f'issue[\.,]?\s{vi_pattern}',
    f'({vi_pattern}\s)?e?sp?e?(ecial)?[\s_\-\.](issue)?(_number_)?[\s_-]?({vi_pattern})?(["“][{alphabets}\s]+?["”])?',
    f'ö(zel)?(\ss(ayı)?|\(special\))?(\s?{vi_pattern})?',
    f'({numero}[{separators}\s]?)?hors[{separators}\s]série[{separators}\s]{vi_pattern}',
    '특별호',
    f'([{alphabets}]+\s{year_pattern}\s)?\(?(jan(uary)?|feb(ruary)?|mar(ch)?|apr(il)?|may|jun(e)?|jul(y)?|aug(ust)?|sep(tember)?|oct(ober)?|(nov|dec)(ember)?|spring|summer|autumn|winter)(\s{year_pattern})?\)?',
    f'{vi_pattern},\spart\s{vi_pattern}\sof\s{vi_pattern}',
    f'sayı[{separators}\s]\s?{vi_pattern}',
    f'issues?\s{vi_pattern},\s(supplement|part)\s{vi_pattern}',
    f'issues?\s{vi_pattern}\.?\spp\.\s[a-z\d]+[^a-z\d]+[a-z\d]+'
]
invalid_vi_patterns = {
    f'.*?(?:vol\.?(?:ume)?|tome)(?:[{separators}]?\s?){vi_pattern}[\-&\/_,\.:+;\(\)\[\]|\s]*(?:{numero}|issues?)[{separators}|\s]*(?:sp[eé]cial\s)?{vi_pattern}': 'vol_iss',
    f'{vi_pattern},\s?{numero}\s?{vi_pattern}': 'vol_iss',
    f'tập\s?{vi_pattern},?\s?số\s?{vi_pattern}': 'vol_iss',
    f'issues?\s{vi_pattern}\svol\.?(?:ume)?\s{vi_pattern}(?:.*?{year_pattern}.*?)?': 'iss_vol_year',
    f"{vi_pattern}\s?\({vi_pattern}'{year_pattern}\)": 'vol_iss_year',
    f'cilt[{separators}\s]\s?{vi_pattern}[{separators}\s]sayı[{separators}\s]\s?{vi_pattern}(?:[{separators}\s]\s?temmuz\s{year_pattern})?': 'vol_iss_year',
    '&na;|n\/a|not\savailable': 'del',
    '[\:\-\.`ё/]': 'del',
    f'\${{[{alphabets}]+(\.[{alphabets}]+)?}}': 'del',
    f"[&\/_,:+;\|`'#]\s*{vi_pattern}": 'all',
    f'[\->+]{vi_pattern}': 'do_nothing',
    f"{vi_pattern}[\.+]": "do_nothing",
    f"{numero}?[{separators}]?\s?{vi_pattern}[&\/_,:;\|`'\(\[\{{]": 'all',
    f'{vi_pattern}\(\)': 'all',
    f'n[�?]+{vi_pattern}': 'all',
    f'{vi_pattern}(?:â\x80[\x92\x93\x94]|�+|â|\?+){vi_pattern}': 'sep',
    f'{vi_pattern}\s?\(first\sserie': 's)'
}