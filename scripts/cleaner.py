def clean_hyphen(string:str) -> str:
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
    '''
    wrong_characters = ['\u00AD', '\u2011', '\u2012', '\u2013', '\u2014', '\u2043', '\u2212']
    for c in wrong_characters:
        string = string.replace(c, '\u002D')
    if 'isbn:' in string: # TODO: mettere a parte utilizzando il normalizer (single-responsability principle)
        string.replace(u'\u002D', '')
    return string

def clean_title(title:str):
    '''
    Concerning titles of bibliographic resources ('venue' and 'title' columns), 
    every word that composes a title is capitalized except for those that have capitals within them 
    (probably acronyms, e.g. 'FaBiO and CiTO'). This exception, however, does not include entirely capitalized titles. 
    Finally, null characters and spaces are removed.
    '''
    title = title.replace("\0", "")
    if title.isupper():
        title = title.lower()
    words = title.split()
    for i, w in enumerate(words):
        if not any(x.isupper() for x in w):
            words[i] = w.title()
    newtitle = " ".join(words)
    return newtitle
