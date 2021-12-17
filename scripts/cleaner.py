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
        print(string)
    if 'isbn:' in string: # TODO: mettere a parte utilizzando il normalizer (single-responsability principle)
        string.replace(u'\u002D', '')
    return string
