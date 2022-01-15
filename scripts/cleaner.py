import re
from dateutil.parser import parse
from datetime import datetime
from meta.lib.master_of_regex import *

class Cleaner:
    def __init__(self, string:str):
        '''
        :params string: the string to be cleaned. Null characters are immediately removed
        :type string: str
        '''
        self.string = string.replace("\0", "")

    def normalize_hyphens(self) -> str:
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

        :returns: str -- the string with normalized hyphens
        '''
        string = self.string
        wrong_characters = ['\u00AD', '\u2011', '\u2012', '\u2013', '\u2014', '\u2043', '\u2212']
        for c in wrong_characters:
            string = string.replace(c, '\u002D')
        if 'isbn:' in string: # TODO: mettere a parte utilizzando il normalizer (single-responsability principle)
            string.replace(u'\u002D', '')
        return string

    def clean_title(self) -> str:
        '''
        Concerning titles of bibliographic resources ('venue' and 'title' columns), 
        every word in the title is capitalized except for those that have capitals within them 
        (probably acronyms, e.g. 'FaBiO and CiTO'). This exception, however, does not include entirely capitalized titles. 
        Finally, null characters and spaces are removed.

        :returns: str -- The cleaned title
        '''
        title = self.string
        if title.isupper():
            title = title.lower()
        words = title.split()
        for i, w in enumerate(words):
            if not any(x.isupper() for x in w):
                words[i] = w.title()
        new_title = ' '.join(words)
        return new_title

    def __date_parse_hack(self, date:str) -> datetime:
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

    def clean_date(self) -> str:
        '''
        It tries to parse a date-string into a datetime object, 
        considering both the validity of the format (YYYYY-MM-DD) and the value (e.g. 30 February is not a valid date). 
        For example, a date 2020-02-30 will become 2020-02, because the day is invalid. 
        On the other hand, 2020-27-12 will become 2020 since the day
        and month are invalid. 
        If the year is not valid (e.g.year >9999) data would be totally discarded.

        :returns: str -- The cleaned date or an empty string
        '''
        date = self.string
        try:
            date = self.__date_parse_hack(date)
        except ValueError:
            try:
                # e.g. 2021-12-17
                if len(date) == 10:
                    try:
                        # Maybe only the day is invalid, try year-month
                        new_date = date[:-3]
                        date = self.__date_parse_hack(new_date)
                    except ValueError:
                        try:
                            # Maybe only the month is invalid, try year
                            new_date = date[:-6]
                            date = self.__date_parse_hack(new_date)
                        except ValueError:
                            date = ''
                # e.g. 2021-12
                elif len(date) == 7:
                    # Maybe only the month is invalid, try year
                    try:
                        new_date = date[:-3]
                        date = self.__date_parse_hack(new_date)
                    except ValueError:
                        date = ''
                else:
                    date = ''
            except ValueError:
                date = ''
        return date

    def clean_name(self) -> str:
        '''
        The first letter of each element of the name is capitalized and superfluous spaces are removed.

        :returns: str -- The cleaned name
        '''
        name = self.string
        if ',' in name:
            split_name = re.split(comma_and_spaces, name)
            first_name = split_name[1].split()
            for i, w in enumerate(first_name):
                first_name[i] = w.title()
            new_first_name = ' '.join(first_name)
            surname = split_name[0].split()
            for i, w in enumerate(surname):
                surname[i] = w.title()
            new_surname = ' '.join(surname)
            if new_surname:
                new_name = new_surname + ', ' + new_first_name
            else:
                new_name = ''
        else:
            split_name = name.split()
            for i, w in enumerate(split_name):
                split_name[i] = w.capitalize()
            new_name = ' '.join(split_name)
        return new_name
