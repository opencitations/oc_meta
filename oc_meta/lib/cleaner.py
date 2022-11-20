#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2019 Silvio Peroni <essepuntato@gmail.com>
# Copyright 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# Copyright 2021 Simone Persiani <iosonopersia@gmail.com>
# Copyright 2021-2022 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


import html
import re
from datetime import datetime
from typing import Tuple, Union

from dateutil.parser import parse
from oc_idmanager import DOIManager, ISBNManager, ISSNManager, ORCIDManager

from oc_meta.lib.master_of_regex import *
from oc_meta.lib.master_of_regex import (invalid_vi_patterns,
                                         issues_valid_patterns,
                                         volumes_valid_patterns)


class Cleaner:
    def __init__(self, string:str):
        '''
        :params string: the string to be cleaned.
        :type string: str
        '''
        self.string = string

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
        string = self.string
        wrong_characters = {'\u00AD', '\u06D4', '\u2010', '\u2011', '\u2012', '\u2013', '\u2014', '\u2043', '\u2212', '\u2796', '\u2CBA', '\uFE58'}
        for c in wrong_characters:
            string = string.replace(c, '\u002D')
        if 'isbn:' in string:
            string.replace(u'\u002D', '')
        return string
    
    def normalize_spaces(self) -> str:
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
        string = self.string
        wrong_characters = {'\u0009', '\u00A0', '&nbsp;', '\u200B', '\u202F', '\u2003', '\u2005', '\u2009'}
        for c in wrong_characters:
            string = string.replace(c, '\u0020')
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
                first_name[i] = Cleaner(w).clean_title()
            new_first_name = ' '.join(first_name)
            surname = split_name[0].split()
            for i, w in enumerate(surname):
                surname[i] = Cleaner(w).clean_title()
            new_surname = ' '.join(surname)
            if new_surname:
                new_name = new_surname + ', ' + new_first_name
            else:
                new_name = ''
        else:
            split_name = name.split()
            for i, w in enumerate(split_name):
                split_name[i] = Cleaner(w).clean_title()
            new_name = ' '.join(split_name)
        return new_name
        
    def remove_unwanted_characters(self) -> str:
        '''
        This method helps remove unwanted characters from authors' names. 
        Such characters are all characters other than letters, numbers, space, '&', apostroph, or dots that are not preceded by letters. 
        Numbers and '&' are significant if the author is an organization and not a person.
        Finally, hyphens are normalized, Unicode encodings decoded, and extra spaces removed.

        :returns: str -- The cleaned name
        '''
        unwanted_characters = {'[', ']', '{', '}', '(', ')', '?', ';', ','}
        clean_string = str()
        for i, c in enumerate(self.string):
            if c == '.':
                if self.string[i-1].isalpha():
                    clean_string += c
            elif c not in unwanted_characters:
                clean_string += c
        clean_string = ' '.join(clean_string.split()).strip()
        clean_string = html.unescape(clean_string)
        clean_string = Cleaner(clean_string).normalize_hyphens()
        return clean_string
    
    @staticmethod
    def clean_ra_list(ra_list:list) -> list:
        '''
        This method removes responsible agents reported as 'Not Available'.

        :returns: list -- The cleaned responsible agents' list
        '''
        new_ra_list = list()
        for ra in ra_list:
            if ',' in ra:
                split_name = re.split(comma_and_spaces, ra)
                first_name = split_name[1] if split_name[1].lower() != 'not available' else ''
                given_name = split_name[0] if split_name[0].lower() != 'not available' else ''
                if given_name:
                    if first_name:
                        new_ra_list.append(ra)
                    else:
                        new_ra_list.append(f'{given_name}, ')
                else:
                    continue
            else:
                if ra.lower() != 'not available':
                    new_ra_list.append(ra)
        return new_ra_list
        
    
    def normalize_id(self, valid_dois_cache:dict=dict()) -> Union[str, None]:
        '''
        This function verifies and normalizes identifiers whose schema corresponds to a DOI, an ISSN, an ISBN or an ORCID.

        :returns: Union[str, None] -- The normalized identifier if it is valid, None otherwise
        '''
        identifier = self.string.split(':', 1)
        schema = identifier[0].lower()
        value = identifier[1]
        use_api_service = True if valid_dois_cache else False
        validator = 'is_valid' if use_api_service else 'check_digit'
        if schema == 'doi':
            doi_manager = DOIManager(data=valid_dois_cache, use_api_service=use_api_service)
            valid_id = doi_manager.normalise(value, include_prefix=True) if getattr(doi_manager, validator)(value) else None
        elif schema == 'isbn':
            isbn_manager = ISBNManager()
            valid_id = isbn_manager.normalise(value, include_prefix=True) if getattr(isbn_manager, validator)(value) else None
        elif schema == 'issn':
            if value == '0000-0000':
                valid_id = None
            else:
                issn_manager = ISSNManager()
                valid_id = issn_manager.normalise(value, include_prefix=True) if getattr(issn_manager, validator)(value) else None
        elif schema == 'orcid':
            orcid_manager = ORCIDManager()
            valid_id = orcid_manager.normalise(value, include_prefix=True) if getattr(orcid_manager, validator)(value) else None
        else:
            valid_id = f'{schema}:{value}'
        return valid_id

    @classmethod
    def clean_volume_and_issue(cls, row:dict) -> None:
        output = {'volume': '', 'issue': '', 'pub_date': ''}
        for field in {'volume', 'issue'}:
            vi = row[field]
            vi = Cleaner(vi).normalize_hyphens()
            vi = Cleaner(vi).normalize_spaces().strip()
            vi = html.unescape(vi)
            for pattern, strategy in invalid_vi_patterns.items():
                pattern = f'^{pattern}$'
                capturing_groups = re.search(pattern, vi, re.IGNORECASE)
                if capturing_groups:
                    if strategy == 'del':
                        row[field] = ''
                    elif strategy == 'do_nothing':
                        row[field] = vi
                    elif strategy == 's)':
                        row[field] = f'{vi}s)'
                    else:
                        row[field] = ''
                        whatever, volume, issue, pub_date = cls.fix_invalid_vi(capturing_groups, strategy)
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
            for pattern in volumes_valid_patterns:
                pattern = f'^{pattern}$'
                if re.search(pattern, vi, re.IGNORECASE):
                    if field == 'issue':
                        switch_vi['volume'] = vi
            for pattern in issues_valid_patterns:
                pattern = f'^{pattern}$'
                if re.search(pattern, vi, re.IGNORECASE):
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
    
    @staticmethod
    def fix_invalid_vi(capturing_groups:re.Match, strategy:str) -> Tuple[str, str, str, str]:
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

    def remove_ascii(self):
        unwanted_chars = '\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x7f\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f\xa0\xa1\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf\xb0\xb1\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf\xc0\xc1\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xdb\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff'
        clean_string = ''.join([' ' if c in unwanted_chars else c for c in self.string])
        clean_string = ' '.join(clean_string.split())
        return clean_string           