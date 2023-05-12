#!python
# Copyright 2019, Silvio Peroni <essepuntato@gmail.com>
# Copyright 2022, Giuseppe Grieco <giuseppe.grieco3@unibo.it>, Arianna Moretti <arianna.moretti4@unibo.it>, Elia Rizzetto <elia.rizzetto@studio.unibo.it>, Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


import re
from re import match, sub

from oc_idmanager.base import IdentifierManager


class ISBNManager(IdentifierManager):
    """This class implements an identifier manager for isbn identifier"""
    def __init__(self, data={}):
        """ISBN manager constructor."""
        self._p = "isbn:"
        self._data = data
        super(ISBNManager, self).__init__()

    def is_valid(self, id_string, get_extra_info=False):
        isbn = self.normalise(id_string, include_prefix=True)
        if isbn is None:
            return False
        else:
            if isbn not in self._data or self._data[isbn] is None:
                return (
                    self.check_digit(isbn)
                    and self.syntax_ok(isbn)
                )
            return self._data[isbn].get("valid")

    def normalise(self, id_string, include_prefix=False):
        try:
            isbn_string = sub("[^X0-9]", "", id_string.upper())
            return "%s%s" % (self._p if include_prefix else "", isbn_string)
        except:  # Any error in processing the ISBN will return None
            return None

    def check_digit(self, isbn):
        if isbn.startswith(self._p):
            spl = isbn.find(self._p) + len(self._p)
            isbn = isbn[spl:]

        isbn = isbn.replace("-", "")
        isbn = isbn.replace(" ", "")
        check_digit = False
        if len(isbn) == 13:
            total = 0
            val = 1
            for x in isbn:
                if x == "X":
                    x = 10
                total += int(x)*val
                val = 3 if val == 1 else val == 1
            if (total % 10) == 0:
                check_digit = True
        elif len(isbn) == 10:
            total = 0
            val = 10
            for x in isbn:
                if x == "X":
                    x = 10
                total += int(x)*val
                val -= 1
            if (total % 11) == 0:
                check_digit = True

        return check_digit

    def syntax_ok(self, id_string):
        id_string.replace(" ", "")
        id_string.replace("-", "")
        if not id_string.startswith(self._p):
            id_string = self._p+id_string
        if len(id_string) - len(self._p) == 13:
            return True if match("^isbn:97[89][0-9X]{10}$", id_string, re.IGNORECASE) else False
        elif len(id_string) - len(self._p) == 10:
            return True if match("^isbn:[0-9X]{10}$", id_string, re.IGNORECASE) else False
        else:
            return False

