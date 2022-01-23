'''
Split by ';' outside '[]' (any spaces before and after ';').
'''
semicolon_in_people_field = '\s*;\s*(?=[^\]]*(?:\[|$))'

'''
It gets string inside '[]' ignoring any space between (ex: [ TARGET  ] --> TARGET).
An id schema must be present, followed by a colon.
Before the colon, there must be any character that is not a square bracket 
to prevent that in strings like 'Boezaart, Andr[eacute] [meta:123]' the id captured is '[eacute] [meta:123]'.
Alternatively, empty square brackets containing one or more spaces also represent a valid match.
'''
ids_inside_square_brackets = '\[\s*(.*?)\s*\]'

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