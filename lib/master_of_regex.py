'''
Split by ';' outside '[]' (any spaces before and after ';').
More precisely, if there is the following pattern after the semicolon preceded and followed by zero or more spaces, do not consider that semicolon. 
The negative pattern is the following: any character excluding open square bracket zero or several times, 
followed by closed square brackets and, finally, semicolon preceded and followed by zero or more spaces or the end of the string.
Moreover, a comma is mandatory in an author/editor's name.
'''
semicolon_in_people_field = '\s*;\s*(?![^\[]*?\](?:\s*;\s*|$))(?=[^\[]*?,|$)'

'''
Split publishers by ';' outside '[]' (any spaces before and after ';').
More precisely, if it finds the following pattern after the semicolon preceded and followed by zero or more spaces, do not consider that semicolon. 
The negative pattern is the following: any character excluding open square bracket zero or several times, 
followed by closed square brackets and finally semicolon preceded and followed by zero or more spaces or the end of the string.
'''
semicolon_in_publisher_field = '\s*;\s*(?![^\[]*?\](?:\s*;\s*|$))'

'''
It gets string inside '[]' ignoring any space between (ex: [ TARGET  ] --> TARGET).
An id schema must be present, followed by a colon.
Before the colon, there must be any character that is not a square bracket 
to prevent that in strings like 'Boezaart, Andr[eacute] [meta:123]' the id captured is '[eacute] [meta:123]'.
Alternatively, empty square brackets containing one or more spaces also represent a valid match.
'''
ids_inside_square_brackets = '\[((?:\s*[^\]\[]*?:.*?\s*)|(?:\s*))\]'

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