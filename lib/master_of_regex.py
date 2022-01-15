'''
Split authors by ';' outside '[]' (any spaces before and after ';').
More precisely, if it finds the following pattern after the semicolon preceded and followed by zero or more spaces, do not consider that semicolon. 
The negative pattern is the following: any character excluding open square bracket zero or several times, 
followed by square brackets closed and finally semicolon preceded and followed by zero or more spaces or the end of the string.
'''
semicolon_in_ra_field = '\s*;\s*(?![^\[]*?\](?:\s*;\s*|$))'

'''
It gets string inside '[]' ignoring any space between (ex: [ TARGET  ] --> TARGET).
An id schema must be present, followed by a colon.
'''
id_inside_square_brackets = '\[\s*(.*?:.*?)\s*\]'

'''
It gets the name and ids in two capturing groups.
As for ids, it gets the string inside '[]' ignoring any space between (ex: [ TARGET  ] --> TARGET).
An id schema must be present, followed by a colon.
'''
name_and_ids = '\s*(.*?)\s*\[\s*(.*?:.*?)\s*\]'

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