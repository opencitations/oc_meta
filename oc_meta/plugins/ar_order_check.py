AR_DICT = {
    '062203405093': {'next': '062203405094', 'ra': '0623031163'}, 
    '062203405094': {'next': '062203405095', 'ra': '062202383008'}, 
    '062203405095': {'next': '062203405096', 'ra': '066039319'}, 
    '062203405096': {'next': '062203405097', 'ra': '062202383009'}, 
    '062203405097': {'next': '062203405098', 'ra': '062202383010'}, 
    '062203405098': {'next': '062203405099', 'ra': '0613046025'}, 
    '062203405099': {'next': '062203405100', 'ra': '062202383011'}, 
    '062203405100': {'next': '062203405101', 'ra': '062202383012'}, 
    '062203405101': {'next': '062203405102', 'ra': '062202383013'}, 
    '062203405102': {'next': '', 'ra': '0612046979'}, 
    '06015555539': {'next': '06015555540', 'ra': '06011061209'}, 
    '06015555540': {'next': '06015555542', 'ra': '061303739'}, 
    '06015555541': {'next': '06015555542', 'ra': '0612046979'}, 
    '06015555542': {'next': '06015555543', 'ra': '06011061210'}, 
    '06015555543': {'next': '06015555544', 'ra': '06011061211'}, 
    '06015555544': {'next': '06015555545', 'ra': '06011061212'}, 
    '06015555545': {'next': '062203405093', 'ra': '06011061213'}}
ar_list = list()
last = ''
count = 0
while count < len(AR_DICT):
    print(last)
    for ar_metaid, ar_data in AR_DICT.items():
        if ar_data['next'] == last:
            ar_dic = dict()
            ar_dic[ar_metaid] = ''
            ar_list.append(ar_dic)
            last = ar_metaid
            count += 1
ar_list.reverse()