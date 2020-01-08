import requests, os

def requestalo (orcid):
    url = 'http://orcid.org/' + orcid
    headers = {'accept': 'application/xml'}
    response = requests.get(url, headers=headers)
    filename = "sumamries/" + orcid[-3:] + "/" + orcid + ".xml"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w+') as f:
        f.write(response.text)


orcid_lst = ["0000-0003-0530-4305", "0000-0001-5366-5194", "0000-0001-5506-523X", "0000-0002-7562-5203", "0000-0002-6893-7452"]
for x in orcid_lst:
    requestalo(x)