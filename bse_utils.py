"""
Utility functions used by other modules - eg. getting a List of stocks

A little bit about BSE 'groups'

A Group - Highly liquid and high market cap
Z Group - Stocks not in dmat form - we are not interested in those
D Group - Stocks listed with relaxed listing norms, previously listed on RSEs
T Group - Stocks in Trade for Trade (under survelliance)
M Group - SME Perhaps?
B Group - Everything else
DT - D group Trade for trade
There are others

IP - No idea what they are

We'd be interested in A, B, T and D, DT mainly.

"""

import requests
import BeautifulSoup as bs4
import sys

from collections import namedtuple

GROUPS_INTERESTED = ('A', 'B', 'T', 'D', 'DT')

url = 'http://www.bseindia.com/corporates/List_Scrips.aspx?expandable=1'
x = requests.get(url)

if not x.ok:
    sys.exit(-1)

html = bs4.BeautifulSoup(x.text)

hidden_elems = html.findAll(attrs={'type':'hidden'})

form_data = {}
for el in hidden_elems:
    m = el.attrMap
    if m.has_key('value'):
        form_data[m['name']] = m['value']


for k,v in form_data.items():
    print k, len(v)


other_data = {
            'WINDOW_NAMER' : '1',
            'myDestination': '#',
            'ctl00$ContentPlaceHolder1$hdnCode' : '',
            'ctl00$ContentPlaceHolder1$ddSegment' : 'Equity',
            'ctl00$ContentPlaceHolder1$ddlStatus' : 'Active',
            'ctl00$ContentPlaceHolder1$getTExtData' : '',
            'ctl00$ContentPlaceHolder1$ddlGroup' : 'Select',
            'ctl00$ContentPlaceHolder1$ddlIndustry' : 'Select',
}
buttons_data = {
            'ctl00$ContentPlaceHolder1$btnSubmit.x' : '34',
            'ctl00$ContentPlaceHolder1$btnSubmit.y' : '8'
}

more_data_1 = { '__EVENTTARGET' : '', '__EVENTARGUMENT' : '' }
more_data_2 = { '__EVENTTARGET' : 'ctl00$ContentPlaceHolder1$lnkDownload', '__EVENTARGUMENT' : '' }

form_data.update(other_data)
form_data.update(buttons_data)

print form_data.keys()
y = requests.post(url, data=form_data, stream=True)
if y.ok:
    html2 = bs4.BeautifulSoup(y.text)
else:
    print y.status_code

hidden_elems = html2.findAll(attrs={'type':'hidden'})
print hidden_elems

form_data = {}
for el in hidden_elems:
    m = el.attrMap
    if m.has_key('value'):
        form_data[m['name']] = m['value']

for k,v in form_data.items():
    print k, len(v)

form_data.update(other_data)
form_data.update(more_data_2)

print form_data.keys()

y = requests.post(url, data=form_data, stream=True)
if y.ok:
    print y.text
else:
    print y.status_code
