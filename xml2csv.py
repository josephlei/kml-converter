from bs4 import BeautifulSoup

soup = BeautifulSoup(open("parking.html"))

for tables in soup.find_all('table'):
    for tr in tables.find_all('tr'):
        tds = tr.find_all('td')
        key, val = ['','']
        if len(tds) == 2:           # Check if two rows (means, key=val exists)
            key = tds[0].text
            val = tds[1].text
            if val == '<Null>': val = 'NULL'
        else:
            for tmp in tds:
                if tmp.find('table'): break
                else:
                    key = 'key'
                    val = tmp.text
        print key + " = " + val
