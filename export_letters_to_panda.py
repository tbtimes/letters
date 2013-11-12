import os
import json
import requests
import csvkit
from csvkit import CSVKitDictReader as cdr
from django.template.defaultfilters import slugify
from ftplib import FTP
import datetime

# PASSWORDS AND KEYS ARE STORED IN ENVIRONMENT VARIABLES
# TO USE OUTSIDE ITS HOME ENVIRONMENT, THESE VARIABLES WILL HAVE TO BE SET MANUALLY
USER = os.getenv('PANDA_USER')
PANDA_KEY = os.getenv('PANDA_API_KEY')
FTP_LOGON = os.getenv('TIMES_LOGON')

PANDA_API = 'http://ec2-23-23-103-70.compute-1.amazonaws.com/api/1.0'
PANDA_AUTH_PARAMS = {
    'email': USER,
    'api_key': PANDA_KEY
}
PANDA_DATASET_SLUG = 'reader-letters'
PANDA_DATASET_URL = '%s/dataset/%s/' % (PANDA_API, PANDA_DATASET_SLUG)
PANDA_DATA_URL = '%s/dataset/%s/data/' % (PANDA_API, PANDA_DATASET_SLUG)
PANDA_BULK_UPDATE_SIZE = 1000
COLUMNS = ['date', 'subject', 'body', 'name', 'address', 'city', 'state/country', 'zipcode', 'phone', 'email', 'section']

def panda_get(url, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.get(url, params=params)

def panda_put(url, data, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.put(url, data, params=params, headers={ 'Content-Type': 'application/json' })

def panda_delete(url, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.delete(url, params=params)

# LETTERS_FTP = 'spwire'
# LETTERS_FTP = 'dti-xen13'
LETTERS_FTP = 'hn-dti02'
FFOLDERS = [0, 100, 200, 300, 400, 500, 600, 700, 800, 900]
processed = 0
put_data = {'objects': []}
ftp = FTP(LETTERS_FTP, USER, FTP_LOGON)

for fold in FFOLDERS:
    PATH = "/External input/archive/%s/" % fold
    try:
        ftp.cwd(PATH)
    except:
        print "couldn't find directory %s" % PATH
        continue
    else:
        print "checking folder %s" % ftp.pwd()
        postlines = []
        entries = []
        ftp.retrlines('LIST', postlines.append)
        for each in postlines:
            entries.append([bit.strip() for bit in each.split(' ') if bit.strip()])
        # for entry in entries:# ['07-31-13', '02:13PM', '1665', '0']
        for entry in entries:# ['-rwxrwxrwx', '1', 'owner', 'group', '666', 'Nov', '4', '22:27', '67']
            processed += 1
            body = []
            ftp.retrlines('RETR %s' % entry[-1], body.append)
            if body and body[0].startswith('#SOHn=LET') and 'a=' in body[0]:# we have a letter, with a name!
                # dateraw = "%s %s" % (entry[0].strip(), entry[1].strip())
                year = datetime.datetime.now().year
                dateraw = "%s %s %s %s" % (year, entry[5], entry[6], entry[7])
                dater = datetime.datetime.strptime(dateraw, "%Y %b %d %H:%M")
                DATE = dater.strftime("%Y-%m-%d %I:%M%p")
                metaline = body[0].replace('#SOHn=LET', '').replace('#STX', '')
                if 'k=' in metaline:
                    split1 = metaline.split('k=')[1].strip()
                    SECTION = ''
                else:
                    split1 = metaline.split('null')[1].strip()
                split2 = split1.split('a=')
                SECTION = split2[0].strip()
                split3 = split2[1].split('h=')#['Al Mccray, PO Box 280486, Tampa, FL/USA 33682 ', '(813)2440664, mccray@tampanewsandtalk.com']
                namer = split3[0].split(',')#['Al Mccray', ' PO Box 280486', ' Tampa', ' FL/USA 33682']
                NAME = namer[0].strip().decode('latin-1')
                ADDR = namer[1].strip()
                if len(namer) > 2:
                    CITY = namer[2].strip()
                else:
                    CITY = ''
                if len(namer)>3:
                    mixer = namer[3].split(' ')
                    STATE = mixer[0].strip()
                    if len(mixer)>1:
                        ZIP = mixer[1].strip()
                    else:
                        ZIP = ''
                else:
                    STATE, ZIP = ('', '')
                if len(split3)>1:
                    phoner = split3[1].split(',')#[(813)2440664', ' mccray@tampanewsandtalk.com']
                else:
                    phoner = []
                if len(phoner)==2:
                    PHONE = phoner[0].strip()
                    EMAIL = phoner[1].strip()
                elif len(phoner)==1:
                    if '@' in phoner[0]:
                        EMAIL = phoner[0].strip()
                        PHONE = ''
                    else:
                        EMAIL = ''
                        PHONE = phoner[0].strip()
                else:
                        EMAIL = ''
                        PHONE = ''
                SUBJ = body[1].replace('Subject:', '').strip().decode('latin-1')
                EID = slugify("%s_%s" % (SUBJ[:5], dater))
                BODY = "\n".join([bit.strip() for bit in body[2:-2] if bit.strip()])
                BODY = BODY.decode('latin-1')
                put_data['objects'].append({
                                'external_id': unicode(EID),
                                'data': [DATE, SUBJ, BODY, NAME, ADDR, CITY, STATE, ZIP, PHONE, EMAIL, SECTION]
                                })
        ftp.cwd("..")

ftp.quit()
msg = "processed %s records and picked up %s letters" % (processed, len(put_data['objects']))
print msg
panda_put(PANDA_DATA_URL, json.dumps(put_data))
