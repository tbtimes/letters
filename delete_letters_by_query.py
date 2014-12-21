"""
script will delete letter entries from Panda
based on a query of the reader-letters dataset
"""
import os, sys

import requests
import json

#Panda params
PANDA_AUTH_PARAMS = {
    'email': os.getenv('PANDA_USER'),
    'api_key': os.getenv('PANDA_API_KEY')
}
QUERY_LIMIT = 1000
PANDA_BULK_UPDATE_SIZE = 1000
PANDA_BASE = os.getenv('PANDA_BASE')
PANDA_API = '%s/api/1.0' % PANDA_BASE
PANDA_ALL_DATA_URL = "%s/data/" % PANDA_API
PANDA_DATASET_BASE = "%s/dataset" % PANDA_API
PANDA_LETTERS_BASE = '%s/reader-letters' % PANDA_DATASET_BASE
PANDA_LETTERS_DATA_URL = '%s/data/' % PANDA_LETTERS_BASE

def panda_get(url, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.get(url, params=params)

def panda_put(url, data, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.put(url, data, params=params, headers={ 'Content-Type': 'application/json' })

def panda_delete(url, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.delete(url, params=params)

tokens = ['JUnit']
query = " ".join(bit for bit in tokens)
def run_deletes(dquery):
    deleted = 0
    req = panda_get(PANDA_LETTERS_DATA_URL, params={'q': dquery, 'limit': QUERY_LIMIT})
    if req.reason == 'OK':
        dat = json.loads(req.text)
        print "found %s letter entries that match query '%s' " % (len(dat['objects']), dquery)
        entries = dat['objects']
        if entries:
            for entry in entries:
                print "deleting letter entry '%s' from Panda" % entry['external_id']
                dx = panda_delete("%s%s" % (PANDA_BASE, entry['resource_uri']))
                if dx.status_code == '204':
                    deleted += 1
    print "deleted %s entries from Panda" % deleted

if __name__ == "__main__":
    sname = os.path.basename(__file__)
    print "%s reporting for duty" % sname
    run_deletes(query)
