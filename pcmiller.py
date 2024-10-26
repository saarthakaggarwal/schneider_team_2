import requests
from urllib.parse import urlencode
import backoff
import json

def getHeaders(apikey):
    headers = {
        'Authorization': f'{apikey}',
        'Accept': 'application/json',
        'Content-type': 'application/json'
    }
    return headers

@backoff.on_exception(backoff.expo,
                      requests.exceptions.ReadTimeout, max_time=10)
def makeRequest(url, headers, params, postData):
    response = requests.post(url, headers=headers, params=params, data=postData, timeout=10)
    return response

