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



def requestHelper(stop_list):
    postData = {
        "ReportRoutes": [{
            "RouteId": "test",
            "Stops": [
                {
                    "Address": {
                        "Zip": "75050"
                    },
                    "Label": "Grand"
                },
                {
                    "Address": {
                        "Zip": "75092"
                    },
                    "Label": "Sherman"
                }
            ],
            "ReportTypes": [
                {
                    "__type": "MileageReportType:http://pcmiler.alk.com/APIs/v1.0",
                    "THoursWithSeconds": False
                }
            ]
        }]
    }
    apikey = '299354C7A83A67439273691EA750BB7F'
    try:
        url = f'https://pcmiler.alk.com/APIs/REST/v1.0/Service.svc/route/routeReports'
        params = {
            'dataVersion': 'current'
        }
        postData = postData
        headers = pcmiller.getHeaders(apikey)
        response = pcmiller.makeRequest(url, headers, urlencode(params), postData=json.dumps(postData))
        jsonobj = response.json()
        print(jsonobj)
    except requests.exceptions.ReadTimeout as e:
        print(f"Request Timed out after trying 10 times with an exponential backoff: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")