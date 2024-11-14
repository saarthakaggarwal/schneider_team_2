import requests
from urllib.parse import urlencode
import backoff
import json
from PIL import Image
from io import BytesIO


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
            "Stops": stop_list,
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
        headers = getHeaders(apikey)
        response = makeRequest(url, headers, urlencode(params), postData=json.dumps(postData))
        jsonobj = response.json()
        return jsonobj
    except requests.exceptions.ReadTimeout as e:
        print(f"Request Timed out after trying 10 times with an exponential backoff: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        

def routeCalc(stop_list, assetId=None, region=4):
    # Construct the stops parameter from stop_list
    stops = ';'.join([f"{stop['Lon']},{stop['Lat']}" for stop in stop_list])

    # Define the request parameters
    params = {
        'stops': stops,
        'assetId': assetId,
        'region': region
    }

    apikey = '299354C7A83A67439273691EA750BB7F'
    
    try:
        url = 'https://pcmiler.alk.com/apis/rest/v1.0/Service.svc/route/routePath'
        headers = getHeaders(apikey)

        # Send the GET request with parameters
        response = requests.get(url, headers=headers, params=params)

        # Check if the response was successful
        if response.status_code == 200:
            jsonobj = response.json()
            return jsonobj
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")

    except requests.exceptions.ReadTimeout as e:
        print(f"Request timed out: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")



def mapGenerator(stop_list):
    postData = {
        "Map": {
            "Viewport": {
                "Center": None,
                "ScreenCenter": None,
                "ZoomRadius": 0,
                "CornerA": None,
                "CornerB": None,
                "Region": 0
            },
            "Projection": 0,
            "Style": 0,
            "ImageOption": 0,
            "Width": 1366,
            "Height": 768,
            "Drawers": [8, 2, 7, 17, 15],
            "LegendDrawer": [
                {
                    "Type": 0,
                    "DrawOnMap": True
                }
            ],
            "MapLayering": 0
        },
        "Routes": [
            {
                "RouteId": None,
                "Stops": [
                    {
                        "Address": {
                            "City": stop["City"],
                            "State": stop["State"],
                            "Zip": stop.get("Zip", ""),
                        },
                        "Region": 4,
                        "IsViaPoint": False
                    }
                    for stop in stop_list
                ],
                "Options": {
                    "HighwayOnly": True,
                    "DistanceUnits": 0  # 0 for miles, 1 for kilometers
                },
                "DrawLeastCost": False
            }
        ]
    }

    
    apikey = '299354C7A83A67439273691EA750BB7F'
    try:
        url = 'https://pcmiler.alk.com/apis/rest/v1.0/service.svc/mapRoutes?dataset=Current'
        params = {
            'dataVersion': 'current'
        }
        headers = getHeaders(apikey)
        
        # Make the request to the Map Routes API
        response = requests.post(url, headers=headers, params=params, data=json.dumps(postData))
        response.raise_for_status()  # Raise an error for bad responses
        print("HELLO")
        
        img = Image.open(BytesIO(response.content))
        return img
    except requests.exceptions.ReadTimeout as e:
        print(f"Request timed out: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

        
        
        

def get_coords_by_zipcode(zipcode, region="NA"):
    # Base URL for Single Search API
    url = f"https://singlesearch.alk.com/{region}/api/search"
    apikey = '299354C7A83A67439273691EA750BB7F'
    # Parameters
    params = {
        "authToken": apikey,
        "query": zipcode,  # ZIP code as the query
        "include": "Meta"  # Include metadata for better results
    }

    # Make the API request
    try:
        response = requests.get(url, params=params)

        # Check response status
        if response.status_code == 200:
            data = response.json()

            # Extract coordinates from the first location result
            locations = data.get("Locations", [])
            if locations:
                coords = locations[0].get("Coords")
                print(f"Coordinates for ZIP code {zipcode}: {coords}")
                return coords
            else:
                print(f"No results found for ZIP code {zipcode}.")
                return None
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None


