from dotenv import load_dotenv
import os
import requests
import pandas as pd

load_dotenv(override=True)


def get_og_auth_token() -> str:
    """
    Gets an OpenGround authentication token using the client credentials flow.
    """    
    url = "https://imsoidc.bentley.com/connect/token"
    payload = {
        "grant_type": "client_credentials",
        "scope": "openground ",
        "client_id": os.getenv('OPENGROUND_CLIENT_ID_ADMIN'),
        "client_secret": os.getenv('OPENGROUND_CLIENT_SECRET_ADMIN'),
    }
    headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    }
        
    response = requests.request("POST", url, headers=headers, data=payload)
    
    if response.status_code != 200:
        raise Exception(f'Error getting OpenGround token: {response.text}')
    
    token = response.json()['access_token']
    return token

def get_og_headers() -> dict:
    return{
        'KeynetixCloud': 'U3VwZXJCYXRtYW5GYXN0',
        'Content-Type': 'application/json',
        'Expect': '100-continue',
        'instanceId': os.getenv('CLOUD_ID'),
        'Authorization': f"Bearer {get_og_auth_token()}",
        'User-Agent':  'AA+ET',
    }

def get_root_url():
    region = os.getenv('CLOUD_REGION')
    return f'https://api.{region}.openground.cloud/api/v1.0'

def get_projects_ids():
    url = f'{get_root_url()}/data/projects'
    response = requests.get(url, headers=get_og_headers())

    if response.status_code != 200:
        raise Exception(f'Error getting projects: {response.text}')
    
    projects = response.json()
    out = {}
    for project in projects:

        cloud_id = project['Id']

        for d in project['DataFields']:
            if d['Header'] == 'ProjectID':
                name = d['Value']
                break

        out[name] = cloud_id

    return out

def get_project_locations(project_id: str) -> pd.DataFrame:

    url = f'{get_root_url()}/data/projects/{project_id}/groups/LocationDetails'

    response = requests.get(url, headers=get_og_headers())

    if response.status_code != 200:
        raise Exception(f'Error getting locations: {response.text}')
    
    response = response.json()

    out = {}
    for location in response:
        cloud_id = location['Id']

        for d in location['DataFields']:
            if d['Header'] == 'LocationDetails.LocationID':
                name = d['Value']
                break
        out[name] = cloud_id
    return out


def delete_location_by_id(project_id, location_id: str) -> None:

    url = (
        f'{get_root_url()}/data/projects/{project_id}/'
        f'groups/LocationDetails/delete'
    )
    response = requests.put(
        url, headers=get_og_headers(), json=[location_id]
    )

    if response.status_code != 200:
        raise Exception(f'Error deleting location: {response.text}')