from dotenv import load_dotenv
import json
import os
import math
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
        "client_id": os.getenv("OPENGROUND_CLIENT_ID_ADMIN"),
        "client_secret": os.getenv("OPENGROUND_CLIENT_SECRET_ADMIN"),
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code != 200:
        raise Exception(f"Error getting OpenGround token: {response.text}")

    token = response.json()["access_token"]
    return token


def get_og_headers() -> dict:
    return {
        "KeynetixCloud": "U3VwZXJCYXRtYW5GYXN0",
        "Content-Type": "application/json",
        "Expect": "100-continue",
        "instanceId": os.getenv("CLOUD_ID"),
        "Authorization": f"Bearer {get_og_auth_token()}",
        "User-Agent": "AA+ET",
    }


def get_root_url():
    region = os.getenv("CLOUD_REGION")
    return f"https://api.{region}.openground.cloud/api/v1.0"


def get_projects_ids():
    url = f"{get_root_url()}/data/projects"
    response = requests.get(url, headers=get_og_headers())

    if response.status_code != 200:
        raise Exception(f"Error getting projects: {response.text}")

    projects = response.json()
    out = {}
    for project in projects:

        cloud_id = project["Id"]

        for d in project["DataFields"]:
            if d["Header"] == "ProjectID":
                name = d["Value"]
                break

        out[name] = cloud_id

    return out


def get_project_locations(project_id: str) -> pd.DataFrame:

    url = f"{get_root_url()}/data/projects/{project_id}/groups/LocationDetails"

    response = requests.get(url, headers=get_og_headers())

    if response.status_code != 200:
        raise Exception(f"Error getting locations: {response.text}")

    response = response.json()

    out = {}
    for location in response:
        cloud_id = location["Id"]

        for d in location["DataFields"]:
            if d["Header"] == "LocationDetails.LocationID":
                name = d["Value"]
                break
        out[name] = cloud_id
    return out


def delete_location_by_id(project_id: str, location_id: str) -> None:

    url = (
        f"{get_root_url()}/data/projects/{project_id}/" f"groups/LocationDetails/delete"
    )
    response = requests.put(url, headers=get_og_headers(), json=[location_id])

    if response.status_code != 200:
        raise Exception(f"Error deleting location: {response.text}")


def insert_in_bulk(project_id: str, group_name: str, records: list[list[dict]]):
    """
    Args:
        project_id (str): Openground Project cloud id.
        group_name (str): Openground table to be loaded.
        records (list[list[dict]]): Openground records. See below for form.

    .. note::
        Openground API allows a maximum of 1000 entries for each bulk action.

        However, 503 errors are frequently experienced when loading up to 1000
        records. Thus, aworkaround is to set MAX_ALLOWED_BULK=500 for a more
        consistent performance.

        Records are loaded iteratively in packets of `MAX_ALLOWED_BULK`
        records.

    .. code-block:: python

        records = [
                {
                    "Header": "LocationID",
                    "Value": "WCR_B-01"
                },
                {
                    "Header": "uui_DrillingMethod",
                    "Value": "HSA"
                },
                {
                    "Header": "FinalDepth",
                    "Value": "140"
                }
            ],
            [
                {
                    "Header": "LocationID",
                    "Value": "WCR_B-02"
                },
                {
                    "Header": "uui_DrillingMethod",
                    "Value": "HSA"
                },
                {
                    "Header": "FinalDepth",
                    "Value": "140"
                }
            ]
        ]

    """

    def _load_recs(project_id: str, group_name: str, records: list[list[dict]]) -> None:
        """Convenience method to load up to `MAX_ALLOWED_BULK` records"""

        # Create POST body
        bulk_records = [{"Group": group_name, "DataFields": r} for r in records]
        payload = {"DataEntries": bulk_records}
        payload = json.dumps(payload)

        url = f"{get_root_url()}/data/projects/{project_id}/groups/{group_name}/bulk"

        # Make request and logging
        r = requests.post(url, headers=get_og_headers(), data=payload)

    MAX_ALLOWED_BULK = 1000

    if len(records) < MAX_ALLOWED_BULK:
        _load_recs(project_id, group_name, records)
    else:
        n_records = len(records)
        iterations = math.ceil(n_records / MAX_ALLOWED_BULK)
        start = 0
        for i in range(1, iterations + 1):
            end = min(i * MAX_ALLOWED_BULK, n_records)
            print(f"Packet {i}/{iterations} Start")
            _load_recs(project_id, group_name, records[start:end])
            print(f"Packet {i}/{iterations} End")
            start = end
