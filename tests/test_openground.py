from dotenv import load_dotenv
import json
import numpy as np
import os
import requests
import pandas as pd

from src import openground

load_dotenv(override=True)

PROJECT_CLOUD_ID = "b1e7058f-f750-4add-8245-21244b458432"


def test_get_og_auth_token():
    openground.get_og_auth_token()


def test_get_og_headers():
    openground.get_og_headers()


def test_get_root_url():
    openground.get_root_url()


def test_get_projects():
    r = openground.get_projects_ids()
    assert "z_DF Test - Empty" in r


def test_get_project_locations():
    r = openground.get_project_locations(PROJECT_CLOUD_ID)
    assert "BR-TN-3(SCPT)" in r


def test_execute_query():

    payload = {
        "Projections": [
            {"Group": "LocationDetails", "Header": "LocationID"},
            {"Group": "StaticConePenetrationGeneral", "Header": "TestNumber"},
            {"Group": "StaticConePenetrationData", "Header": "ConeResistance"},
        ],
        "Group": "StaticConePenetrationData",
        "Projects": [PROJECT_CLOUD_ID],
    }
    df = openground.execute_query(payload)
    assert len(df) > 3000


def test_get_static_cone_general_records():

    out = openground.get_static_cone_general_records(PROJECT_CLOUD_ID)
    assert "BR-TN-3(SCPT)" in out
