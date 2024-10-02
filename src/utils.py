from dotenv import load_dotenv
import json
import numpy as np
import os
import requests
import pandas as pd

from src.models import CPTGeneral, CPTData
from src import openground

load_dotenv(override=True)


def parse_conetec(filepath: str, cpt_name: str) -> tuple[CPTGeneral, CPTData]:
    """
    Parses a CPT Conetec file in xls format. The file is expected to conform
    to a certain structure.

    This function will raise an error if the file is non-conforming.

    Returns:
        CPTGeneral: A CPTGeneral object representing the metadata of the CPT.
        CPTData: A CPTData object representing the raw data of the CPT.
    """
    df = pd.read_excel(filepath, header=None)
    df = df.iloc[:, 1:]

    # Input checks
    filt = df.isnull().sum(axis=1) == len(df.columns)
    end_of_metadata = df[filt].index.max()
    if not end_of_metadata > 0:
        raise IOError("Empty row between metadata and CPT data not satisfied.")

    # EXTRACT METADATA
    df_meta = df[:end_of_metadata]
    df_meta = df_meta.dropna(axis=1, how="all")
    df_meta = df_meta.reset_index(drop=True)
    df_meta.columns = ["Field", "Value"]

    if "ConeTec" in df_meta.iloc[0, 0]:
        pass
    elif "CPT Inc." in df_meta.iloc[0, 0]:
        pass
    else:
        raise ValueError("Conetec name not in header")

    df_meta = df_meta.set_index("Field")
    df_meta = df_meta.dropna(axis=0, how="all")

    # DEV NOTE: This is a very conservative approach but it makes sense at this
    # stage. This allows us to know if there are more attributes in Contec's
    # files such as pre-drilled depth and groundwater
    expected = set(
        [
            "Interpretation Format:",
            "Run ID:",
            "Job No:",
            "Client:",
            "Project:",
            "Facility:",
            "Sounding ID:",
            "Cone ID:",
            "Operator:",
            "CPT Date:",
            "CPT Time:",
            "CPT File:",
            "Tip Units:",
            "Sleeve Units:",
            "PP Units:",
            "Tip Conversion to bar:",
            "Sleeve Conversion to bar:",
            "PP Conversion to meters:",
            "Easting / Long:",
            "Northing / Lat:",
            "Elevation:",
            "Tip Net Area Ratio:",
            "Averaging Interval:",
            "Col  5 (Extra Module) Parameter",
            "Col  5 (Extra Module) Units",
            "Coord Source:",
            "Coord Type:",
            "UTM Zone:",
            "Easting / Long:",
            "Northing / Lat:",
            "Elevation:",
            "Norm. SBT Charts extended for Low Fr:",
            "Extended Y Axis on SBT Charts:",
        ]
    )
    assert set(df_meta.index).difference(expected) == set()

    # CHECK RAW DATA
    cols = df.loc[end_of_metadata + 1].to_list()
    if cols != ["Depth", "Depth", "qc", "qt", "fs", "u", "Rf"]:
        raise IOError(f"Parsed columns differ. Columns: {cols}")

    units = df.loc[end_of_metadata + 2].to_list()
    if units != ["m", "ft", "tsf", "tsf", "tsf", "ft", "%"]:
        raise IOError(f"Parsed units differ. Units: {units}")

    # EXTRACT RAW DATA
    # DEV NOTE: Code below is basic data cleaning and specific to source.
    data = df.loc[end_of_metadata + 1 :]
    data = data.reset_index(drop=True)
    data.columns = list(range(len(data.columns)))
    data = data.drop(columns=[0])  # Drop Depth (m) column

    # Set column names to CPT parameter
    data.columns = data.iloc[0].values
    data = data.drop(index=0)
    data = data.drop(columns="Rf")
    data = data.drop(index=1)
    data = data.reset_index(drop=True)
    data = data.rename(columns={"Depth": "depth"})
    data = data.astype(float)
    data = data.mask(data <= -9000, np.nan)

    # UNIT CONVERSION
    # DEV NOTE: At this point we know that all parameters, except for pore
    # pressure are in the correct unit and need no conversion.
    data["u"] = data["u"] * 62.4 / 2000  # ft of water to tsf

    datetime = (
        f'{df_meta.loc["CPT Date:", "Value"]} ' f'{df_meta.loc["CPT Time:", "Value"]}'
    )
    datetime = pd.to_datetime(datetime).strftime("%Y-%m-%dT%H:%M:%SZ")

    cpt = CPTGeneral(
        source_file=os.path.basename(filepath),
        name=cpt_name,
        timestamp=datetime,
        area_ratio=df_meta.loc["Tip Net Area Ratio:", "Value"],
        cone_id=str(df_meta.loc["Cone ID:", "Value"]),
        cone_type="EC",
        subcontractor="ConeTec",
        test_id=str(df_meta.loc["Run ID:", "Value"]),
        # This are not found in Contec's files
        depth_gwt=None,
        pen_rate=None,
        remarks=None,
    )

    cpt_data = CPTData(
        cpt_name=cpt_name,
        depth=data["depth"].values,
        qc=data["qc"].values,
        qt=data["qt"].values,
        fs=data["fs"].values,
        u2=data["u"].values,
    )

    return cpt, cpt_data


def insert_location_from_cpt_test(
    cpt: CPTGeneral,
    project_id: str,
    location_type: str,
) -> str:
    """
    Inserts a location in OpenGround's `LocationDetails` table from a CPt test.

    Updates the project's `locations` attribute.

    This function is provided as both a convenience and necessity because of
    the following two reasons:

        * A location is required to insert a CPT test in OpenGround.
        * The schema does not have a timestamp field for the CPT test. This
            is instead stored in the `LocationDetails` table.
    """

    data = {
        "Group": "LocationDetails",
        "DataFields": [
            {"Header": "LocationID", "Value": cpt.name},
            {"Header": "uui_LocationType", "Value": location_type},
            {"Header": "DateStart", "Value": cpt.timestamp},
        ],
    }
    payload = json.dumps(data)
    url = (
        f"{openground.get_root_url()}/data/projects/{project_id}/groups/LocationDetails"
    )
    r = requests.post(url, data=payload, headers=openground.get_og_headers())

    if r.status_code != 200:
        raise Exception(f"Error inserting Location: {r.text}")

    return r.json()["Id"]


def insert_cpt_test(cpt: CPTGeneral, project_id: str) -> str:
    """
    Inserts a CPT test in OpenGround's `StaticConePenetrationGeneral` table
    from a CPTGeneral object.

    The attribute `DateEnd` is removed from the record as it goes into the
    `LocationDetails` table.

    The `uui_LocationDetails` foreign key is mapped to the corresponding cloud_id.
    """
    # Makes record conformant to OpenGround's schema:
    # Formats attributes to {"Header": key, "Value": value}.
    # Removes `DateEnd` attribute as it goes into `LocationDetails`.
    # Maps `uui_LocationDetails` to the corresponding cloud_id.

    locations = openground.get_project_locations(project_id)
    if cpt.name not in locations:
        raise ValueError(
            f"Location {cpt.name} not found in project. Locations found: {locations}"
        )

    record = []
    for key, value in cpt.og_record.items():

        if key == "DateStart":
            continue

        if key == "uui_LocationDetails":
            value = locations[value]

        record.append({"Header": key, "Value": value})

    # POST request
    data = {"Group": "StaticConePenetrationGeneral", "DataFields": record}
    payload = json.dumps(data)
    url = (
        f"{openground.get_root_url()}/data/projects/{project_id}/groups/"
        f"StaticConePenetrationGeneral"
    )
    r = requests.post(url, data=payload, headers=openground.get_og_headers())

    if r.status_code != 200:
        raise Exception(f"Error inserting CPT test: {r.text}")
    return r.json()["Id"]


def _format(d: dict) -> list[dict]:
    """
    Transforms a dictionary into a list of dictionaries in the form of
    {"Header": "field_name", "Value": "field_value"} as required in the
    Openground API.
    """
    output = []
    for key, value in d.items():
        if "Date" in key and type(value) != str:
            value = value.strftime("%Y-%m-%dT%H:%M:%SZ")
        output.append({"Header": key, "Value": value})
    return output


def _format_records(recs: list[dict]) -> list[list[dict]]:
    formatted_rec = []
    for r in recs:
        formatted_rec.append(_format(r))
    return formatted_rec


def _extract_records_from_df(df: pd.DataFrame) -> list:
    """
    Extracts records from a pd.DataFrame, removing any columns with null
    values.

    Args:
        df (pd.DataFrame): The input pd.DataFrame from which records will be
            extracted.

    Returns:
        list: A list of dictionaries, where each dictionary represents a record
            (row) from the DataFrame,  containing only non-null values for each
            record.

    .. code-bloc:: python

        data = {
            'Name': ['John', 'Alice', 'Bob'],
            'Age': [25, 30, None],
            'City': ['New York', None, 'San Francisco'],
            'Gender': ['Male', 'Female', 'Male']
        }

        df = pd.DataFrame(data)
        extracted_records = extract_records_from_df(df)
        >>> extracted_records

            [
            {'Name': 'John', 'Age': 25.0, 'City': 'York', 'Gender': 'Male'},
            {'Name': 'Alice', 'Age': 30.0, 'Gender': 'Female'},
            {'Name': 'Bob', 'City': 'San Francisco', 'Gender': 'Male'}
            ]
    """

    records = df.to_dict(orient="index")
    for _, d in records.items():
        for key, value in d.copy().items():
            if pd.isna(value):
                d.pop(key)

    return list(records.values())


def transform_df_to_openground_rec(df: pd.DataFrame) -> list[list[dict]]:
    """
    Converts a dataframe into a list of lists where each inner list is a
    dictionary conformant to the Openground structure.

    .. code-bloc:: python

        data = {
            'Name': ['John', 'Alice', 'Bob'],
            'Age': [25, 30, None],
            'City': ['New York', None, 'San Francisco'],
            'Gender': ['Male', 'Female', 'Male']
        }

        df = pd.DataFrame(data)
        recs = transform_df_to_openground_rec(df)
        >>> recs = [
                [
                    {'Header': 'Name', 'Value': 'John'},
                    {'Header': 'Age', 'Value': 25},
                    {'Header': 'City', 'Value': 'York'},
                    {'Header': 'Gender', 'Value': 'Male'},
                ],
                [
                    {'Header': 'Name', 'Value': 'Alice'},
                    {'Header': 'Age', 'Value': 30.0},
                    {'Header': 'Gender', 'Value': 'Female'},
                ],
                [
                    {'Header': 'Name', 'Value': 'Bob'},
                    {'Header': 'City', 'Value': 'San Francisco'},
                    {'Header': 'Gender', 'Value': 'Male'},
                ]
            ]
    """
    return _format_records(_extract_records_from_df(df))


def get_number_cpt_records(project_id: str, cpt_name: str) -> int:
    """
    Returns the number of CPT readings loaded in OpenGround's
    `StaticConePenetrationData` table for a given CPT ID.

    Returns 0 if there are no CPT records or if specific cpt_name does not exist.
    """    
    payload = {
        "Projections": [
        {"Group": "LocationDetails", "Header": "LocationID"},
        {"Group": "StaticConePenetrationGeneral", "Header": "TestNumber"},
        {"Group": "StaticConePenetrationData", "Header": "ConeResistance"}
    ],
        "Group": "StaticConePenetrationData",
        "Projects": [project_id]
    }
    df = openground.execute_query(payload)
    if len(df) == 0:
        return 0
    return len(df[df['LocationID'] == cpt_name])

def insert_cpt_data(cpt_data: CPTData, project_id: str) -> None:
    """Inserts CPT data in OpenGround's `StaticConePenetrationData` table."""

    data = cpt_data.data
    assert len(data["uui_StaticConePenetrationGeneral"].unique()) == 1
    cpt_name = data["uui_StaticConePenetrationGeneral"].unique()[0]
    data = data.reset_index(drop=True)
    assert data["Depth"].is_unique
    assert data['CorrectedConeResistance'] is not None

    records = transform_df_to_openground_rec(data)
    openground.insert_in_bulk(project_id, "StaticConePenetrationData", records)

    loaded = get_number_cpt_records(project_id, cpt_name)
    assert loaded == len(data), f"Loaded {loaded} records, expected {len(data)}"