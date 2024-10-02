import numpy as np
import pandas as pd

from src.models import CPTGeneral, CPTData


def parse_conetec(filepath: str, cpt_id: str) -> tuple[CPTGeneral, CPTData]:
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
    # DEV NOTE: Two rows of headers are expected.
    # One with the parameter name and the other with the units.
    # At this stage, the parameters and units have been consistent.
    # The only peculiarity is that there could be an extra column for
    # passive gamma-ray radiattion.

    # Code below written for expressiveness and not efficiency.
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
        filepath=filepath,
        cpt_id=cpt_id,
        timestamp=datetime,
        area_ratio=df_meta.loc["Tip Net Area Ratio:", "Value"],
        cone_id=df_meta.loc["Cone ID:", "Value"],
        cone_type="EC",
        subcontractor="ConeTec",
        test_id=df_meta.loc["Run ID:", "Value"],
        # This are not found in Contec's files
        depth_gwt=None,
        pen_rate=None,
        remarks=None,
    )

    cpt_data = CPTData(
        cpt_id=cpt_id,
        depth=data["depth"].values,
        qc=data["qc"].values,
        qt=data["qt"].values,
        fs=data["fs"].values,
        u2=data["u"].values,
    )

    return cpt, cpt_data
