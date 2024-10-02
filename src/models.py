import numpy as np
import pandas as pd


class CPTGeneral:
    """
    Data-oriented class that models a valid record for insertion in
    OpenGround`s `StaticConePenetrationGeneral` table.
    """

    OG_MAPPER = {
        "source_file": "AssociatedFileReference",
        "cpt_id": "uui_LocationDetails",
        "area_ratio": "ConeAreaRatio",
        "cone_id": "ConeReference",
        "depth_gwt": "DepthGroundwater",
        "pen_rate": "NominalRateOfPenetration",
        "remarks": "Remarks",
        "subcontractor": "Subcontractor",
        "test_id": "TestNumber",
        "cone_type": "uui_TestType",
        "pre_drill_depth": "PreDrillDepth",
        "timestamp": "DateStart",
    }

    def __init__(
        self,
        source_file: str,
        cpt_id: str,
        timestamp: str,  # "%Y-%m-%dT%H:%M:%SZ"
        area_ratio: float,  # unitless
        cone_id: str,
        depth_gwt: float,  # ft
        pen_rate: float,  # cm/s
        remarks: str,
        subcontractor: str,
        test_id: str,
        cone_type: str,
        pre_drill_depth: float = None,  # ft
    ) -> None:
        """
        Initializes a `CPTGeneral` object. Timestamp is "%Y-%m-%dT%H:%M:%SZ".
        """
        self.source_file = source_file
        self.cpt_id = cpt_id
        self.timestamp = timestamp
        self.area_ratio = area_ratio
        self.cone_id = cone_id
        self.depth_gwt = depth_gwt
        self.pen_rate = pen_rate
        self.remarks = remarks
        self.subcontractor = subcontractor
        self.test_id = test_id
        self.cone_type = cone_type
        self.pre_drill_depth = pre_drill_depth
        self.og_record = self._get_og_record()

    def _get_og_record(self) -> list[dict]:
        """
        Returns a dictionary in the form {'attribute':'value'} where the
        attributes names are conformant to OpenGround's schema.

        Attributes with value `None` are not included in the record.
        """

        record = {}
        for attr_name in dir(self):

            if not attr_name.startswith("__") and attr_name != "OG_MAPPER":

                attr_value = getattr(self, attr_name)
                if attr_value is not None and not callable(attr_value):
                    k = self.OG_MAPPER[attr_name]
                    record[k] = attr_value

        # Sort record by key
        sorted_list = sorted(record.items())
        sorted_dict = {}
        for key, value in sorted_list:
            sorted_dict[key] = value

        return sorted_dict


class CPTData:
    """
    Data-oriented class that holds valid records for insertion in OpenGround`s
    `StaticConePenetrationData` table.

    OpenGround attributes and units:
        ConeResistance [tsf]
        CorrectedConeResistance [tsf]
        Depth [ft]
        ExcessPorePressure (u2-u0) [tsf]
        FacePorewaterPressure (u1) [tsf]
        InSituPorePressure (u0) [tsf]
        LocalUnitSideFrictionResistance (fs) [tsf]
        NaturalGammaRadiation [counts per second]
        ShoulderPorewaterPressure (u2) [tsf]
        SlopeIndicator1 [deg]
        SlopeIndicator2 [deg]
    """

    OG_MAPPER = {
        "depth": "Depth",
        "qc": "ConeResistance",
        "fs": "LocalUnitSideFrictionResistance",
        "u2": "ShoulderPorewaterPressure",
        "qt": "CorrectedConeResistance",
        "gamma_rad": "NaturalGammaRadiation",
    }

    def __init__(
        self,
        cpt_id: str,
        depth: np.ndarray,  # depth [ft]
        qc: np.ndarray,  # cone resistance [tsf]
        fs: np.ndarray,  # local unit side friction resistance [tsf]
        u2: np.ndarray,  # shoulder porewater pressure [tsf]
        qt: np.ndarray = None,  # corrected cone resistance [tsf]
    ) -> None:

        self.depth = depth
        self.qc = qc
        self.fs = fs
        self.u2 = u2
        self.qt = qt
        self.data = self._attrs_to_dataframe()
        self.data["uui_StaticConePenetrationGeneral"] = cpt_id

    def _attrs_to_dataframe(self) -> pd.DataFrame:
        """
        Convert an object's attributes into a Pandas DataFrame excluding those
        attributes that are `None`.

        Returns:
            pd.DataFrame: A Pandas DataFrame where each column corresponds to
                an attribute of the input object, and each row contains the
                attribute values.
        """
        data_dict = {}
        for attr_name in dir(self):

            # Exclude special and private attributes and functions
            if not attr_name.startswith("__"):

                attr_value = getattr(self, attr_name)
                if isinstance(attr_value, np.ndarray):
                    data_dict[attr_name] = attr_value

        df = pd.DataFrame(data_dict)
        df.rename(columns=self.OG_MAPPER, inplace=True)
        df = df.sort_index(axis=1, ascending=True)

        # Remove rows where depth and cone resistance are NaN.
        # Some CPTs have only either raw or corrected cone resistance. Hence,
        # the conditional.
        if "ConeResistance" in df.columns:
            cols = ["Depth", "ConeResistance"]
        elif "CorrectedConeResistance" in df.columns:
            cols = ["Depth", "CorrectedConeResistance"]
        else:
            raise ValueError(f"Condition not designed for. Columns are {df.columns}.")

        df.dropna(axis=0, how="all", subset=cols, inplace=True)
        return df
