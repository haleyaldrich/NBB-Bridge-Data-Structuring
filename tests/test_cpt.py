import os
import numpy as np


from src import utils, openground

PROJECT_CLOUD_ID = "b1e7058f-f750-4add-8245-21244b458432"

def get_test_file_path(filename: str):
    return os.path.join(os.path.dirname(__file__), "test_files", filename)


def test_utils_parse_conetec():

    f = get_test_file_path("24-53-28244_SPBR-B13E-1A-BSC.XLS")
    cpt, cpt_data = utils.parse_conetec(f, "cpt_test")
    assert cpt.source_file == "24-53-28244_SPBR-B13E-1A-BSC.XLS"
    assert cpt.name == "cpt_test"
    assert cpt.area_ratio == 0.80
    assert cpt.cone_type == "EC"
    assert cpt.timestamp.startswith("2024")
    assert cpt.cone_id == "652:T1500F15U35"
    assert cpt.subcontractor == "ConeTec"
    assert cpt.test_id == "1726602193"
    assert cpt.depth_gwt is None
    assert cpt.pen_rate is None
    assert cpt.depth_gwt is None
    assert cpt.remarks is None
    assert cpt.pre_drill_depth is None

    # DEV NOTE: Units in OG are depth [ft], qc [tsf], qt [tsf], fs [tsf], u2 [tsf]
    assert np.allclose(cpt_data.depth[:3], np.array([0.08202, 0.16404, 0.24606]))
    assert np.allclose(cpt_data.qc[:3], np.array([24.718, 185.459, 337.209]))
    assert np.allclose(
        cpt_data.qt[:3], np.array([24.71592117, 185.46112877, 337.21475579])
    )
    assert np.allclose(cpt_data.fs[:3], np.array([0.24, 0.29, 0.385]))

    raw_u2_data = np.array([-0.3330, 0.3410, 0.9220])
    raw_u2_data = raw_u2_data * 62.4 / 2000  # ft of water to tsf
    assert np.allclose(cpt_data.u2[:3], raw_u2_data)


def test_insert_location_from_cpt_test():

    cpt_name = "cpt_test"

    # Insert location
    f = get_test_file_path("24-53-28244_SPBR-B13E-1A-BSC.XLS")
    cpt, _ = utils.parse_conetec(f, cpt_name)
    r = utils.insert_location_from_cpt_test(cpt, PROJECT_CLOUD_ID, "CPT")

    # Check location was inserted
    assert cpt_name in openground.get_project_locations(PROJECT_CLOUD_ID)

    # Delete location
    openground.delete_location_by_id(PROJECT_CLOUD_ID, r)


def test_insert_cpt_test():

    cpt_name = "cpt_test"

    # Parse file and insert location
    f = get_test_file_path("24-53-28244_SPBR-B13E-1A-BSC.XLS")
    cpt, _ = utils.parse_conetec(f, cpt_name)
    r = utils.insert_location_from_cpt_test(cpt, PROJECT_CLOUD_ID, "CPT")
    assert cpt_name in openground.get_project_locations(PROJECT_CLOUD_ID)

    # Insert CPT test
    utils.insert_cpt_test(cpt, PROJECT_CLOUD_ID)

    # Delete location and by extension the CPT test
    openground.delete_location_by_id(PROJECT_CLOUD_ID, r)


def test_transform_df_to_openground_rec():

    f = get_test_file_path("24-53-28244_SPBR-B13E-1A-BSC.XLS")
    _, cpt_data = utils.parse_conetec(f, "cpt_test")
    records = utils.transform_df_to_openground_rec(cpt_data.data)
    assert len(records) == 116

def test_get_number_cpt_records():

    n = utils.get_number_cpt_records(PROJECT_CLOUD_ID, 'BR-TN-3(SCPT)')
    assert n == 483


def test_insert_cpt_data():

    cpt_name = "cpt_test"


    # Parse file and insert location
    f = get_test_file_path("24-53-28244_SPBR-B13E-1A-BSC.XLS")
    cpt, cpt_data = utils.parse_conetec(f, cpt_name)
    utils.insert_location_from_cpt_test(cpt, PROJECT_CLOUD_ID, "CPT")
    assert cpt_name in openground.get_project_locations(PROJECT_CLOUD_ID)

    # Insert CPT test
    utils.insert_cpt_test(cpt, PROJECT_CLOUD_ID)

    # Insert CPT data
    try:
        utils.insert_cpt_data(cpt_data, PROJECT_CLOUD_ID)
    except Exception as e:
        raise e
    finally:
        # Delete location and by extension the CPT test
        location = openground.get_project_locations(PROJECT_CLOUD_ID)[cpt_name]
        openground.delete_location_by_id(PROJECT_CLOUD_ID, location)

