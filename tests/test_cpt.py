import os
import pandas as pd

from src import models, utils


def get_test_file_path(filename: str):
    return os.path.join(os.path.dirname(__file__), 'test_files', filename)



def test_utils_parse_conetec():

    f = get_test_file_path('24-53-28244_SPBR-B13E-1A-BSC.XLS')
    utils.parse_conetec(f, '24-53-28244_SPBR-B13E-1A-BSC')