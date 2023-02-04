import os
import zipfile
from io import TextIOWrapper

import pandas as pd

ROOT_PATH = os.path.abspath(  # return the absolute path of the following
    os.path.join(  # concatenate the directory of the following
        __file__,  # path of current execution file
        os.pardir  # parent directory string
    )
)
RESOURCES_PATH = '/resources/'


def open_zipped_file(file_name: str):
    """Open zipped file and read it in non-binary mode."""
    zipped_file = ROOT_PATH + RESOURCES_PATH + file_name
    with zipfile.ZipFile(zipped_file, 'r') as my_zip:
        return TextIOWrapper(
            my_zip.open(
                my_zip.namelist()[0]
            )
        )
