import pandas as pd
import glob
import os
import warnings

warnings.filterwarnings("ignore")
from typing import Literal
from .config import OUTPUTS_DIR


def get_last_mod_date_files():
    final_files = glob.glob(OUTPUTS_DIR + f"/**/final/*")
    files_dates = {}
    name_date = {}
    files_no_date = []

    for file in final_files:
        try:
            df = pd.recsv(os.path.abspath(file), parse_dates=["DATEMODIFIED"])
            mod = df["DATEMODIFIED"].max()
            files_dates[os.path.abspath(file)] = mod
        except:
            files_no_date.append(os.path.abspath(file))

    for key in files_dates.keys():
        new_key = os.path.split(key)[1].split(".")[0]
        name_date[new_key] = files_dates[key]

    name_date = (
        pd.DataFrame(data=name_date.values(), columns=["date"], index=name_date.keys())
        .reset_index()
        .rename(columns={"index": "name"})
    )
    name_date["date2"] = name_date["date"].apply(
        lambda x: None if pd.isna(x) else x.strftime("%Y-%m-%d %X")
    )
    return name_date
