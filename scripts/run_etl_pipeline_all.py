import sys
import os
import glob
import ast
import datetime
import pandas as pd
import numpy as np

sys.path.insert(0, "\\".join(os.path.dirname(__file__).split("\\")[:-1]))
from src.client.geo_client import GEO_Client
from src.config import OUTPUTS_DIR
import warnings

warnings.filterwarnings("ignore")

features_files = glob.glob(OUTPUTS_DIR + f"/**/features/*")
attributes_files = glob.glob(OUTPUTS_DIR + f"/**/attributes/*")

geo_client = GEO_Client()


def run_etl():
    print("Running ETL")
    geo_client.get_all_attributes()
    print("Saved GEOAttributes")
    geo_client.get_all_features()
    print("Saved GEOFeatures")
    merge_and_parse_files_final()
    print("Saved Final GEOTables")


def merge_and_parse_files_final():
    """
    Merging features and attributes for each layer in each MapService.
    Saving these files in URI outputs/final/
    """
    for file in features_files:

        variable = file.split("\\")[-3]
        if "final" not in os.listdir(os.path.join(OUTPUTS_DIR, f"{variable}")):
            os.mkdir(os.path.join(OUTPUTS_DIR, f"{variable}", "final"))

        name_ = file.split("\\")[-1]
        try:
            df = pd.recsv(file)
        except:
            continue
        if len(df) == 0:
            continue
        df.columns = df.columns.str.replace("attributes.", "")
        df.columns = df.columns.str.replace("geometry.", "")

        for col in df.columns:
            if "DATE" in col:
                indeces = pd.to_datetime(df[col], errors="coerce")[
                    pd.to_datetime(df[col], errors="coerce").isna()
                ].index
                df.loc[indeces, col] = np.nan
                try:
                    indeces = df[df[col] < 0].index
                    df.loc[indeces, col] = np.nan
                except:
                    pass
                df[col] = df[col].apply(lambda x: np.nan if x > 2647813300000 else x)
                df[col] = df[col].apply(
                    lambda x: (
                        datetime.datetime.fromtimestamp(x / 1e3)
                        if pd.isna(x) == False
                        else x
                    )
                )

        attributes_file = [
            x
            for x in attributes_files
            if "\\" + file.split("\\")[-3] in x and "\\" + file.split("\\")[-1] in x
        ]
        assert len(attributes_file) == 1
        try:
            attributes = pd.recsv(attributes_file[0])
        except:
            attributes = pd.DataFrame()

        if len(attributes) == 0:
            pass
        else:
            attributes["column"] = attributes["column"].str.upper()
            for col in attributes["column"].unique():
                attributes_dict = (
                    attributes[attributes["column"] == col]
                    .set_index("id")["name"]
                    .to_dict()
                )
                try:  ########### Added because in Shunt Reactor there are some weird Attributes' columns.
                    if df[col].dtype == float:
                        df[col] = df[col].fillna(-1)
                        df[col] = df[col].astype(int).astype(str).map(attributes_dict)
                        df[col] = df[col].replace("-1", pd.NA)
                    else:
                        df[col] = df[col].astype(str).map(attributes_dict)
                except:  ########### Added because in Shunt Reactor there are some weird Attributes' columns.
                    pass
        if "rings" in df.columns:
            indices = df[~df["rings"].isna()].index
            df.loc[indices, "rings"] = df.loc[indices, "rings"].apply(
                lambda x: ast.literal_eval(x)
            )
            df.loc[indices, "x"] = df.loc[indices, "rings"].apply(
                lambda x: (
                    (
                        np.sum([coord[0] for coord in x[0]])
                        / len([coord[0] for coord in x[0]])
                    )
                    if len(x) != 0
                    else np.nan
                )
            )
            df.loc[indices, "y"] = df.loc[indices, "rings"].apply(
                lambda x: (
                    (
                        np.sum([coord[1] for coord in x[0]])
                        / len([coord[1] for coord in x[0]])
                    )
                    if len(x) != 0
                    else np.nan
                )
            )

        if "paths" in df.columns:
            indices = df[~df["paths"].isna()].index
            df.loc[indices, "paths"] = df.loc[indices, "paths"].apply(
                lambda x: ast.literal_eval(x)
            )
            df.loc[indices, "x"] = df.loc[indices, "paths"].apply(
                lambda x: (
                    (
                        np.sum([coord[0] for coord in x[0]])
                        / len([coord[0] for coord in x[0]])
                    )
                    if len(x) != 0
                    else np.nan
                )
            )
            df.loc[indices, "y"] = df.loc[indices, "paths"].apply(
                lambda x: (
                    (
                        np.sum([coord[1] for coord in x[0]])
                        / len([coord[1] for coord in x[0]])
                    )
                    if len(x) != 0
                    else np.nan
                )
            )

        df.to_csv(os.path.join(OUTPUTS_DIR, f"{variable}", "final", name_), index=False)
        print(f"Saved final: {variable} {name_}")


if __name__ == "__main__":
    run_etl()
