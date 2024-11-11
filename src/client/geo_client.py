import requests
from bs4 import BeautifulSoup
from requests_ntlm import HttpNtlmAuth
import re
import json
import pandas as pd
import datetime
import os
from collections import defaultdict
import warnings

warnings.filterwarnings("ignore")

from ..config import OUTPUTS_DIR
from ..utils import get_last_mod_date_files

pattern = re.compile(r'window\["_csrf_"\] = "([^"]+)"')
map_service_list = [0, 1, 2, 3, 6, 7, 8]
map_service_names = [
    "capital",
    "provincia",
    "capitalalto_volt",
    "LandBase",
    "provinciaalto_volt",
    "provincia",
    "capital",
]
map_service_dict = dict(zip(map_service_list, map_service_names))
folder_name = [
    "provincia",
    "sin",
    "landbase",
    "sin_provincia",
    "provincia",
]
name_dict = dict(zip(map_service_names, folder_name))

map_layers_without_features = [(0, 1)]


def retry(retries=4):
    def decorator_retry(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"Attempt {attempt + 1} failed: {e}")
                    last_exception = e
            raise last_exception

        return wrapper

    return decorator_retry


class GEO_Client:

    def __init__(
        self,
        username="username",
        username_2="username2",
        password="password",
        home_url="https://arcgis.coess.io/",
        url="https://arcgis.coess.io/Geocortex/",
        login_url="https://arcgis.coess.io/portal/sharing/oauth2/signin",
    ):
        self.username = username
        self.username_2 = username_2
        self.password = password
        self.home_url = home_url
        self.url = url
        self.login_url = login_url
        self.auth = HttpNtlmAuth(username, password)
        self.all_features = defaultdict(list)
        self.log_in()
        self.index_df = None
        self.modified_dates = get_last_mod_date_files()

    @retry(retries=3)
    def log_in(self):
        """
        Take self.Session to a logged in state by passing through the two layers of security they have:
            1. Log in in with a Microsoft pop-up.
            2. Log in in to the arcGEOServer.

        Args:
            Within the __init__ of the initialized class.

        Returns:
            Many variables updated:
            - Session signed in.
            - token retrieved as self.token.
        """
        self.session = requests.Session()
        self.payload = {"username": self.username, "password": self.password}

        self.response = self.session.get(self.home_url, auth=self.auth, verify=False)
        self.soup_1 = BeautifulSoup(self.response.content, "html.parser")

        self.form = self.soup_1.find("form", {"id": "Form1"})
        self.viewstate = self.form.find("input", {"name": "__VIEWSTATE"})["value"]
        self.viewstate_gen = self.form.find("input", {"name": "__VIEWSTATEGENERATOR"})[
            "value"
        ]
        self.eventvalidation = self.form.find("input", {"name": "__EVENTVALIDATION"})[
            "value"
        ]

        self.payload = {
            "__VIEWSTATE": self.viewstate,
            "__VIEWSTATEGENERATOR": self.viewstate_gen,
            "__EVENTVALIDATION": self.eventvalidation,
        }

        self.form_action_url = self.home_url + self.form["action"]
        self.form_response = self.session.post(
            self.form_action_url, data=self.payload, auth=self.auth, verify=False
        )
        self.form_soup = BeautifulSoup(self.form_response.content, "html.parser")

        self.response_2 = self.session.get(self.url, verify=False)
        self.soup_2 = BeautifulSoup(self.response_2.content, "html.parser")

        self.input_string = self.soup_2.find_all("script")[2].string
        vals = re.search(r"var oAuthInfo = ({.*?})\r\n", self.input_string, re.DOTALL)
        self.auth_data = json.loads(vals.group(1))

        self.payload = {
            "oauth_state": self.auth_data["oauth_state"],
            "authorize": "true",
            "username": self.username_2,
            "password": self.password,
        }

        self.response_signin = self.session.post(
            self.login_url, data=self.payload, verify=False
        )
        self.soup_signin = BeautifulSoup(self.response_signin.content, "html.parser")

        token_re = re.search(r"gcx-(.*)", self.soup_signin.find("form")["action"])
        self.token = token_re.group(1)
        print("Logged in and retrieved GEOToken")

    def get_available_layers(self):
        """
        Get all the layers available by looping through all the Map Services in the API.

        Returns:
            self.index_df (pd.DataFrame): DataFrame with MapService / Layers values.
        """
        self.index_df = pd.DataFrame()

        for map_service in map_service_list:
            try:
                url = f"https://arcgis.coess.io/Geocortex/Essentials/REST/sites/SIN/map/mapservices/{map_service}/rest/services/x/MapServer/"
                # Append the token to the request parameters
                self.index_params = {"f": "json", "token": f"{self.token}"}
                # Make the request for seeing the available layers to extract data from.
                self.index_response = requests.get(
                    url, verify=False, params=self.index_params, timeout=30
                )
                aux = pd.json_normalize(self.index_response.json()["layers"])
                aux["map_service"] = map_service
                aux["map_service_name"] = map_service_dict[map_service]
                self.index_df = pd.concat([self.index_df, aux])
            except:
                continue

        self.index_df = self.index_df.reset_index(drop=True)
        self.index_df = self.index_df.merge(self.modified_dates, how="left", on="name")

        return self.index_df

    def fetch_layers_features(
        self, variable: str, variable_2: int, variable_3: list = None
    ):
        """
        Send petition to fetch the features from the given MapService (variable: name, variable_2: ID) and the list of layers from that MapService to be retrieved (variable_3).
        If variable_3 = None, all layers will be retrieved.

        Args:
            variable (str): Name of the Map Service
            variable_2 (int): Id of the Map Service
            variable_3 (list, optional): List of layers to be retrieved. Defaults to None.
        """

        if isinstance(self.index_df, pd.DataFrame):
            pass
        else:
            self.get_available_layers()

        if variable not in os.listdir(OUTPUTS_DIR):
            os.mkdir(os.path.join(OUTPUTS_DIR, variable))

        if "features" not in os.listdir(os.path.join(OUTPUTS_DIR, variable)):
            os.mkdir(os.path.join(OUTPUTS_DIR, variable, "features"))

        if variable_3 == None:
            filtered_index = self.index_df[self.index_df["map_service"] == variable_2]
        else:
            filtered_index = self.index_df[
                (self.index_df["map_service"] == variable_2)
                & (self.index_df["id"].isin(variable_3))
            ]

        for map_service_, layer_, name_ in filtered_index[
            ["map_service", "id", "name"]
        ].values:

            url = f"https://arcgis.coess.io/Geocortex/Essentials/REST/sites/SIN/map/mapservices/{map_service_}/rest/services/x/MapServer/{layer_}/query"

            # Append the token to the request parameters
            self.feature_params = {
                "token": f"{self.token}",  # Token form the webpage after signing in.
                "f": "json",  # Fromat we want.
                "returnGeometry": "true",  # We want the coordinates.
                "where": "('1' = '1')",  # We want to query everything within the ID we have selected.
                "spatialRel": "esriSpatialRelIntersects",
                "outFields": "*",  # All
                "outSR": "4326",  # We want the normal coordinates used in the world.
                "resultOffset": 0,  # Starting at the first record
                "resultRecordCount": 1000,  # Number of records to fetch per request
            }

            # Make the request
            self.feature_query_with_paging(url, map_service_, layer_)
            pd.json_normalize(self.all_features[layer_]).to_csv(
                os.path.join(OUTPUTS_DIR, variable, "features", f"{name_}.csv"),
                index=False,
            )
            print(f"{map_service_}, {layer_}, {name_} saved")

    def feature_query_with_paging(self, url: str, mapservice: str, layer: str):
        """
        Querying features using requests from fetch_layers_features() method.

        Args:
            url (str): url for requests.get()
            layer (str): layer that is being queried.

        Returns:
            all_features (dict): Dictionary with features' data for the MapServices / layers.
        """
        print(f"Querying Features, {mapservice}, {layer}")
        while True:

            # Make the request
            self.feature_response = requests.get(
                url, verify=False, params=self.feature_params
            )
            if [x for x in json.loads(self.feature_response.text).keys()][0] == "error":
                print("Error in querying.")
                if (mapservice, layer) in map_layers_without_features:
                    print("Error captured")
                    self.all_features[layer] = {}
                    break
                print(f"Attempting to re-log-in {mapservice}, {layer}")
                self.log_in()
                self.feature_params["token"] = self.token
                self.feature_response = requests.get(
                    url, verify=False, params=self.feature_params
                )
            else:
                pass
            data = self.feature_response.json()

            # Add features to the list
            if "features" in data:
                self.all_features[layer].extend(data["features"])
                print(
                    f"{datetime.datetime.now().strftime('%H:%M:%S')}: Retrieved {len(data['features'])} features ({layer})"
                )
            else:
                break

            # Check if the number of records fetched is less than the limit
            if len(data["features"]) < self.feature_params["resultRecordCount"]:
                break

            # Update the offset for the next query
            self.feature_params["resultOffset"] += self.feature_params[
                "resultRecordCount"
            ]

        return self.all_features

    def fetch_layers_attributes(
        self, variable: str, variable_2: int, variable_3: list = None
    ):
        """
        Send petition to fetch the attributes from the given MapService (variable: name, variable_2: ID) and the list of layers from that MapService to be retrieved (variable_3). If variable_3 = None, all layers will be retrieved.

        Args:
            variable (str): Name of the Map Service
            variable_2 (int): Id of the Map Service
            variable_3 (list, optional): List of layers to be retrieved. Defaults to None.
        """

        if isinstance(self.index_df, pd.DataFrame):
            pass
        else:
            self.get_available_layers()

        if variable not in os.listdir(OUTPUTS_DIR):
            os.mkdir(os.path.join(OUTPUTS_DIR, variable))

        if "attributes" not in os.listdir(os.path.join(OUTPUTS_DIR, variable)):
            os.mkdir(os.path.join(OUTPUTS_DIR, variable, "attributes"))

        if variable_3 == None:
            filtered_index = self.index_df[self.index_df["map_service"] == variable_2]
        else:
            filtered_index = self.index_df[
                (self.index_df["map_service"] == variable_2)
                & (self.index_df["id"].isin(variable_3))
            ]

        for map_service_, layer_, name_ in filtered_index[
            ["map_service", "id", "name"]
        ].values:

            url = f"https://arcgis.coess.io/Geocortex/Essentials/REST/sites/SIN/map/mapservices/{map_service_}/rest/services/x/MapServer/{layer_}"

            # Append the token to the request parameters
            self.attributes_params = {
                "token": self.token,  # Token form the webpage after signing in.
                "f": "json",  # Fromat we want.
                "returnGeometry": "true",  # We want the coordinates.
                "where": "('1' = '1')",  # We want to query everything within the ID we have selected.
                "spatialRel": "esriSpatialRelIntersects",
                "outFields": "*",  # All
                "outSR": "4326",  # We want the normal coordinates used in the world.
            }

            self.attributes_response = requests.get(
                url, verify=False, params=self.attributes_params
            )

            # Make the request
            if [x for x in json.loads(self.attributes_response.text).keys()][
                0
            ] == "error":
                self.log_in()
                self.attributes_params["token"] = self.token
                self.attributes_response = requests.get(
                    url, verify=False, params=self.attributes_params
                )
            else:
                pass

            with open(
                os.path.join(OUTPUTS_DIR, variable, "attributes", name_), "w"
            ) as json_file:
                json.dump(self.attributes_response.json(), json_file, indent=4)

            ## Getting the substitution dictionary for the ID column
            try:
                types_df = pd.json_normalize(self.attributes_response.json()["types"])[
                    ["id", "name"]
                ]
                types_df["column"] = self.attributes_response.json()["typeIdField"]
            except:
                types_df = pd.DataFrame()

            ## Getting all the fields for substitution
            attributes = pd.json_normalize(self.attributes_response.json()["fields"])
            self.attributes_df = pd.DataFrame()
            if ("domain.codedValues" in attributes.columns) == True:
                attributes = attributes[~attributes["domain.codedValues"].isna()]
                for col in attributes["name"]:
                    if (
                        len(
                            pd.json_normalize(
                                attributes[attributes["name"] == col][
                                    "domain.codedValues"
                                ].values[0]
                            )
                        )
                        == 0
                    ):
                        continue
                    attributes_dict = (
                        pd.json_normalize(
                            attributes[attributes["name"] == col][
                                "domain.codedValues"
                            ].values[0]
                        )
                        .set_index("code")
                        .iloc[:, 0]
                        .to_dict()
                    )
                    aux = (
                        pd.DataFrame.from_dict(
                            attributes_dict, orient="index", columns=["name"]
                        )
                        .reset_index()
                        .rename(columns={"index": "id"})
                    )
                    aux["column"] = col
                    self.attributes_df = pd.concat([self.attributes_df, aux])

            self.attributes_df = pd.concat([self.attributes_df, types_df]).reset_index(
                drop=True
            )

            self.attributes_df.to_csv(
                os.path.join(OUTPUTS_DIR, variable, "attributes", f"{name_}.csv"),
                index=False,
            )
            print(f"{map_service_}, {layer_}, {name_} saved")

    def fetch_missing_layers_features(
        self, variable: str, variable_2: int, variable_3: list = None
    ):
        """
        Send petition to fetch the features from the given MapService (variable: name, variable_2: ID) and the list of layers from that MapService to be retrieved (variable_3).
        If variable_3 = None, all layers will be retrieved.

        Args:
            variable (str): Name of the Map Service
            variable_2 (int): Id of the Map Service
            variable_3 (list, optional): List of layers to be retrieved. Defaults to None.
        """

        if isinstance(self.index_df, pd.DataFrame):
            pass
        else:
            self.get_available_layers()

        if variable not in os.listdir(OUTPUTS_DIR):
            os.mkdir(os.path.join(OUTPUTS_DIR, variable))

        if "features" not in os.listdir(os.path.join(OUTPUTS_DIR, variable)):
            os.mkdir(os.path.join(OUTPUTS_DIR, variable, "features"))

        if variable_3 == None:
            filtered_index = self.index_df[self.index_df["map_service"] == variable_2]
        else:
            filtered_index = self.index_df[
                (self.index_df["map_service"] == variable_2)
                & (self.index_df["id"].isin(variable_3))
            ]

        filtered_index = filtered_index[~filtered_index["date"].isna()]
        print(f"Filtered index for MapService {variable_2}")
        print(filtered_index)

        for map_service_, layer_, name_, date_ in filtered_index[
            ["map_service", "id", "name", "date2"]
        ].values:

            url = f"https://arcgis.coess.io/Geocortex/Essentials/REST/sites/SIN/map/mapservices/{map_service_}/rest/services/x/MapServer/{layer_}/query"

            # Append the token to the request parameters
            self.feature_params = {
                "token": f"{self.token}",  # Token form the webpage after signing in.
                "f": "json",  # Fromat we want.
                "returnGeometry": "true",  # We want the coordinates.
                "where": f"(DATEMODIFIED > DATE '{date_}')",  # We want to query everything within the ID we have selected.
                "spatialRel": "esriSpatialRelIntersects",
                "outFields": "*",  # All
                "outSR": "4326",  # We want the normal coordinates used in the world.
                "resultOffset": 0,  # Starting at the first record
                "resultRecordCount": 1000,  # Number of records to fetch per request
            }

            try:
                current_file = pd.recsv(
                    os.path.join(OUTPUTS_DIR, variable, "features", f"{name_}.csv")
                )
            except:
                continue

            # Make the request
            self.feature_query_with_paging(url, map_service_, layer_)
            new_inputs = pd.json_normalize(self.all_features[layer_])
            output = pd.concat([current_file, new_inputs])
            for_dropping = []
            for col in output.columns:
                if any(isinstance(i, list) for i in output[col]):
                    pass
                else:
                    for_dropping.append(col)
            output = output.drop_duplicates(subset=for_dropping)
            output.to_csv(
                os.path.join(OUTPUTS_DIR, variable, "features", f"{name_}.csv"),
                index=False,
            )
            print(f"{map_service_}, {layer_}, {name_} saved")

    def get_all_attributes(self):
        """
        Getting all Attributes
        """
        for id, mapserv in map_service_dict.items():
            self.fetch_layers_attributes(name_dict[mapserv], id)

    def get_new_features(self):
        """
        Getting new features
        """
        for id, mapserv in map_service_dict.items():
            self.fetch_missing_layers_features(name_dict[mapserv], id)

    def get_all_features(self):
        """
        Getting all features
        """
        self.get_available_layers()
        for id, mapserv in map_service_dict.items():
            self.fetch_layers_features(name_dict[mapserv], id)
