"""Load the Raster selected via json file."""

import json
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Polygon, shape
from shapely.geometry.polygon import Polygon

with Path("../config/download.json").open() as f:
    config = json.load(f)

copernicus_user = config["copernicus_user"]  # copernicus User
copernicus_password = config["copernicus_password"]  # copernicus Password
ft = Polygon(config["area"])  # WKT Representation of BBOX
data_collection = config["data_collection"]  # Sentinel satellite

start_date_string = config["start_date"]
start_date = datetime.strptime(start_date_string, "%Y-%m-%d").date
end_date_string = config["end_date"]
end_date = datetime.strptime(end_date_string, "%Y-%m-%d").date()
attributes = config["attributes"]


def get_keycloak(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    try:
        r = requests.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            data=data,
            timeout=10,
        )
        r.raise_for_status()
    except Exception:
        error_txt = f"Keycloak token creation failed. Response from the server was: {r.json()}"
        raise AttributeError(error_txt) from None
    return r.json()["access_token"]

add_attributes = ""
#if len(attributes):
#    for name, value in attributes.items():
#        add_attributes += f" and Attributes/OData.StringAttribute/any(att:att/Name eq '{name}' and att/OData.StringAttribute/Value eq '{value}')"


json_ = requests.get(
    f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{data_collection}' and OData.CSC.Intersects(area=geography'SRID=4326;{ft}') and ContentDate/Start gt {start_date_string}T00:00:00.000Z and ContentDate/Start lt {end_date_string}T00:00:00.000Z&$count=True&$top=1000"
).json()
p_df = pd.DataFrame.from_dict(json_["value"])  # Fetch available dataset
if p_df.shape[0] > 0:
    p_df["geometry"] = p_df["GeoFootprint"].apply(shape)
    p_gdf = gpd.GeoDataFrame(p_df).set_geometry("geometry")  # Convert PD to GPD
    p_gdf = p_gdf[~p_gdf["Name"].str.contains("L1C")]  # Remove L1C dataset
    print(f" total tiles found {len(p_gdf)}")
    p_gdf["identifier"] = p_gdf["Name"].str.split(".").str[0]
    num_samples = len(p_gdf)

    if num_samples == 0:
        print("No tiles found for today")
    else:
        ## download all tiles from server
        for index, feat in enumerate(p_gdf.iterfeatures()):
            try:
                session = requests.Session()
                keycloak_token = get_keycloak(copernicus_user, copernicus_password)
                session.headers.update({"Authorization": f"Bearer {keycloak_token}"})
                url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({feat['properties']['Id']})/$value"
                response = session.get(url, allow_redirects=False)
                while response.status_code in (301, 302, 303, 307):
                    url = response.headers["Location"]
                    response = session.get(url, allow_redirects=False)
                print(feat["properties"]["Id"])
                file = session.get(url, verify=False, allow_redirects=True)

                with open(
                    f"{feat['properties']['identifier']}.zip",  # location to save zip from copernicus
                    "wb",
                ) as p_df:
                    print(feat["properties"]["Name"])
                    p_df.write(file.content)
            except:
                print("problem with server")
else:
    print("no data found")
