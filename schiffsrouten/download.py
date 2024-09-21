"""Load the Raster selected via json file."""

import json
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Polygon, shape
from tqdm import tqdm


@dataclass
class UserData:
    name: str
    password: str


def get_keycloak(user: UserData) -> str:
    """Lock up a bearer token."""
    data = {
        "client_id": "cdse-public",
        "username": user.name,
        "password": user.password,
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


@dataclass
class Period:
    """Period to search in."""

    start: str
    end: str


def get_search_string(collection: str, area: Polygon, period: Period, attributes: dict[str, str]) -> str:
    """Build a search string for a given collection and area."""
    add_attributes = ""
    if len(attributes):
        for name, value in attributes.items():
            add_attributes += f" and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq '{name}' and att/OData.CSC.StringAttribute/Value eq '{value}')"

    base_query = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{collection}' and OData.CSC.Intersects(area=geography'SRID=4326;{area}') and ContentDate/Start gt {period.start}T00:00:00.000Z and ContentDate/Start lt {period.end}T00:00:00.000Z"
    final_query = "&$count=True&$top=1000&$expand=Attributes"
    return base_query + add_attributes + final_query


def to_geo(data: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert the DataFrame to usable GeoDataFrame."""
    local_df = data.copy()
    return (
        local_df.assign(geometry=lambda x: x.GeoFootprint.apply(shape))
        .set_geometry("geometry")
        .assign(identifier=lambda x: x.Name.str.split(".").str[0])
        .loc[lambda x: ~x["Name"].str.contains("L1C")]
    )


def download_raster(properties: dict, token: str, save_dir: Path) -> None:
    """Download raster using the keycloak token."""
    session = requests.Session()

    session.headers.update({"Authorization": f"Bearer {token}"})
    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({properties['Id']})/$value"
    response = session.get(url, allow_redirects=False)
    while response.status_code in (301, 302, 303, 307):
        url = response.headers["Location"]
        response = session.get(url, allow_redirects=False)

    file = session.get(url, verify=False, allow_redirects=True)

    with (save_dir / f"{properties['identifier']}.zip").open("wb") as f:
        f.write(file.content)


with Path("../config/download.json").open() as f:
    config = json.load(f)

json_ = requests.get(
    get_search_string(
        collection=config["data_collection"],
        area=Polygon(config["area"]),
        period=Period(**config["period"]),
        attributes=config["attributes"]
    ),
    timeout=10,
).json()
p_df = pd.DataFrame.from_dict(json_["value"])  # Fetch available dataset
if p_df.shape[0] == 0:
    error_txt = "No data found"
    raise AttributeError(error_txt)

p_gdf = to_geo(p_df)
num_samples = len(p_gdf)
if num_samples == 0:
    error_txt = "No tiles found for given period"
    raise AttributeError(error_txt)

## download all tiles from server
token = get_keycloak(UserData(**config["user"]))
for feat in tqdm(p_gdf.iterfeatures()):
    try:
        download_raster(feat["properties"],token , Path(config["save_dir"]))
    except:
        print("problem with server")
