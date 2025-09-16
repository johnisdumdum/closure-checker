# app.py
import os
import json
import logging
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify
import msal
from dateutil import parser as dateparser
from shapely.geometry import shape, Point, mapping
from shapely.ops import transform
from pyproj import Transformer

# ---- Config / env ----
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
TENANT_ID = os.environ.get("TENANT_ID")
SP_SITE_URL = os.environ.get("SP_SITE_URL")  # e.g. https://contoso.sharepoint.com/sites/YourSite
CITY_LAYER_QUERY = os.environ.get("CITY_LAYER_QUERY")  # ArcGIS GeoJSON URL
BUFFER_METERS = float(os.environ.get("BUFFER_METERS", "30"))

# Graph base
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Setup Flask
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Projection transformers
to_3857 = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True).transform
to_4326 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True).transform

# ---- MSAL token helper ----
def acquire_token():
    if not CLIENT_ID or not CLIENT_SECRET or not TENANT_ID:
        raise Exception("CLIENT_ID, CLIENT_SECRET and TENANT_ID environment variables are required.")
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = msal.ConfidentialClientApplication(CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET)
    scopes = ["https://graph.microsoft.com/.default"]
    result = app.acquire_token_for_client(scopes=scopes)
    if "access_token" not in result:
        raise Exception(f"Unable to acquire token: {result.get('error_description') or result}")
    return result["access_token"]

# ---- Graph / SharePoint helpers ----
def get_site_id_from_url(access_token, site_url):
    # site_url like: https://contoso.sharepoint.com/sites/YourSite
    # split hostname and path
    if not site_url.startswith("https://"):
        raise ValueError("SP_SITE_URL must be a full https URL to your SharePoint site.")
    parts = site_url[len("https://"):].split("/", 1)
    hostname = parts[0]  # contoso.sharepoint.com
    path = ""
    if len(parts) > 1:
        path = "/" + parts[1]  # e.g. /sites/YourSite
    endpoint = f"{GRAPH_BASE}/sites/{hostname}:{path}"
    headers = {"Authorization": f"Bearer {access_token}"}
    r = requests.get(endpoint, headers=headers)
    r.raise_for_status()
    data = r.json()
    # data['id'] is the siteId used in other calls
    return data["id"]

def get_list_id(access_token, site_id, list_name):
    headers = {"Authorization": f"Bearer {access_token}"}
    # Query lists and find by displayName
    r = requests.get(f"{GRAPH_BASE}/sites/{site_id}/lists", headers=headers)
    r.raise_for_status()
    for li in r.json().get("value", []):
        if li.get("displayName", "").lower() == list_name.lower():
            return li["id"]
    raise Exception(f"List named '{list_name}' not found on site.")

def fetch_list_items(access_token, site_id, list_id):
    headers = {"Authorization": f"Bearer {access_token}"}
    # Expand fields for each item
    url = f"{GRAPH_BASE}/sites/{site_id}/lists/{list_id}/items?expand=fields"
    items = []
    while url:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
        items.extend(data.get("value", []))
        # pagination
        url = data.get("@odata.nextLink")
    return items

def find_item_by_field(access_token, site_id, list_id, field_name, value):
    """Try to use $filter first; fallback to local scan"""
    headers = {"Authorization": f"Bearer {access_token}"}
    # Attempt OData filter on fields; note: not all tenants/supports all field filters; use fallback
    safe_val = str(value).replace("'", "''")
    try:
        url = f"{GRAPH_BASE}/sites/{site_id}/lists/{list_id}/items?$filter=fields/{field_name} eq '{safe_val}'&$expand=fields"
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
        vals = data.get("value", [])
        if vals:
            return vals[0]
    except Exception:
        app.logger.info("Filter query failed — falling back to full list scan")

    # fallback: fetch all and scan locally
    items = fetch_list_items(access_token, site_id, list_id)
    for it in items:
        fields = it.get("fields", {})
        if str(fields.get(field_name)) == str(value):
            return it
    return None

def create_list_item(access_token, site_id, list_id, fields_dict):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    url = f"{GRAPH_BASE}/sites/{site_id}/lists/{list_id}/items"
    body = {"fields": fields_dict}
    r = requests.post(url, headers=headers, json=body)
    r.raise_for_status()
    return r.json()

def update_list_item(access_token, site_id, list_id, item_id, fields_dict):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    url = f"{GRAPH_BASE}/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
    r = requests.patch(url, headers=headers, json=fields_dict)
    r.raise_for_status()
    return r.json()

# ---- ArcGIS / geometry helpers ----
def fetch_arcgis_features():
    if not CITY_LAYER_QUERY:
        raise Exception("CITY_LAYER_QUERY environment variable required.")
    r = requests.get(CITY_LAYER_QUERY)
    r.raise_for_status()
    return r.json().get("features", [])

def make_buffered_geometry(geojson_geometry, buffer_meters=BUFFER_METERS):
    geom = shape(geojson_geometry)
    # project to 3857, buffer, project back
    proj_geom = transform(to_3857, geom)
    buffered = proj_geom.buffer(buffer_meters)
    buffered_wgs84 = transform(to_4326, buffered)
    return buffered_wgs84

def feature_active(feature_props):
    # Look for start / end properties (common ArcGIS names: StartDate, start, FROM_DATE, etc.)
    # Customize if your dataset has specific field names.
    def try_get_date(keys):
        for k in keys:
            v = feature_props.get(k)
            if v:
                try:
                    return dateparser.parse(v)
                except Exception:
                    pass
        return None

    # common guesses
    start_keys = ["start", "StartDate", "from", "from_dt", "startDate", "FROM_DATE"]
    end_keys = ["end", "EndDate", "to", "to_dt", "endDate", "TO_DATE"]

    start_dt = try_get_date(start_keys)
    end_dt = try_get_date(end_keys)

    now = datetime.now(timezone.utc)
    if start_dt and start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt and end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)

    # If both exist: check now within [start,end].
    if start_dt and end_dt:
        return start_dt <= now <= end_dt
    # If only start exists: active if start <= now
    if start_dt and not end_dt:
        return start_dt <= now
    # If only end exists: active if now <= end
    if end_dt and not start_dt:
        return now <= end_dt
    # If neither, assume active
    return True

# ---- Core check logic ----
def check_site_against_features(lat, lng, features):
    point = Point(lng, lat)  # shapely uses (x=lon, y=lat)
    conflicts = []
    for feat in features:
        props = feat.get("properties", {})
        geom = feat.get("geometry")
        if not geom:
            continue
        try:
            buffered = make_buffered_geometry(geom)
        except Exception as e:
            app.logger.warning(f"Failed to buffer feature: {e}")
            continue
        if buffered.contains(point):
            if feature_active(props):
                # collect minimal conflict summary
                conflicts.append({
                    "id": props.get("OBJECTID") or props.get("id") or props.get("FID"),
                    "summary": props.get("Summary") or props.get("name") or props.get("type"),
                    "start": props.get("start") or props.get("StartDate"),
                    "end": props.get("end") or props.get("EndDate"),
                    "raw_props": props
                })
    blocked = len(conflicts) > 0
    return blocked, conflicts

# ---- Endpoints ----
@app.route("/api/check", methods=["POST"])
def api_check():
    data = request.get_json(force=True)
    lat = data.get("lat")
    lng = data.get("lng")
    site_id = data.get("site_id")
    name = data.get("name")
    if lat is None or lng is None:
        return jsonify({"error": "lat and lng required"}), 400

    features = fetch_arcgis_features()
    blocked, conflicts = check_site_against_features(float(lat), float(lng), features)
    return jsonify({
        "site_id": site_id,
        "name": name,
        "lat": lat,
        "lng": lng,
        "is_blocked": blocked,
        "num_conflicts": len(conflicts),
        "conflicts": conflicts,
        "last_checked_utc": datetime.now(timezone.utc).isoformat()
    })

@app.route("/api/run", methods=["POST"])
def api_run():
    """
    Run full sync:
    - read SiteMaster
    - compute conflicts for each site
    - upsert row in ClosureStatus
    """
    token = acquire_token()
    site_id = get_site_id_from_url(token, SP_SITE_URL)
    # list names (change if your lists are named differently)
    sites_list_name = "SiteMaster"
    closures_list_name = "ClosureStatus"

    sites_list_id = get_list_id(token, site_id, sites_list_name)
    closures_list_id = get_list_id(token, site_id, closures_list_name)

    site_items = fetch_list_items(token, site_id, sites_list_id)
    features = fetch_arcgis_features()

    results = []
    for it in site_items:
        fields = it.get("fields", {})
        # try multiple possible field names; adjust if your columns have different internal names
        sid = fields.get("site_id") or fields.get("SiteID") or fields.get("ID") or fields.get("Title")
        name = fields.get("name") or fields.get("Name") or fields.get("title")
        lat = fields.get("lat") or fields.get("latitude")
        lng = fields.get("lng") or fields.get("longitude")
        if lat is None or lng is None:
            app.logger.info(f"Skipping site {sid} missing coords")
            continue

        blocked, conflicts = check_site_against_features(float(lat), float(lng), features)
        conflicts_json = json.dumps(conflicts)
        last_checked = datetime.now(timezone.utc).isoformat()
        # Build fields for ClosureStatus list
        closure_fields = {
            "site_id": str(sid),
            "sitename": str(name),
            "is_blocked": str(blocked).lower() == "true",  # ensure boolean
            "num_conflicts": len(conflicts),
            "conflicts_json": conflicts_json,
            "last_checked_utc": last_checked
        }

        # Upsert: try to find existing item with same site_id
        existing = find_item_by_field(token, site_id, closures_list_id, "site_id", sid)
        if existing:
            item_id = existing["id"]
            update_list_item(token, site_id, closures_list_id, item_id, closure_fields)
            action = "updated"
        else:
            created = create_list_item(token, site_id, closures_list_id, closure_fields)
            action = "created"

        results.append({"site_id": sid, "action": action, "blocked": blocked, "num_conflicts": len(conflicts)})

    return jsonify({"status": "done", "results": results})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
