import requests
import json
import os
import logging
from typing import List, Dict, Optional, Iterable
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from minio import Minio
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

YANDEX_DIRECT_BASE_URL = "https://api.direct.yandex.com/json/v5"
DIRECT_CLIENT_LOGIN = "e-20035215"
YANDEX_DIRECT_TOKEN = "y0__xCq2Pr8BRjgmDog6tL1rRRmflVziW5xUSrpp1A3rjxJmZ2haQ"
YANDEX_WEBAPI_URL = "https://direct.yandex.ru/wizard/web-api/aggregate"

load_dotenv()

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

client = Minio(
    endpoint=S3_ENDPOINT_URL,
    access_key=S3_ACCESS_KEY,
    secret_key=S3_SECRET_KEY,
    secure=False
)

def load_cookies_from_minio(bucket_name=S3_BUCKET_NAME, object_name="cookies_for_campaigns/user_1_cookies.json"):
    response = client.get_object(bucket_name, object_name)
    data = response.read().decode("utf-8")
    response.close()
    response.release_conn()
    return json.loads(data)

def update_headers_with_csrf(headers: dict, cookies: dict) -> dict:
    csrf_token = cookies.get("_direct_csrf_token")
    if csrf_token:
        headers["x-csrf-token"] = csrf_token
    return headers

def _chunked(it: Iterable, n: int):
    it = list(it)
    for i in range(0, len(it), n):
        yield it[i:i+n]

def _ensure_bearer(token: Optional[str]) -> str:
    token = token.strip()
    if not token.lower().startswith("bearer "):
        return "Bearer " + token
    return token

def recursive_find_tracking(obj) -> Optional[str]:
    if obj is None:
        return None
    if isinstance(obj, str):
        if "utm_" in obj or "utm=" in obj or "{campaign" in obj:
            return obj
    if isinstance(obj, dict):
        for v in obj.values():
            found = recursive_find_tracking(v)
            if found:
                return found
    if isinstance(obj, list):
        for v in obj:
            found = recursive_find_tracking(v)
            if found:
                return found
    return None

def collect_campaigns_with_tracking(data) -> Dict[str, str]:
    found = {}
    def walk(x):
        if isinstance(x, dict):
            cid = x.get("Id") or x.get("id")
            if cid and cid not in found:
                t = recursive_find_tracking(x)
                if t:
                    found[str(cid)] = t
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for el in x:
                walk(el)
    walk(data)
    return found

def direct_api_get_tracking_params(campaign_ids: List[str]) -> Dict[str, str]:
    token = _ensure_bearer(YANDEX_DIRECT_TOKEN)
    headers = {
        "Authorization": token,
        "Client-Login": DIRECT_CLIENT_LOGIN,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    found = {}
    for chunk in _chunked(campaign_ids, 1000):
        req = {
            "method": "get",
            "params": {
                "SelectionCriteria": {"Ids": [int(x) for x in chunk]},
                "FieldNames": ["Id", "Name"],
                "TextCampaignFieldNames": ["TrackingParams"]
            }
        }
        r = requests.post(f"{YANDEX_DIRECT_BASE_URL}/campaigns", json=req, headers=headers)
        if r.status_code != 200:
            logger.warning("API error %s", r.text[:300])
            continue
        j = r.json()
        found.update(collect_campaigns_with_tracking(j))
    return found

def get_tracking_params_web(campaign_id: str, cookies, headers):
    params = {
        "query[ulogin]": "e-20035215",
        "query[id]": campaign_id,
        "route": "campaign",
        "ulogin": "e-20035215"
    }
    r = requests.get(YANDEX_WEBAPI_URL, headers=headers, cookies=cookies, params=params)
    data = r.json()
    Path(f"campaign_{campaign_id}_raw.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return recursive_find_tracking(data)

def get_tracking_from_banner(campaign_id: str) -> Optional[str]:
    token = _ensure_bearer(YANDEX_DIRECT_TOKEN)
    headers = {
        "Authorization": token,
        "Client-Login": DIRECT_CLIENT_LOGIN,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    req = {
        "method": "get",
        "params": {
            "SelectionCriteria": {"CampaignIds": [int(campaign_id)]},
            "FieldNames": ["Id", "CampaignId", "Type"],
            "CpmBannerAdBuilderAdFieldNames": ["Href"]
        }
    }

    r = requests.post(f"{YANDEX_DIRECT_BASE_URL}/ads", json=req, headers=headers)
    if r.status_code != 200:
        logger.warning("Banner API error %s", r.text[:300])
        return None

    j = r.json()
    Path(f"campaigns_ads_{campaign_id}_raw.json").write_text(json.dumps(j, ensure_ascii=False, indent=2), encoding="utf-8")

    ads = j.get("result", {}).get("Ads", [])
    for ad in ads:
        href = ad.get("CpmBannerAdBuilderAd", {}).get("Href")
        if href and "utm_" in href:
            query = urlparse(href).query or href.split("?", 1)[-1]
            return query
    return None
import glob

def cleanup_temp_json_files():
    patterns = ["campaign_*.json", "campaigns_ads_*.json"]
    deleted = 0
    for pattern in patterns:
        for path in glob.glob(pattern):
            try:
                os.remove(path)
                deleted += 1
            except Exception as e:
                logger.warning(f"Не удалось удалить {path}: {e}")
    if deleted:
        logger.info(f"Удалено {deleted} временных JSON-файлов.")
    else:
        logger.info("Временные JSON-файлы отсутствуют — нечего удалять.")

def get_campaign_params(campaign_ids: List[str]) -> Dict[str, Optional[str]]:
    cookies = load_cookies_from_minio()
    headers = json.load(open("headers.json", encoding="utf-8"))
    headers = update_headers_with_csrf(headers, cookies)

    result = {}

    api_data = direct_api_get_tracking_params(campaign_ids)
    result.update(api_data)

    for cid in campaign_ids:
        if cid not in result or not result[cid]:
            result[cid] = get_tracking_params_web(cid, cookies, headers)

    for cid in campaign_ids:
        if not result.get(cid):
            result[cid] = get_tracking_from_banner(cid)

    cleanup_temp_json_files()

    return result


if __name__ == "__main__":
    # print(get_campaign_params(["703986845", "703723040", "702469969", " 702468674", "702470368", "702496972", "702498562"]))
    print(get_campaign_params(['704010325']))
