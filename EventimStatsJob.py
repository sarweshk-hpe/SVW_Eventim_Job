import os
import requests
import pandas as pd
import json
import datetime
import logging
from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://api.eventimsports.com"
TOKEN_ENDPOINT = "/token"
REGISTRATIONS_ENDPOINT = "/webhook/v1/events/registrations/"

def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def get_credentials() -> Dict[str, str]:
    client_id = os.environ.get("EVENTIM_CLIENT_ID")
    client_secret = os.environ.get("EVENTIM_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise EnvironmentError("Missing EVENTIM_CLIENT_ID or EVENTIM_CLIENT_SECRET in environment variables")
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }

def get_report_dir() -> str:
    report_dir = os.environ.get("REPORT_OUTPUT_DIR")
    if not report_dir:
        raise EnvironmentError("Missing REPORT_OUTPUT_DIR in environment variables")
    os.makedirs(report_dir, exist_ok=True)
    return report_dir

def fetch_access_token(session: requests.Session) -> str:
    try:
        headers = {'User-Agent': 'Automation_Client'}
        response = session.post(f"{BASE_URL}{TOKEN_ENDPOINT}", headers=headers, data=get_credentials())
        response.raise_for_status()
        return response.json()['access_token']
    except RequestException as e:
        logging.error(f"Failed to fetch access token: {e}")
        raise

def fetch_registration_uuids(session: requests.Session, token: str) -> List[str]:
    headers = {
        'User-Agent': 'Automation_Client',
        'Authorization': f'Bearer {token}'
    }
    try:
        response = session.get(f"{BASE_URL}{REGISTRATIONS_ENDPOINT}", headers=headers)
        response.raise_for_status()
        return [reg['uuid'] for reg in response.json()]
    except RequestException as e:
        logging.error(f"Failed to fetch registrations: {e}")
        raise

def fetch_registration_detail(session: requests.Session, token: str, uuid: str) -> Optional[pd.DataFrame]:
    headers = {
        'User-Agent': 'Automation_Client',
        'Authorization': f'Bearer {token}'
    }
    try:
        url = f"{BASE_URL}{REGISTRATIONS_ENDPOINT}{uuid}"
        response = session.get(url, headers=headers)
        response.raise_for_status()
        df = pd.json_normalize(response.json())
        df.columns = [col.split('.')[-1] for col in df.columns]
        df['timestamp'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        return df
    except RequestException as e:
        logging.warning(f"Failed to fetch data for UUID {uuid}: {e}")
        return None

def main():
    session = create_session()
    try:
        token = fetch_access_token(session)
        uuids = fetch_registration_uuids(session, token)
        logging.info(f"Fetched {len(uuids)} registrations")

        all_data: List[pd.DataFrame] = []
        for uuid in uuids:
            df = fetch_registration_detail(session, token, uuid)
            if df is not None:
                all_data.append(df)

        if not all_data:
            logging.warning("No data fetched for any registration.")
            return

        final_df = pd.concat(all_data, ignore_index=True)
        report_dir = get_report_dir()
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        filename = os.path.join(report_dir, f"Eventim_Stats_SVW_{timestamp}.xlsx")
        final_df.to_excel(filename, index=False)
        logging.info(f"Saved output to {filename}")

    except Exception as e:
        logging.error(f"Script failed: {e}")

if __name__ == "__main__":
    main()
