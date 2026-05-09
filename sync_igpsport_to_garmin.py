#!/usr/bin/env python3
"""
Sync iGPSport cycling activities to Garmin Connect.

This script uses garminconnect 0.3.2+ with use_webservice=True to avoid mobile domain issues.
"""

import os
import json
import time
import random
import datetime
import tempfile
import logging
from dateutil.parser import parse
from typing import Dict, List, Optional

from garminconnect import Garmin
from requests import Session, get as requests_get

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("igpsport-to-garmin")

LAST_SYNC_FILE = "last_sync_date.json"
OVERLAP_BUFFER_MINUTES = 5


class IGPSportClient:
    REGION_CONFIG = {
        "cn": {
            "base_url": "https://prod.zh.igpsport.com/service",
            "origin": "https://login.passport.igpsport.cn",
            "referer": "https://login.passport.igpsport.cn/",
        },
        "global": {
            "base_url": "https://prod.en.igpsport.com/service",
            "origin": "https://login.passport.igpsport.com",
            "referer": "https://login.passport.igpsport.com/",
        },
    }

    def __init__(self, username: str, password: str, region: str = "cn"):
        self.username = username
        self.password = password
        self.token = None
        self.region = (region or "cn").lower()
        if self.region not in self.REGION_CONFIG:
            logger.warning(f"Unknown region '{self.region}', defaulting to 'cn'")
            self.region = "cn"
        cfg = self.REGION_CONFIG[self.region]
        self.base_url = cfg["base_url"]
        self.session = Session()
        self.session.headers.update({
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "origin": cfg["origin"],
            "referer": cfg["referer"],
        })

    def login(self) -> bool:
        url = f"{self.base_url}/auth/account/login"
        data = {"username": self.username, "password": self.password, "appId": "igpsport-web"}
        try:
            resp = self.session.post(url, json=data)
            resp.raise_for_status()
            result = resp.json()
            if result["code"] == 0 and "data" in result:
                self.token = result["data"]["access_token"]
                self.session.headers.update({"authorization": f"Bearer {self.token}"})
                logger.info("Successfully logged in to iGPSport")
                return True
            else:
                logger.error(f"Login failed: {result.get('message')}")
                return False
        except Exception as e:
            logger.error(f"Login error: {e}")
            return False

    def get_activities(self, page_no: int = 1, page_size: int = 20) -> Dict:
        if not self.token:
            return {}
        url = f"{self.base_url}/web-gateway/web-analyze/activity/queryMyActivity"
        params = {"pageNo": page_no, "pageSize": page_size, "reqType": 0, "sort": 1}
        try:
            resp = self.session.get(url, params=params)
            resp.raise_for_status()
            result = resp.json()
            return result.get("data", {}) if result.get("code") == 0 else {}
        except Exception as e:
            logger.error(f"Error getting activities: {e}")
            return {}

    def get_activity_detail(self, ride_id: int) -> Dict:
        if not self.token:
            return {}
        url = f"{self.base_url}/web-gateway/web-analyze/activity/queryActivityDetail/{ride_id}"
        try:
            resp = self.session.get(url)
            resp.raise_for_status()
            result = resp.json()
            return result.get("data", {}) if result.get("code") == 0 else {}
        except Exception as e:
            logger.error(f"Error getting activity detail: {e}")
            return {}

    def download_fit_file(self, fit_url: str) -> Optional[bytes]:
        try:
            resp = requests_get(fit_url)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.error(f"Download error: {e}")
            return None


class GarminClient:
    def __init__(self, email: str, password: str, domain: str, max_retries: int = 3, retry_delay: int = 5):
        self.email = email
        self.password = password
        self.domain = domain  # 'cn' or 'com'
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.client = None
        self.authenticated = False

    def authenticate(self, force: bool = False) -> bool:
        try:
            is_cn = (self.domain == 'cn')
            logger.info(f"Logging in to Garmin {'CN' if is_cn else 'global'} region...")
            # v0.3.4 版本不再需要 use_webservice 参数
            self.client = Garmin(
                email=self.email,
                password=self.password,
                is_cn=is_cn
            )
            self.client.login()
            self.client.get_full_name()  # 验证登录
            logger.info("Successfully authenticated with Garmin Connect")
            self.authenticated = True
            return True
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            self.authenticated = False
            self.client = None
            return False

    def get_activities(self, start_date=None, limit=10) -> List[Dict]:
        if not self.authenticated and not self.authenticate():
            return []
        try:
            activities = self.client.get_activities(start=0, limit=limit)
            return activities if isinstance(activities, list) else []
        except Exception as e:
            logger.error(f"Get activities error: {e}")
            self.authenticated = False
            if self.authenticate(force=True):
                return self.get_activities(start_date, limit)
            return []

    def upload_fit(self, fit_data: bytes, activity_name=None) -> Optional[Dict]:
        if not self.authenticated and not self.authenticate():
            return None

        retries = 0
        last_error = None
        tmp_path = None

        while retries <= self.max_retries:
            try:
                if retries > 0:
                    delay = (self.retry_delay * (2**(retries-1))) + random.uniform(0,2)
                    time.sleep(delay)
                with tempfile.NamedTemporaryFile(suffix=".fit", delete=False) as f:
                    f.write(fit_data)
                    tmp_path = f.name
                resp = self.client.upload_activity(tmp_path)
                if tmp_path and os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                logger.info("Upload successful")
                return resp
            except Exception as e:
                last_error = e
                retries += 1
                logger.warning(f"Upload attempt {retries} failed: {e}")
                error_str = str(e).lower()
                if "auth" in error_str or "login" in error_str:
                    self.authenticated = False
                    self.authenticate(force=True)
                if "409" in error_str:
                    logger.warning("Duplicate activity, skipping")
                    if tmp_path and os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                    return None
                if tmp_path and os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                    tmp_path = None
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        logger.error(f"Upload failed after {self.max_retries} attempts")
        return None


def load_last_sync_date():
    try:
        if os.path.exists(LAST_SYNC_FILE):
            with open(LAST_SYNC_FILE) as f:
                return datetime.datetime.fromisoformat(json.load(f)["last_sync_date"])
    except Exception as e:
        logger.error(f"Load last sync date error: {e}")
    return datetime.datetime.now() - datetime.timedelta(days=30)


def save_last_sync_date(dt):
    try:
        with open(LAST_SYNC_FILE, "w") as f:
            json.dump({"last_sync_date": dt.isoformat()}, f)
    except Exception as e:
        logger.error(f"Save last sync date error: {e}")


def activities_overlap(st1, d1, st2, d2):
    e1 = st1 + datetime.timedelta(seconds=d1)
    e2 = st2 + datetime.timedelta(seconds=d2)
    buf = datetime.timedelta(minutes=OVERLAP_BUFFER_MINUTES)
    return (st1 - buf <= st2 <= e1 + buf) or (st1 - buf <= e2 <= e1 + buf) or \
           (st2 - buf <= st1 <= e2 + buf) or (st2 - buf <= e1 <= e2 + buf)


def collect_activities_to_sync(igpsport, garmin, last_sync_date):
    garmin_acts = garmin.get_activities(limit=20)
    garmin_times = [(parse(a.get("startTimeLocal", "")), a.get("duration", 0)) for a in garmin_acts if a.get("startTimeLocal")]

    data = igpsport.get_activities(1, 20)
    if not data or "rows" not in data:
        return []

    to_sync = []
    for act in data["rows"]:
        try:
            start_str = act.get("startTime", "")
            act_id = act.get("rideId")
            if "." in start_str:
                y, m, d = start_str.split(".")
                start_date = datetime.datetime(int(y), int(m), int(d))
            else:
                start_date = parse(start_str)
            if start_date.date() < last_sync_date.date():
                logger.info(f"Skipping {act_id} (older)")
                continue

            detail = igpsport.get_activity_detail(act_id)
            if not detail:
                continue
            det_start = parse(detail.get("startTime", ""))
            det_dur = detail.get("totalTime", 0)

            overlap = any(activities_overlap(det_start, det_dur, gs, gd) for gs, gd in garmin_times)
            if overlap:
                continue

            fit_url = detail.get("fitUrl") or act.get("fitOssPath")
            if not fit_url:
                continue
            to_sync.append({"activity_id": act_id, "fit_url": fit_url, "start_time": det_start, "duration": det_dur})
        except Exception as e:
            logger.error(f"Error processing activity: {e}")
    return to_sync


def main():
    igpsport_user = os.environ.get("IGPSPORT_USERNAME")
    igpsport_pass = os.environ.get("IGPSPORT_PASSWORD")
    igpsport_region = os.environ.get("IGPSPORT_REGION")
    garmin_email = os.environ.get("GARMIN_EMAIL")
    garmin_pass = os.environ.get("GARMIN_PASSWORD")
    garmin_domain = os.environ.get("GARMIN_DOMAIN") or "com"

    if not all([igpsport_user, igpsport_pass, garmin_email, garmin_pass]):
        logger.error("Missing environment variables")
        return

    igpsport = IGPSportClient(igpsport_user, igpsport_pass, igpsport_region)
    garmin = GarminClient(garmin_email, garmin_pass, garmin_domain)

    if not igpsport.login():
        logger.error("iGPSport login failed")
        return

    last_sync = load_last_sync_date()
    logger.info(f"Last sync date: {last_sync}")

    activities = collect_activities_to_sync(igpsport, garmin, last_sync)
    if not activities:
        logger.info("No new activities")
        return

    logger.info(f"Found {len(activities)} activities to sync")

    if not garmin.authenticate():
        logger.error("Garmin authentication failed")
        return

    synced = 0
    latest_date = None
    for act in activities:
        fit_data = igpsport.download_fit_file(act["fit_url"])
        if not fit_data:
            logger.warning(f"Download failed for {act['activity_id']}")
            continue
        if garmin.upload_fit(fit_data):
            synced += 1
            if latest_date is None or act["start_time"] > latest_date:
                latest_date = act["start_time"]
            time.sleep(2)
        else:
            logger.warning(f"Upload failed for {act['activity_id']}")

        if latest_date and synced > 0:
            save_last_sync_date(latest_date)

    if latest_date and synced > 0:
        save_last_sync_date(latest_date)
        logger.info(f"Updated last sync date to {latest_date}")

    logger.info(f"Done: {synced}/{len(activities)} uploaded")


if __name__ == "__main__":
    main()
