#!/usr/bin/env python3
"""
Sync iGPSport cycling activities to Garmin Connect.

This script runs periodically in GitHub Actions to download FIT files from iGPSport
and upload them to Garmin Connect, with filtering to avoid duplicates.
"""

import os
import json
import time
import random
import datetime
from pathlib import Path
import logging
from dateutil.parser import parse
from typing import Dict, List, Optional

import garminconnect
from garminconnect import Garmin
from requests import Session, get as requests_get

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("igpsport-to-garmin")

# Constants
LAST_SYNC_FILE = "last_sync_date.json"
OVERLAP_BUFFER_MINUTES = 5  # Consider activities overlapping if within 5 minutes


class IGPSportClient:
    """Client for the iGPSport API."""

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
            logger.warning(
                f"Unknown igpsport region '{self.region}', defaulting to 'cn'"
            )
            self.region = "cn"

        region_cfg = self.REGION_CONFIG[self.region]
        self.base_url = region_cfg["base_url"]
        self.session = Session()
        self.session.headers.update(
            {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                "accept": "application/json, text/plain, */*",
                "content-type": "application/json",
                "origin": region_cfg["origin"],
                "referer": region_cfg["referer"],
            }
        )

    def login(self) -> bool:
        """Login to iGPSport."""
        url = f"{self.base_url}/auth/account/login"
        data = {
            "username": self.username,
            "password": self.password,
            "appId": "igpsport-web",
        }

        try:
            response = self.session.post(url, json=data)
            response.raise_for_status()
            result = response.json()

            if result["code"] == 0 and "data" in result:
                access_token = result["data"]["access_token"]
                self.token = access_token
                self.session.headers.update({"authorization": f"Bearer {access_token}"})
                logger.info("Successfully logged in to iGPSport")
                return True
            else:
                logger.error(f"Login failed: {result.get('message', 'Unknown error')}")
                return False
        except Exception as e:
            logger.error(f"Error during login: {e}")
            return False

    def get_activities(self, page_no: int = 1, page_size: int = 20) -> Dict:
        """Get list of activities."""
        if not self.token:
            logger.error("Not logged in. Call login() first.")
            return {}

        url = f"{self.base_url}/web-gateway/web-analyze/activity/queryMyActivity"
        params = {"pageNo": page_no, "pageSize": page_size, "reqType": 0, "sort": 1}

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            result = response.json()

            if result["code"] == 0 and "data" in result:
                return result["data"]
            else:
                logger.error(
                    f"Failed to get activities: {result.get('message', 'Unknown error')}"
                )
                return {}
        except Exception as e:
            logger.error(f"Error getting activities: {e}")
            return {}

    def get_activity_detail(self, ride_id: int) -> Dict:
        """Get details for a specific activity."""
        if not self.token:
            logger.error("Not logged in. Call login() first.")
            return {}

        url = f"{self.base_url}/web-gateway/web-analyze/activity/queryActivityDetail/{ride_id}"

        try:
            response = self.session.get(url)
            response.raise_for_status()
            result = response.json()

            if result["code"] == 0 and "data" in result:
                return result["data"]
            else:
                logger.error(
                    f"Failed to get activity detail: {result.get('message', 'Unknown error')}"
                )
                return {}
        except Exception as e:
            logger.error(f"Error getting activity detail: {e}")
            return {}

    def download_fit_file(self, fit_url: str) -> Optional[bytes]:
        """Download a FIT file from the given URL."""
        try:
            response = requests_get(fit_url)
            response.raise_for_status()
            return response.content
        except Exception as e:
            logger.error(f"Error downloading FIT file: {e}")
            return None


class GarminClient:
    """
    Client for Garmin Connect API using the python-garminconnect library.

    Uses version 0.3.0+ for native auth engine, token management and cloudflare bypass.
    """

    def __init__(
        self,
        email: str,
        password: str,
        domain: str,
        max_retries: int = 3,
        retry_delay: int = 5,
    ):
        self.email = email
        self.password = password
        # domain 形如 'cn' 或 'com'
        self.domain = domain
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.client: Optional[Garmin] = None
        self.authenticated = False

    def authenticate(self, force: bool = False) -> bool:
        """
        Authenticate with Garmin Connect.

        Args:
            force: If True, force a new authentication even if a token exists

        Returns:
            True if authentication is successful, False otherwise
        """
        try:
            # 新版库会根据 domain 参数自动决定使用 garmin.com 还是 garmin.cn
            # is_cn=None 时 domain 参数必须为 'cn' 或 'com'，库内部会自动判断
            # 如果明确是大陆账号，可以设置 is_cn=True
            is_cn = (self.domain == 'cn')
            logger.info(f"Logging in to Garmin {'CN' if is_cn else 'global'} region...")
            
            # 创建 Garmin 对象，并使用 token 存储
            token_store_path = os.path.expanduser("~/.garminconnect")
            # 新版库在初始化时就会尝试加载 ~/.garminconnect 下的 token
            self.client = Garmin(
                email=self.email,
                password=self.password,
                is_cn=is_cn,
                session_data=None  # 不显式提供，让库自动处理持久化
            )
            
            # 调用 login()，它会自动处理网络请求、token 刷新等
            # 如果之前有有效 token，会自动复用；否则会使用账号密码全新登录
            self.client.login()
            
            # 可选：验证登录是否成功
            self.client.get_full_name()
            logger.info("Successfully authenticated with Garmin Connect")
            self.authenticated = True
            return True

        except Exception as e:
            logger.error(f"Error authenticating with Garmin Connect: {e}")
            self.authenticated = False
            self.client = None
            return False

    def get_activities(self, start_date: Optional[datetime.datetime] = None, limit: int = 10) -> List[Dict]:
        """Get recent activities from Garmin Connect."""
        if not self.authenticated and not self.authenticate():
            logger.error("Cannot get activities: Not authenticated with Garmin")
            return []

        try:
            # get_activities 返回一个 list，默认按时间倒序（最新的在前）
            activities = self.client.get_activities(start=0, limit=limit)
            if isinstance(activities, list):
                return activities
            else:
                logger.warning(f"Unexpected type for activities: {type(activities)}")
                return []
        except Exception as e:
            logger.error(f"Error getting activities from Garmin Connect: {e}")
            # 尝试重新认证
            self.authenticated = False
            if self.authenticate(force=True):
                return self.get_activities(start_date, limit)
            return []

    def upload_fit(self, fit_data: bytes, activity_name: str = None) -> Optional[Dict]:
        """
        Upload a FIT file to Garmin Connect with retry mechanism.

        Args:
            fit_data: The binary FIT file data
            activity_name: Optional name for the activity (not directly used by upload)

        Returns:
            Dict with upload response or None if all attempts failed
        """
        if not self.authenticated and not self.authenticate():
            logger.error("Cannot upload activity: Not authenticated with Garmin")
            return None

        retries = 0
        last_error = None
        tmp_path = None

        while retries <= self.max_retries:
            try:
                if retries > 0:
                    delay = (self.retry_delay * (2 ** (retries - 1))) + random.uniform(0, 2)
                    logger.info(
                        f"Retrying upload (attempt {retries}/{self.max_retries}) after {delay:.2f}s delay..."
                    )
                    time.sleep(delay)

                # 将二进制数据写入临时 .fit 文件
                with tempfile.NamedTemporaryFile(suffix=".fit", delete=False) as tmp_file:
                    tmp_file.write(fit_data)
                    tmp_path = tmp_file.name

                # 上传文件
                # upload_activity 返回内容: 如果成功通常包含 uploadId 等
                upload_response = self.client.upload_activity(tmp_path)
                
                # 可选：如果提供了活动名称，尝试修改
                if activity_name and upload_response and "activityId" in upload_response:
                    try:
                        self.client.set_activity_description(upload_response["activityId"], activity_name)
                        logger.info(f"Renamed activity to '{activity_name}'")
                    except Exception as e:
                        logger.warning(f"Failed to rename activity: {e}")

                # 清理临时文件
                if tmp_path and os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                    
                logger.info(f"Successfully uploaded activity to Garmin Connect")
                return upload_response

            except Exception as e:
                last_error = e
                retries += 1
                logger.warning(
                    f"Upload attempt {retries} failed: {activity_name or 'Unknown Activity'}, {len(fit_data)} bytes, {e}"
                )

                # 认证相关错误 -> 强制重新登录
                error_str = str(e).lower()
                if "auth" in error_str or "login" in error_str or "token" in error_str:
                    logger.info("Authentication issue detected. Re-authenticating...")
                    self.authenticated = False
                    self.authenticate(force=True)

                # 速率限制 -> 额外延迟
                if "429" in error_str or "rate" in error_str:
                    extra_delay = 30 + random.uniform(0, 10)
                    logger.warning(f"Rate limiting detected. Adding {extra_delay:.2f}s delay...")
                    time.sleep(extra_delay)

                # 409 Conflict 表示活动可能已存在（重复上传），跳过此活动
                if "409" in error_str or "conflict" in error_str:
                    logger.warning("409 Conflict detected. Skipping activity.")
                    if tmp_path and os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                    return None

                # 清理临时文件
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        os.unlink(tmp_path)
                    except Exception:
                        pass
                    tmp_path = None

        # 所有重试尝试失败
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

        logger.error(f"Failed to upload after {self.max_retries} attempts. Last error: {last_error}")
        return None


def load_last_sync_date() -> datetime.datetime:
    """Load the last sync date from the JSON file."""
    try:
        if os.path.exists(LAST_SYNC_FILE):
            with open(LAST_SYNC_FILE, "r") as f:
                data = json.load(f)
                return datetime.datetime.fromisoformat(data["last_sync_date"])
        else:
            # 默认同步最近 30 天的数据
            return datetime.datetime.now() - datetime.timedelta(days=30)
    except Exception as e:
        logger.error(f"Error loading last sync date: {e}")
        # 出错时默认30天
        return datetime.datetime.now() - datetime.timedelta(days=30)


def save_last_sync_date(sync_date: datetime.datetime) -> None:
    """Save the last sync date to the JSON file."""
    try:
        with open(LAST_SYNC_FILE, "w") as f:
            json.dump({"last_sync_date": sync_date.isoformat()}, f)
    except Exception as e:
        logger.error(f"Error saving last sync date: {e}")


def activities_overlap(
    start_time1: datetime.datetime,
    duration1: int,
    start_time2: datetime.datetime,
    duration2: int,
) -> bool:
    """Check if two activities overlap in time (including a buffer)."""
    end_time1 = start_time1 + datetime.timedelta(seconds=duration1)
    end_time2 = start_time2 + datetime.timedelta(seconds=duration2)

    buffer = datetime.timedelta(minutes=OVERLAP_BUFFER_MINUTES)

    # 检查两个时间段是否有重叠（带缓冲）
    return (
        (start_time1 - buffer <= start_time2 <= end_time1 + buffer)
        or (start_time1 - buffer <= end_time2 <= end_time1 + buffer)
        or (start_time2 - buffer <= start_time1 <= end_time2 + buffer)
        or (start_time2 - buffer <= end_time1 <= end_time2 + buffer)
    )


def collect_activities_to_sync(
    igpsport_client: IGPSportClient,
    garmin_client: GarminClient,
    last_sync_date: datetime.datetime,
) -> List[Dict]:
    """Collect and filter activities to sync."""
    # 获取最近的一些 Garmin 活动用于去重判断
    garmin_activities = garmin_client.get_activities(limit=20)
    garmin_activity_times = []
    for activity in garmin_activities:
        try:
            start_time = parse(activity.get("startTimeLocal", ""))
            duration = activity.get("duration", 0)
            garmin_activity_times.append((start_time, duration))
        except Exception as e:
            logger.warning(f"Error parsing Garmin activity time: {e}")

    # 获取 iGPSport 的活动列表
    page_no = 1
    page_size = 20
    activities_data = igpsport_client.get_activities(page_no, page_size)

    if not activities_data or "rows" not in activities_data:
        logger.error("Failed to get activities from iGPSport")
        return []

    activities = activities_data["rows"]
    activities_to_sync = []

    for activity in activities:
        try:
            # 分析 iGPSport 活动的时间
            start_time_str = activity.get("startTime", "")
            activity_id = activity.get("rideId")
            # 处理类似 "2024.11.20" 的非标准格式
            if "." in start_time_str:
                parts = start_time_str.split(".")
                if len(parts) == 3:
                    year, month, day = parts
                    start_time = datetime.datetime(int(year), int(month), int(day))
                else:
                    logger.warning(f"Invalid date format: {start_time_str}")
                    continue
            else:
                start_time = parse(start_time_str)

            # 只同步上次同步日期之后的活动
            if start_time.date() < last_sync_date.date():
                logger.info(
                    f"Skipping activity {activity_id} from {start_time} (older than last sync)"
                )
                continue

            # 获取活动详情（包含更精准的时间、时长、fit文件链接）
            activity_detail = igpsport_client.get_activity_detail(activity_id)
            if not activity_detail:
                logger.warning(f"Could not get details for activity {activity_id}")
                continue

            detail_start_time = parse(activity_detail.get("startTime", ""))
            detail_duration = activity_detail.get("totalTime", 0)

            # 检查是否与已有 Garmin 活动时间重叠
            overlaps = False
            for garmin_start, garmin_duration in garmin_activity_times:
                if activities_overlap(
                    detail_start_time, detail_duration, garmin_start, garmin_duration
                ):
                    logger.info(
                        f"Skipping activity {activity_id} due to time overlap with existing Garmin activity"
                    )
                    overlaps = True
                    break

            if overlaps:
                continue

            # 获取 FIT 文件的 URL
            fit_url = activity_detail.get("fitUrl")
            if not fit_url:
                fit_url = activity.get("fitOssPath")

            if not fit_url:
                logger.warning(f"No FIT file URL for activity {activity_id}")
                continue

            activities_to_sync.append(
                {
                    "activity_id": activity_id,
                    "fit_url": fit_url,
                    "start_time": detail_start_time,
                    "duration": detail_duration,
                }
            )

        except Exception as e:
            logger.error(f"Error processing activity: {e}")

    return activities_to_sync


def main():
    """Main execution function."""
    # 读取环境变量
    igpsport_username = os.environ.get("IGPSPORT_USERNAME")
    igpsport_password = os.environ.get("IGPSPORT_PASSWORD")
    igpsport_region = os.environ.get("IGPSPORT_REGION")
    garmin_email = os.environ.get("GARMIN_EMAIL")
    garmin_password = os.environ.get("GARMIN_PASSWORD")
    garmin_domain = os.environ.get("GARMIN_DOMAIN") or "com"

    if not all(
        [
            igpsport_username,
            igpsport_password,
            garmin_email,
            garmin_password,
        ]
    ):
        logger.error("Missing required environment variables")
        return

    # 初始化客户端
    igpsport_client = IGPSportClient(
        igpsport_username, igpsport_password, igpsport_region
    )
    garmin_client = GarminClient(garmin_email, garmin_password, garmin_domain)

    # 登录 iGPSport
    if not igpsport_client.login():
        logger.error("Failed to authenticate with iGPSport")
        return

    # 上次同步日期
    last_sync_date = load_last_sync_date()
    logger.info(f"Last sync date: {last_sync_date}")

    # 筛选需要同步的活动（可能触发 Garmin 认证）
    activities_to_sync = collect_activities_to_sync(
        igpsport_client, garmin_client, last_sync_date
    )

    if not activities_to_sync:
        logger.info("No new activities to sync")
        return

    logger.info(f"Found {len(activities_to_sync)} activities to sync")

    # 确保 Garmin 已认证
    if not garmin_client.authenticate():
        logger.error("Failed to authenticate with Garmin")
        return

    sync_count = 0
    latest_synced_date = None

    # 逐一上传
    for activity_info in activities_to_sync:
        activity_id = activity_info["activity_id"]
        fit_url = activity_info["fit_url"]
        start_time = activity_info["start_time"]

        # 下载 FIT 文件
        fit_data = igpsport_client.download_fit_file(fit_url)
        if not fit_data:
            logger.warning(f"Failed to download FIT file for activity {activity_id}")
            continue

        # 上传到 Garmin
        result = garmin_client.upload_fit(fit_data)
        if result:
            logger.info(f"Successfully uploaded activity {activity_id} to Garmin")
            sync_count += 1

            # 更新最新的同步日期
            if latest_synced_date is None or start_time > latest_synced_date:
                latest_synced_date = start_time

            # 避免请求过频
            time.sleep(2)
        else:
            logger.warning(
                f"Failed to upload activity {activity_id} to Garmin after all retry attempts"
            )

        # 每次成功上传后都保存进度，避免中断后全部重新上传
        if latest_synced_date and sync_count > 0:
            save_last_sync_date(latest_synced_date)

    # 最终更新
    if latest_synced_date and sync_count > 0:
        save_last_sync_date(latest_synced_date)
        logger.info(f"Updated last sync date to: {latest_synced_date}")
    else:
        logger.info("No activities were synced, last sync date remains unchanged")

    logger.info(
        f"Sync completed: {sync_count} activities uploaded, {len(activities_to_sync) - sync_count} activities failed"
    )


if __name__ == "__main__":
    main()
