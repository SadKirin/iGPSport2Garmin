#!/usr/bin/env python3
"""
iGPSport to Garmin Connect Sync Script
- Syncs activities from iGPSport to Garmin Connect
- Handles Chinese region (CN) accounts properly
- Includes duplicate activity prevention
- Improved error handling and logging
"""

import json
import os
import time
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

import requests
from dateutil import parser as date_parser
from garminconnect import Garmin, GarminConnectAuthenticationError, GarminConnectConnectionError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class iGPSportClient:
    """Client for interacting with iGPSport API"""
    
    BASE_URL = "https://my.igpsport.com"
    
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.last_sync_date = None
        self.user_id = None
        
    def login(self) -> bool:
        """Login to iGPSport account"""
        try:
            login_url = f"{self.BASE_URL}/Account/Login"
            payload = {
                'Email': self.username,
                'Password': self.password,
                'RememberMe': 'true'
            }
            
            response = self.session.post(login_url, data=payload, timeout=30)
            response.raise_for_status()
            
            if "Login" not in response.url and "Home" in response.url:
                logger.info("Successfully logged in to iGPSport")
                self._get_user_info()
                return True
            else:
                logger.error("Failed to login to iGPSport")
                return False
                
        except Exception as e:
            logger.error(f"iGPSport login error: {e}")
            return False
    
    def _get_user_info(self):
        """Get user information including user ID"""
        try:
            user_info_url = f"{self.BASE_URL}/Home/GetUserInfo"
            response = self.session.get(user_info_url, timeout=30)
            response.raise_for_status()
            user_data = response.json()
            self.user_id = user_data.get('UserId')
            logger.info(f"Got user ID: {self.user_id}")
        except Exception as e:
            logger.error(f"Failed to get user info: {e}")
    
    def get_last_sync_date(self) -> datetime:
        """Get the last sync date from local storage or default"""
        sync_file = "last_sync.txt"
        try:
            if os.path.exists(sync_file):
                with open(sync_file, 'r') as f:
                    date_str = f.read().strip()
                    return date_parser.parse(date_str)
        except Exception as e:
            logger.warning(f"Error reading last sync date: {e}")
        
        # Default to 30 days ago if no sync file exists
        default_date = datetime.now() - timedelta(days=30)
        logger.info(f"Using default last sync date: {default_date}")
        return default_date
    
    def save_last_sync_date(self, sync_date: datetime):
        """Save the last sync date to local storage"""
        sync_file = "last_sync.txt"
        try:
            with open(sync_file, 'w') as f:
                f.write(sync_date.isoformat())
            logger.info(f"Saved last sync date: {sync_date}")
        except Exception as e:
            logger.error(f"Error saving last sync date: {e}")
    
    def get_activities_since(self, since_date: datetime) -> List[Dict]:
        """Get activities from iGPSport since the specified date"""
        try:
            activities_url = f"{self.BASE_URL}/Activity/ActivityList"
            params = {
                'page': 1,
                'rows': 100,
                'sidx': 'StartTime',
                'sord': 'desc'
            }
            
            response = self.session.get(activities_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            activities = []
            for row in data.get('rows', []):
                start_time = date_parser.parse(row['cell'][3])
                if start_time > since_date:
                    activity = {
                        'id': row['id'],
                        'name': row['cell'][1],
                        'type': row['cell'][2],
                        'start_time': start_time,
                        'distance': float(row['cell'][4]),
                        'duration': row['cell'][5]
                    }
                    activities.append(activity)
            
            logger.info(f"Found {len(activities)} activities since {since_date}")
            return activities
            
        except Exception as e:
            logger.error(f"Error getting activities: {e}")
            return []
    
    def get_activity_file(self, activity_id: str) -> Optional[bytes]:
        """Download activity file (GPX format) from iGPSport"""
        try:
            download_url = f"{self.BASE_URL}/Activity/DownloadOriginalTrack"
            params = {
                'id': activity_id,
                'fileType': '1'  # GPX format
            }
            
            response = self.session.get(download_url, params=params, timeout=60)
            response.raise_for_status()
            
            if response.content and len(response.content) > 0:
                logger.info(f"Downloaded activity file for ID: {activity_id}")
                return response.content
            
            logger.warning(f"Empty activity file for ID: {activity_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error downloading activity {activity_id}: {e}")
            return None

class GarminClient:
    """Client for interacting with Garmin Connect API"""
    
    def __init__(self, email: str, password: str, domain: str = "com"):
        self.email = email
        self.password = password
        self.domain = domain.lower()
        self.client = None
        self.authenticated = False
        self.last_auth_time = None
        self.is_cn = (self.domain == 'cn')
        
    def authenticate(self, force: bool = False) -> bool:
        """Authenticate with Garmin Connect with retry mechanism"""
        try:
            # Check if we need to re-authenticate
            if not force and self.authenticated and self.last_auth_time:
                elapsed = (datetime.now() - self.last_auth_time).total_seconds()
                if elapsed < 3600:  # 1 hour cache
                    logger.debug("Using cached Garmin authentication")
                    return True
            
            logger.info(f"Logging in to Garmin {'CN' if self.is_cn else 'global'} region...")
            
            # Create Garmin client - v0.3.3+ doesn't need use_webservice parameter
            self.client = Garmin(
                email=self.email,
                password=self.password,
                is_cn=self.is_cn
            )
            
            # Attempt login with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.client.login()
                    self.client.get_full_name()  # Verify login
                    logger.info("Successfully authenticated with Garmin Connect")
                    self.authenticated = True
                    self.last_auth_time = datetime.now()
                    return True
                except Exception as e:
                    logger.warning(f"Authentication attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(2 * (attempt + 1))  # Exponential backoff
                        continue
                    raise
            
        except Exception as e:
            logger.error(f"Authentication failed after {max_retries} attempts: {e}")
            self.authenticated = False
            self.client = None
            return False
    
    def activity_exists(self, activity_name: str, start_time: datetime) -> bool:
        """
        Check if an activity already exists in Garmin Connect
        Uses activity name and start time to determine duplicates
        """
        try:
            # Get recent activities (last 7 days should be enough for duplicate checking)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            activities = self.client.get_activities_by_date(
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d")
            )
            
            # Normalize the activity name for comparison
            normalized_name = activity_name.strip().lower()
            
            for activity in activities:
                # Check if activity name matches (case-insensitive)
                garmin_name = activity.get('activityName', '').strip().lower()
                garmin_start_time = date_parser.parse(activity['startTimeLocal'])
                
                # Check if times are close (within 5 minutes)
                time_diff = abs((garmin_start_time - start_time).total_seconds())
                
                if normalized_name == garmin_name and time_diff < 300:  # 5 minutes
                    logger.info(f"Found duplicate activity: {activity_name} at {start_time}")
                    return True
            
            return False
            
        except Exception as e:
            logger.warning(f"Error checking for duplicate activities: {e}")
            # If we can't check, assume it doesn't exist to avoid missing syncs
            return False
    
    def upload_activity(self, activity_data: bytes, activity_name: str) -> Tuple[bool, str]:
        """Upload activity file to Garmin Connect"""
        try:
            # Try to upload as GPX
            response = self.client.upload_activity(activity_data, format="gpx")
            if response and response.get('status') == 'success':
                logger.info(f"Successfully uploaded activity: {activity_name}")
                return True, "success"
            
            # If GPX fails, try FIT format (some activities work better as FIT)
            logger.warning("GPX upload failed, trying FIT format...")
            response = self.client.upload_activity(activity_data, format="fit")
            if response and response.get('status') == 'success':
                logger.info(f"Successfully uploaded activity (FIT format): {activity_name}")
                return True, "success"
            
            error_msg = response.get('error', 'Unknown error') if response else 'No response'
            logger.error(f"Upload failed: {error_msg}")
            return False, error_msg
            
        except Exception as e:
            error_str = str(e)
            if "409" in error_str and "Duplicate Activity" in error_str:
                logger.warning(f"Duplicate activity detected: {activity_name}")
                return False, "duplicate"
            logger.error(f"Upload error: {e}")
            return False, str(e)

def sync_activities(igpsport: iGPSportClient, garmin: GarminClient) -> Dict[str, Any]:
    """Main sync function"""
    results = {
        'total': 0,
        'uploaded': 0,
        'skipped': 0,
        'failed': 0,
        'details': []
    }
    
    try:
        # Get last sync date
        last_sync = igpsport.get_last_sync_date()
        logger.info(f"Last sync date: {last_sync}")
        
        # Get activities from iGPSport
        activities = igpsport.get_activities_since(last_sync)
        if not activities:
            logger.info("No new activities to sync")
            return results
        
        results['total'] = len(activities)
        
        # Authenticate with Garmin
        if not garmin.authenticate():
            logger.error("Failed to authenticate with Garmin, aborting sync")
            results['failed'] = results['total']
            return results
        
        # Process each activity
        for i, activity in enumerate(activities, 1):
            activity_id = activity['id']
            activity_name = activity['name']
            start_time = activity['start_time']
            
            logger.info(f"Processing activity {i}/{len(activities)}: {activity_name} ({activity_id})")
            
            # Check for duplicates in Garmin
            if garmin.activity_exists(activity_name, start_time):
                logger.info(f"Skipping duplicate activity: {activity_name}")
                results['skipped'] += 1
                results['details'].append({
                    'id': activity_id,
                    'name': activity_name,
                    'status': 'skipped',
                    'reason': 'duplicate'
                })
                continue
            
            # Download activity file
            activity_file = igpsport.get_activity_file(activity_id)
            if not activity_file:
                logger.error(f"Failed to download activity file for {activity_id}")
                results['failed'] += 1
                results['details'].append({
                    'id': activity_id,
                    'name': activity_name,
                    'status': 'failed',
                    'reason': 'download_failed'
                })
                continue
            
            # Upload to Garmin
            success, reason = garmin.upload_activity(activity_file, activity_name)
            if success:
                results['uploaded'] += 1
                results['details'].append({
                    'id': activity_id,
                    'name': activity_name,
                    'status': 'uploaded',
                    'reason': 'success'
                })
                # Update last sync date after successful upload
                igpsport.save_last_sync_date(start_time)
            else:
                results['failed'] += 1
                results['details'].append({
                    'id': activity_id,
                    'name': activity_name,
                    'status': 'failed',
                    'reason': reason
                })
            
            # Rate limiting - wait between uploads
            if i < len(activities):
                time.sleep(2)
        
        logger.info(f"Sync completed: {results['uploaded']}/{results['total']} uploaded, "
                   f"{results['skipped']} skipped, {results['failed']} failed")
        
        return results
        
    except Exception as e:
        logger.exception(f"Unexpected error during sync: {e}")
        results['failed'] = results['total']
        return results

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Sync iGPSport activities to Garmin Connect')
    parser.add_argument('--igpsport-user', required=True, help='iGPSport username/email')
    parser.add_argument('--igpsport-pass', required=True, help='iGPSport password')
    parser.add_argument('--garmin-user', required=True, help='Garmin Connect email')
    parser.add_argument('--garmin-pass', required=True, help='Garmin Connect password')
    parser.add_argument('--garmin-domain', default='com', help='Garmin domain (com or cn)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    try:
        # Initialize clients
        igpsport = iGPSportClient(args.igpsport_user, args.igpsport_pass)
        garmin = GarminClient(args.garmin_user, args.garmin_pass, args.garmin_domain)
        
        # Login to iGPSport
        if not igpsport.login():
            logger.error("Failed to login to iGPSport, exiting")
            return
        
        # Run sync
        results = sync_activities(igpsport, garmin)
        
        # Print summary
        print("\n" + "="*50)
        print("SYNC SUMMARY")
        print("="*50)
        print(f"Total activities found: {results['total']}")
        print(f"Successfully uploaded: {results['uploaded']}")
        print(f"Skipped (duplicates): {results['skipped']}")
        print(f"Failed: {results['failed']}")
        print("="*50)
        
        if results['failed'] > 0:
            print("\nFAILED ACTIVITIES:")
            for detail in results['details']:
                if detail['status'] == 'failed':
                    print(f"  - {detail['name']} (ID: {detail['id']}): {detail['reason']}")
        
    except KeyboardInterrupt:
        logger.info("Sync interrupted by user")
    except Exception as e:
        logger.exception(f"Critical error: {e}")
    finally:
        logger.info("Sync process completed")

if __name__ == "__main__":
    main()
