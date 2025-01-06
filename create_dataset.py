import requests
from datetime import datetime, timedelta
import json
from typing import List, Dict, Any
import time
import os
import ssl
from tqdm import tqdm
import pandas as pd
import warnings
import urllib3

# Suppress specific warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


ssl._create_default_https_context = ssl._create_unverified_context

# Basic setup
base_url = "https://epaper.andhrajyothy.com"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}
delay = 2 # 0.1  # delay between requests in seconds

def get_all_pages(edition_id: int, date_str: str) -> List[Dict[str, int]]:
    """Get all pages for a specific edition and date."""
    url = f"{base_url}/Home/GetAllpages"
    params = {
        'editionid': edition_id,
        'editiondate': date_str
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, verify=False)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching pages for edition {edition_id} on {date_str}: {e}")
        return []

def get_stories_on_page(page_id: int) -> List[Dict[str, int]]:
    """Get all stories on a specific page."""
    url = f"{base_url}/Home/getStoriesOnPage"
    params = {'pageid': page_id}
    
    try:
        time.sleep(delay)
        response = requests.get(url, params=params, headers=headers, verify=False)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching stories for page {page_id}: {e}")
        return []

def get_story_detail(story_id: int) -> Dict[str, Any]:
    """Get details for a specific story."""
    url = f"{base_url}/Home/getstorydetail"
    params = {'Storyid': story_id}
    
    try:
        time.sleep(delay)
        response = requests.get(url, params=params, headers=headers, verify=False)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching story {story_id}: {e}")
        return {}

def format_date(date: datetime) -> str:
    """Format date as required by the API."""
    return date.strftime("%d/%m/%Y")

def process_edition(edition_id: int, date_str: str) -> List[Dict[str, Any]]:
    """Process a single edition and return all articles."""
    articles = []
    
    pages = get_all_pages(edition_id, date_str)
    if not pages:
        return articles
    
    for page in pages:
        page_id = page.get('PageId')
        if not page_id:
            continue
            
        stories = get_stories_on_page(page_id)
        for story in stories:
            story_id = story.get('storyid')
            if not story_id:
                continue
                
            story_detail = get_story_detail(story_id)
            if not story_detail or \
               len(story_detail.get("StoryContent", [])) == 0 or \
               story_detail["StoryContent"][0].get("Body", "") == "":
                continue
                
            article = {
                'date': date_str,
                'edition_id': edition_id,
                'page_id': page_id,
                'story_id': story_id,
                'content': story_detail
            }
            articles.append(article)
    
    return articles

def get_date_range(months: int = 3) -> List[datetime]:
    """Generate list of dates from today going back specified number of months."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30 * months)
    date_list = []
    current_date = start_date
    
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += timedelta(days=1)
    
    return date_list

# Create a folder for saving data
save_folder = 'andhrajyothy_dataset'
os.makedirs(save_folder, exist_ok=True)

# Get list of dates to process
dates_to_process = get_date_range(3)
all_articles = []

# Process each date
for current_date in tqdm(dates_to_process, desc="Processing dates"):
    date_str = format_date(current_date)
    daily_articles = []
    
    # Process editions 1 to 225 for each date
    for edition_id in range(1, 226):
        articles = process_edition(edition_id, date_str)
        if articles:
            daily_articles.extend(articles)
                
    # Save daily consolidated files if articles were found
    if daily_articles:
        all_articles.extend(daily_articles)
        
        # Save daily CSV and JSON files
        date_str_filename = date_str.replace('/', '_')
        
        # Save JSON
        json_filename = f"{save_folder}/articles_{date_str_filename}.json"
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(daily_articles, f, ensure_ascii=False, indent=2)

        # Save final consolidated files for all dates
        if all_articles:
            # Save consolidated CSV
            df_all = pd.json_normalize(all_articles)
            final_csv = f"{save_folder}/all_articles_3months.csv"
            df_all.to_csv(final_csv, index=False)
            
            # Save consolidated JSON
            final_json = f"{save_folder}/all_articles_3months.json"
            with open(final_json, 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, ensure_ascii=False, indent=2)
