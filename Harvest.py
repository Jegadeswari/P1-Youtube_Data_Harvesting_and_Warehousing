# Harvest data from a Youtube channel using the Google API, Youtube Data API v3

import re
import pandas as pd
import mysql.connector
from googleapiclient.discovery import build
from mysql.connector import Error

# Set up YouTube Data API
api_key = "AIzaSyCK-ruPpxAt_3zFvccTngT8sPfJ2An4y1E"
youtube = build('youtube', 'v3', developerKey=api_key)

# Extract channel data for a specific channel id

def extract_channel_data(youtube, channel_id):
    print("Extracting channel data .....")
    response = youtube.channels().list(
        part = "snippet,statistics,contentDetails,status",
        id = channel_id
    ).execute()   
    item = response['items'][0]
        
    return {
        'channel_id': channel_id,
        'channel_name': item['snippet']['title'],
        'subscribers': item['statistics']['subscriberCount'],
        'views': item['statistics']['viewCount'],
        'channel_description': item['snippet']['description'],
        'channel_status': item['status']['privacyStatus'],
        'video_count': item['statistics']['videoCount'],            
        'playlist_id': item['contentDetails']['relatedPlaylists']['uploads']}
    
#Extract video_ids of the YouTube channel using the playlist_id
def extract_video_ids(youtube, playlist_id):
    print("Extracting video ids .....")
    video_ids = []
    next_page_token = None
    
    while True:
        response = youtube.playlistItems().list(
            part ="snippet",
            playlistId = playlist_id,
            maxResults = 50,
            pageToken = next_page_token
        ).execute()        
        
        for item in response['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])
        
        next_page_token = response.get('nextPageToken')    
        if not next_page_token:
            break
    return video_ids

# Extract all video details in the Youtube channel using video_ids
def extract_video_data(youtube, video_ids):
    print("Extracting video data .....")
    
    video_data = []
    
    for video_id in video_ids:
        response = youtube.videos().list(
            part = "snippet,ContentDetails,statistics",
            id = video_id
        ).execute()

        for item in response['items']:
            data = dict(
                channel_id = item['snippet']['channelId'],
                channel_name = item['snippet']['channelTitle'],
                video_id = item['id'],
                video_name = item['snippet']['title'],
                video_description = item['snippet'].get('description'),
                tags = item['snippet'].get('tags', []),
                published_date = item['snippet']['publishedAt'],
                views_count = item['statistics'].get('viewCount'),
                likes_count = item['statistics'].get('likeCount'),
                dislikes_count = item['statistics'].get('dislikeCount'),
                favorites_count = item['statistics']['favoriteCount'],
                comments_count = item['statistics'].get('commentCount'),
                duration = convert_to_mins(item['contentDetails']['duration']),
                thumbnail = item['snippet']['thumbnails']['default']['url'],
                caption_status = item['contentDetails']['caption']
                )
            video_data.append(data)
    return video_data

# Convert duration of the video to minutes
def convert_to_mins(time_string):
    hour_match = re.match(r'PT(?P<hours>\d+)H(?P<minutes>\d+)M(?P<seconds>\d+)S', time_string)
    hour_min_match = re.match(r'PT(?P<hours>\d+)H(?P<minutes>\d+)M', time_string)
    min_sec_match = re.match(r'PT(?P<minutes>\d+)M(?P<seconds>\d+)S', time_string)
    hour_sec_match = re.match(r'PT(?P<hours>\d+)H(?P<seconds>\d+)S', time_string)
    hour_only_match = re.match(r'PT(?P<hours>\d+)H', time_string)
    minute_match = re.match(r'PT(?P<minutes>\d+)M', time_string)
    sec_match = re.match(r'PT(?P<seconds>\d+)S', time_string)

    if hour_match:
        hours = int(hour_match.group('hours')) if hour_match.group('hours') else 0
        minutes = int(hour_match.group('minutes')) if hour_match.group('minutes') else 0
        seconds = int(hour_match.group('seconds')) if hour_match.group('seconds') else 0
        return hours * 60 + minutes + seconds / 60
    elif hour_min_match:
        hours = int(hour_min_match.group('hours')) if hour_min_match.group('hours') else 0
        minutes = int(hour_min_match.group('minutes')) if hour_min_match.group('minutes') else 0
        return hours * 60 + minutes
    elif min_sec_match:
        minutes = int(min_sec_match.group('minutes')) if min_sec_match.group('minutes') else 0
        seconds = int(min_sec_match.group('seconds')) if min_sec_match.group('seconds') else 0
        return minutes + seconds / 60
    elif hour_sec_match:
        hours = int(hour_sec_match.group('hours')) if hour_sec_match.group('hours') else 0
        seconds = int(hour_sec_match.group('seconds')) if hour_sec_match.group('seconds') else 0
        return hours * 60 + seconds / 60
    elif hour_only_match:
        hours = int(hour_only_match.group('hours')) if hour_only_match.group('hours') else 0
        return hours * 60
    elif minute_match:
        minutes = int(minute_match.group('minutes')) if minute_match.group('minutes') else 0
        return minutes
    elif sec_match:
        seconds = int(sec_match.group('seconds')) if sec_match.group('seconds') else 0
        return seconds / 60
    else:
      return 0
  
# Extract first 50 comments on each video in the Youtube channel using video_ids
def extract_comments(youtube, video_ids):
    print("Extracting comments data .....")
    comments_data=[]
    try:
        for video_id in video_ids:
            response = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50).execute()
            
            for item in response['items']:
                data = dict(
                    video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_id=item['snippet']['topLevelComment']['id'],
                    comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comments_data.append(data)  
    except:
        pass
    return comments_data

# Extract channel data, video data and comment data of the Youtube channel
def extract_data(id):
    print(" >>>>> Commencing data extraction ..... <<<<<")
    channel_id = id
    channel_data = extract_channel_data(youtube, channel_id)
    video_ids = extract_video_ids(youtube, channel_data['playlist_id'])
    video_data = extract_video_data(youtube, video_ids)
    comment_data = extract_comments(youtube, video_ids)
     
    video_df = pd.DataFrame(video_data)
    comment_df = pd.DataFrame(comment_data)
    
    return (channel_data, video_df, comment_df)

print(" >>>>> End of data extraction ..... <<<<<")