# Data Warehousing using MySQL

import mysql.connector
from mysql.connector import Error
import streamlit as st
import toml
import Harvest

def insert_data(channel_data, video_data, comment_data):
    try:
        print(">>>>> Storing the data in MySQL Warehouse <<<<<")
        
       # Connecting to Database
        host = st.secrets["credentials"]["host"]
        user = st.secrets["credentials"]["user"]
        password = st.secrets["credentials"]["password"]
        database = st.secrets["credentials"]["database"]
        
        mydb = mysql.connector.connect(host=host, user=user, password=password, database=database)
        #mydb = mysql.connector.connect(host="localhost", user="root", password="123456", database="youtube_test")
              
        if mydb.is_connected():
            print("Succesfully connected to MySQL database .....")
            mycur = mydb.cursor(buffered=True)

            # Create database 
            mycur.execute('CREATE DATABASE IF NOT EXISTS youtube_test')
            mycur.execute('use youtube_test')
    
            # Create Tables : Channel, Playlist, Video and Content
            mycur.execute('''
                CREATE TABLE IF NOT EXISTS channels(
                    channel_id VARCHAR(100) NOT NULL PRIMARY KEY,
                    channel_name VARCHAR(255),
                    channel_subscribers INT,
                    channel_views INT,
                    channel_video_count INT,
                    channel_description TEXT,
                    channel_status VARCHAR(255))
            ''')
    
            mycur.execute('''
                CREATE TABLE IF NOT EXISTS videos(
                    channel_id VARCHAR(100),
                    channel_name VARCHAR(255),
                    video_id VARCHAR(255) NOT NULL PRIMARY KEY,
                    playlist_id VARCHAR(100),
                    video_name VARCHAR(255),
                    video_description TEXT,
                    tags VARCHAR(1000),
                    published_date DATETIME,
                    view_count INT,
                    like_count INT,
                    dislike_count INT,
                    favorite_count INT,
                    comment_count INT,
                    duration INT,
                    thumbnail VARCHAR(255),
                    caption_status VARCHAR(255))
            ''')
            
            mycur.execute('''
                create table if not exists comments(
                    channel_id VARCHAR(100),
                    video_id VARCHAR(255),
                    comment_id VARCHAR(255) NOT NULL PRIMARY KEY,
                    comment_text TEXT,
                    comment_author VARCHAR(255),
                    comment_published_date DATETIME)
            ''')
            
            # Insert channel data
            print("Inserting channel data ....")
            mycur.execute('''
                INSERT INTO channels (channel_id, channel_name, channel_subscribers, channel_views, channel_video_count, channel_description, channel_status) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (channel_data['channel_id'], channel_data['channel_name'], channel_data['subscribers'], channel_data['views'], channel_data['video_count'], channel_data['channel_description'], channel_data['channel_status']))
            mydb.commit()
                     
            #Insert video data
            print("Inserting video data .....")
            for _, row in video_data.iterrows():
                video_datetime_str = row['published_date'].replace('T', ' ').replace('Z', '')
                mycur.execute('''
                    INSERT INTO videos (channel_id, channel_name, video_id, playlist_id, video_name, video_description, tags, published_date, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (channel_data['channel_id'], channel_data['channel_name'], row['video_id'], channel_data['playlist_id'], row['video_name'], row['video_description'], ','.join(row['tags']), video_datetime_str, row['views_count'], row['likes_count'], row['dislikes_count'], row['favorites_count'], row['comments_count'], row['duration'], row['thumbnail'], row['caption_status']))
                mydb.commit()

            # Insert comment data
            print("Inserting comment data .....")
            for _, row in comment_data.iterrows():
                cmt_datetime_str = row['comment_published'].replace('T', ' ').replace('Z', '')
                mycur.execute('''
                    INSERT INTO comments (channel_id, video_id, comment_id, comment_text, comment_author, comment_published_date) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                ''', (channel_data['channel_id'], row['video_id'], row['comment_id'], row['comment_text'], row['comment_author'], cmt_datetime_str))
                mydb.commit()

            print("Data inserted successfully .....")
            
    except Error as e:
        print(f"Error: {e}")
    
    finally:
        if mydb.is_connected():
            mycur.close()
            mydb.close()
            print("MySQL connection is closed .....")
            
#ch, video, comment = Harvest.extract_data("UC9Q4rw4dkey9lhK5FnYuigg") 

#insert_data(ch, video, comment)