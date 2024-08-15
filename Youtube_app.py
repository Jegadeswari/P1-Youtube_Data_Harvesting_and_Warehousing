# Streamlit Youtube app for Youtube Data Harvesting and Warehousing 

import streamlit as st
import mysql.connector
import pandas as pd
import Harvest
import Warehouse
import toml

# Connecting to Database

host = st.secrets["credentials"]["host"]
user = st.secrets["credentials"]["user"]
password = st.secrets["credentials"]["password"]
database = st.secrets["credentials"]["database"]
        
mydb = mysql.connector.connect(host=host, user=user, password=password, database=database)

#Title of the app
st.title("YouTube Channel Data Harvesting and Warehousing")
channel_id = st.text_input("Enter YouTube Channel ID:")

# Extracting the Count of Channel, Videos and Comments for display that in app screen
channel_count = pd.read_sql_query("SELECT COUNT(channel_id) AS channel_count FROM channels",mydb)
video_count = pd.read_sql_query("SELECT COUNT(video_id) AS video_count FROM videos",mydb)
comment_count = pd.read_sql_query("SELECT COUNT(comment_id) AS comment_count FROM comments",mydb)

def extract_insert_data_st():
    if st.button("Extract Data and Store in Database"):
            channel_data, video_df, comment_df = Harvest.extract_data(channel_id)
            Warehouse.insert_data(channel_data, video_df, comment_df)
            st.success("Data extraction and storage successful!")

extract_insert_data_st()