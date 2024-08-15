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

# Title of the app
st.title("YouTube Channel Data Harvesting and Warehousing")

# Extract count of Channels, Videos and Comments to display on app screen
channel_count = pd.read_sql_query("SELECT COUNT(channel_id) AS channel_count FROM channels",mydb)
video_count = pd.read_sql_query("SELECT COUNT(video_id) AS video_count FROM videos",mydb)
comment_count = pd.read_sql_query("SELECT COUNT(comment_id) AS comment_count FROM comments",mydb)

count = st.markdown(
        """
        <div style="text-align: left">
            <h2 style="color: whitesmoke; font-family: Arial, Helvetica, sans-serif;">Channels</h2>
            <h1 style="color: crimson; font-family: 'Poppins', sans-serif; font-size: 60px; font-weight: bold; margin-top: -15px;margin-left: 50px">{ch_count}</h1>
        </div>

        <div style=" margin-top: -160px;text-align: center;">
            <h2 style="color: whitesmoke; font-family: Arial, Helvetica, sans-serif;">Videos</h2>
            <h1 style="color: crimson; font-family: 'Poppins', sans-serif; font-size: 60px; font-weight: bold; margin-top: -15px; margin-left: -5px">{v_count}</h1>
        </div>

         <div style=" margin-top: -165px;text-align: right;">
            <h2 style="color: whitesmoke; font-family: Arial, Helvetica, sans-serif;">Comments</h2>
            <h1 style="color: crimson; font-family: 'Poppins', sans-serif; font-size: 65px; font-weight: bold; margin-top: -15px; margin-left: 5px">{cm_count}</h1>
        </div>
        """.format(ch_count=channel_count.at[0, 'channel_count'],v_count=video_count.at[0, 'video_count'], cm_count = comment_count.at[0,'comment_count']), 
        unsafe_allow_html=True
    )

channel_id = st.text_input("Enter YouTube Channel ID:")

# Harvest: Extract data of the required Channel from Youtube
# Warehouse: Store the extracted channel data in the MySQL database
def extract_insert_data_st():
    if st.button("Extract Data and Store in Database"):
            channel_data, video_df, comment_df = Harvest.extract_data(channel_id)
            Warehouse.insert_data(channel_data, video_df, comment_df)
            st.success("Data extraction and storage successful !!!")

extract_insert_data_st()

# Query section
query_options = [
    "0 What are the names of all the videos and their corresponding channels?",
    "1 Which channels have the most number of videos, and how many videos do they have?",
    "2 What are the top 10 most viewed videos and their respective channels?",
    "3 How many comments were made on each video, and what are their corresponding video names?",
    "4 Which videos have the highest number of likes, and what are their corresponding channel names?",
    "5 What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "6 What is the total number of views for each channel, and what are their corresponding channel names?",
    "7 What are the names of all the channels that have published videos in the year 2022?",
    "8 What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "9 Which videos have the highest number of comments, and what are their corresponding channel names?"
    ]
selected_query = st.selectbox("Select Question:", query_options)

if st.button("Execute", type='secondary'):
    
    if selected_query == query_options[0]:
        query_result = pd.read_sql_query("SELECT videos.video_name, channels.channel_name FROM videos INNER JOIN channels ON videos.channel_id = channels.channel_id", mydb)
    elif selected_query == query_options[1]:
        query_result = pd.read_sql_query("SELECT channels.channel_name, COUNT(video_id) AS Num_Videos FROM channels INNER JOIN videos ON channels.channel_id = videos.channel_id GROUP BY channels.channel_name ORDER BY Num_Videos DESC LIMIT 1", mydb)
    elif selected_query == query_options[2]:
        query_result = pd.read_sql_query("SELECT videos.video_name, channels.channel_name FROM videos INNER JOIN channels ON videos.channel_id = channels.channel_id ORDER BY view_count DESC LIMIT 10", mydb)
    elif selected_query == query_options[3]:
        query_result = pd.read_sql_query("SELECT videos.video_name, COUNT(comment_id) AS Num_Comments FROM videos INNER JOIN comments ON videos.video_id = comments.video_id GROUP BY video_name ORDER BY Num_Comments DESC LIMIT 10", mydb)
    elif selected_query == query_options[4]:
        query_result = pd.read_sql_query("SELECT videos.video_name, channels.channel_name FROM videos INNER JOIN channels ON videos.channel_id = channels.channel_id ORDER BY like_count DESC LIMIT 1", mydb)
    elif selected_query == query_options[5]:
        query_result = pd.read_sql_query("SELECT videos.video_name, channels.channel_name, SUM(like_count) AS Total_Likes, SUM(dislike_count) AS Total_Dislikes FROM videos GROUP BY video_name", mydb)
    elif selected_query == query_options[6]:
        query_result = pd.read_sql_query("SELECT channels.channel_name, SUM(channel_views) AS Total_Views FROM channels INNER JOIN videos ON channels.channel_id = videos.channel_id GROUP BY channel_name", mydb)
    elif selected_query == query_options[7]:
        query_result = pd.read_sql_query("SELECT channels.channel_name FROM channels INNER JOIN videos ON channels.channel_id = videos.channel_id WHERE SUBSTRING(videos.published_date, 1, 4) = '2022' GROUP BY channel_name", mydb)
    elif selected_query == query_options[8]:
        query_result = pd.read_sql_query("SELECT channels.channel_name, AVG(videos.duration) AS Average_Duration FROM channels INNER JOIN videos ON channels.channel_id = videos.channel_id GROUP BY channel_name", mydb)
    elif selected_query == query_options[9]:
        query_result = pd.read_sql_query("SELECT videos.video_name, channels.channel_name FROM videos INNER JOIN channels ON videos.channel_id = channels.channel_id ORDER BY comment_count DESC LIMIT 1", mydb)
    mydb.close()

    st.dataframe(query_result)