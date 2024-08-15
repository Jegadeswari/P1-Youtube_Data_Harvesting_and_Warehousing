import streamlit as st
import mysql.connector
import pandas as pd
import Harvest
import Warehouse
import toml

# Connecting to Database
# host = st.secrets["mysql"]["host"]
user = st.secrets["credentials"]["db_user"]
password = st.secrets["credentials"]["db_password"]
# database = st.secrets["mysql"]["database"]

mydb = mysql.connector.connect( user = user, password = password)

#Title of the app
st.title("YouTube Channel Data Harvesting and Data Warehousing")

# Extracting the Count of Channel, Videos and Comments for display that in app screen
channel_count = pd.read_sql_query("SELECT COUNT(Channel_ID) AS channel_count FROM channels",mydb)
video_count = pd.read_sql_query("SELECT COUNT(Video_ID) AS video_count FROM videos",mydb)
comment_count = pd.read_sql_query("SELECT COUNT(Comment_ID) AS comment_count FROM comments",mydb)


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

# Here the Harvest and Warehouse file was called to get the data from channel and stored in the Mysql DB
def extract_insert_data_st():
    if st.button("Extract Data and Store in Database",type='primary'):
            channel_data, video_df, comment_df = Harvest.extract_data(channel_id)
            Warehouse.insert_data(channel_data, video_df, comment_df)
            st.success("Data extraction and storage successful!")

extract_insert_data_st()

# Query section
query_options = [
    "What are the names of all the videos and their corresponding channels?",
    "Which channels have the most number of videos, and how many videos do they have?",
    "What are the top 10 most viewed videos and their respective channels?",
    "How many comments were made on each video, and what are their corresponding video names?",
    "Which videos have the highest number of likes, and what are their corresponding channel names?",
    "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "What is the total number of views for each channel, and what are their corresponding channel names?",
    "What are the names of all the channels that have published videos in the year 2022?",
    "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "Which videos have the highest number of comments, and what are their corresponding channel names?"
    ]
selected_query = st.selectbox("Select Question:", query_options)




if st.button("Execute", type='secondary'):
    
    if selected_query == query_options[0]:
        query_result = pd.read_sql_query("SELECT videos.Video_name, channels.Channel_Name FROM videos INNER JOIN channels ON videos.Channel_ID = channels.Channel_ID", mydb)
    elif selected_query == query_options[1]:
        query_result = pd.read_sql_query("SELECT channels.Channel_Name, COUNT(Video_ID) AS Num_Videos FROM channels INNER JOIN videos ON channels.Channel_ID = videos.Channel_ID GROUP BY Channel_Name ORDER BY Num_Videos DESC LIMIT 1", mydb)
    elif selected_query == query_options[2]:
        query_result = pd.read_sql_query("SELECT videos.Video_name, channels.Channel_Name FROM videos INNER JOIN channels ON videos.Channel_ID = channels.Channel_ID ORDER BY View_count DESC LIMIT 10;", mydb)
    elif selected_query == query_options[3]:
        query_result = pd.read_sql_query("SELECT Video_Name, COUNT(Comment_ID) AS Number_of_Comments FROM videos INNER JOIN comments ON videos.Video_ID = comments.Video_ID GROUP BY Video_name ORDER BY Number_of_Comments DESC LIMIT 10;", mydb)
    elif selected_query == query_options[4]:
        query_result = pd.read_sql_query("SELECT videos.Video_name, videos.Channel_Name FROM videos INNER JOIN channels ON videos.channel_ID = channels.channel_ID ORDER BY Like_count DESC LIMIT 1", mydb)
    elif selected_query == query_options[5]:
        query_result = pd.read_sql_query("SELECT videos.Video_name, SUM(Like_count) AS Total_Likes FROM videos GROUP BY Video_name", mydb)
    elif selected_query == query_options[6]:
        query_result = pd.read_sql_query("SELECT channels.Channel_Name, SUM(channels.Channel_views) AS Total_Views FROM channels INNER JOIN videos ON channels.Channel_ID = videos.Channel_ID GROUP BY Channel_Name", mydb)
    elif selected_query == query_options[7]:
        query_result = pd.read_sql_query("SELECT channels.Channel_Name FROM channels INNER JOIN videos ON channels.Channel_ID = videos.Channel_ID WHERE SUBSTRING(videos.Published_date, 1, 4) = '2022' GROUP BY Channel_Name", mydb)
    elif selected_query == query_options[8]:
        query_result = pd.read_sql_query("SELECT channels.Channel_Name, AVG(videos.duration) AS Average_Duration FROM channels INNER JOIN videos ON videos.Channel_ID = channels.Channel_ID GROUP BY Channel_Name", mydb)
    elif selected_query == query_options[9]:
        query_result = pd.read_sql_query("SELECT videos.Video_name, videos.Channel_Name FROM videos INNER JOIN channels ON videos.Channel_ID = channels.Channel_ID ORDER BY Comment_count DESC LIMIT 1", mydb)
    mydb.close()

    st.dataframe(query_result)
