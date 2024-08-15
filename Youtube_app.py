# Streamlit Youtube app for Youtube Data Harvesting and Warehousing 

import streamlit as st
import mysql.connector
import pandas as pd
import Harvest
import Warehouse
import toml

#Title of the app
st.title("YouTube Channel Data Harvesting and Warehousing")
channel_id = st.text_input("Enter YouTube Channel ID:")

def extract_insert_data_st():
    if st.button("Extract Data and Store in Database"):
            channel_data, video_df, comment_df = Harvest.extract_data(channel_id)
            Warehouse.insert_data(channel_data, video_df, comment_df)
            st.success("Data extraction and storage successful!")

extract_insert_data_st()
