import snowflake.connector
from snowflake.snowpark import Session
from snowflake.sqlalchemy import URL
import streamlit as st
import openai
import pandas as pd

from file_uploader import FileUploader
from JSON_uploader import JSONFileUploader
from transformationer import DataTransformer
from snowflake_uploader import SnowflakeUploader

def display_text_as_large(text):
    ''' Define the HTML structure with style '''
    large_text_html = f"""
        <style>
        .big-font {{
            font-size:30px !important;
        }}
        </style>
        <div class='big-font'>
            {text}
        </div>
        """
    st.markdown(large_text_html, unsafe_allow_html=True)


# Establish Snowflake session
@st.cache_resource
def create_session():
    return Session.builder.configs(st.secrets.connections.snowpark).create()


def main():
    st.title("Data Ingestion Tool")
    
    # title for prompting Parquet file 
    display_text_as_large("1. Upload your dataset for processing")

    myFile = FileUploader()
    label = "Upload a Parquet file"
    uploaded_file = myFile.upload_file(label, "parquet")
    df_Parquet = None  # Initialize to None

    # If a file was uploaded, read the Parquet file
    if uploaded_file:
        myFile.read_parquet(uploaded_file)
        df_Parquet = myFile.get_dataframe()
        if df_Parquet is not None:
            st.dataframe(df_Parquet)  # Display the first few rows of the DataFrame

    # title for prompting json file 
    display_text_as_large("2. Upload the transformation you want to apply")
    
    json_uploader = JSONFileUploader()
    json_uploader.upload_json_file()
    json_rule = json_uploader.get_json_data()  # Initialize to None

    if json_rule is not None:
        json_uploader.display_dataframe()

    # Initialize transformer to None
    transformer = None
    transformed_df = None

    # Check if both files are uploaded and create the transformer object
    if df_Parquet is not None and json_rule is not None:
        transformer = DataTransformer(df_Parquet, json_uploader.get_dataframe())

    # Button to apply transformations
    display_text_as_large("3. Apply Transformation")
    if st.button("Apply Transformations"):
        if transformer:  # Check if transformer is not None
            transformer.apply_transformations()
            transformed_df = transformer.get_transformed_dataframe()
            if transformed_df is not None:
                st.dataframe(transformed_df)
        else:
            st.error("Please upload both Parquet and JSON files before applying transformations.")

    # title for prompting file upload to snowflake 
    display_text_as_large("4. Export Data to Snowflake SQL Database")

    if transformed_df is not None:
        connection_params = {
            "user": st.secrets["connections"]["snowpark"]["user"],
            "password": st.secrets["connections"]["snowpark"]["password"],
            "account": st.secrets["connections"]["snowpark"]["account"],
            "warehouse": st.secrets["connections"]["snowpark"]["warehouse"],
            "database": st.secrets["connections"]["snowpark"]["database"],
            "schema": st.secrets["connections"]["snowpark"]["schema"],
            "role": st.secrets["connections"]["snowpark"]["role"],
        }

        # initialize the snowflake_uploader
        snowflake_uploader = SnowflakeUploader(connection_params, transformer.get_transformed_dataframe())
        
        # Display the connection success message
        session = create_session()
        st.success("Connected to Snowflake!")

        snowflake_uploader.upload_dataframe()

main()



    
