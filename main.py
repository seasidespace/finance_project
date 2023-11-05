import snowflake.connector
from snowflake.snowpark import Session
from snowflake.sqlalchemy import URL
import streamlit as st
import openai
import pandas as pd

from file_uploader import FileUploader
from JSON_uploader import JSONFileUploader
from transformation import DataTransformer


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
            st.dataframe(df_Parquet.head())  # Display the first few rows of the DataFrame

    # title for prompting json file 
    display_text_as_large("2. Upload the transformation you want to apply")
    
    json_uploader = JSONFileUploader()
    json_uploader.upload_json_file()
    json_rule = json_uploader.get_json_data()  # Initialize to None

    if json_rule is not None:
        json_uploader.display_dataframe()

    # Initialize transformer to None
    transformer = None

    # Check if both files are uploaded and create the transformer object
    if df_Parquet is not None and json_rule is not None:
        transformer = DataTransformer(df_Parquet, json_rule.get_dataframe())

    # Button to apply transformations
    if st.button("Apply Transformations"):
        if transformer:  # Check if transformer is not None
            transformer.apply_transformations()
            transformed_df = transformer.get_transformed_dataframe()
            if transformed_df is not None:
                st.dataframe(transformed_df)
        else:
            st.error("Please upload both Parquet and JSON files before applying transformations.")

main()

    
