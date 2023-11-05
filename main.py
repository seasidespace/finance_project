import snowflake.connector
from snowflake.snowpark import Session
from snowflake.sqlalchemy import URL
import streamlit as st
import openai
from file_uploader import FileUploader

def main():
    st.title("Data Ingestion Tool")
    
    myFile = FileUploader()

    # Use the upload_file method to let the user upload a file
    label = "Drag and drop or click to upload a Parquet file"
    uploaded_file = myFile.upload_file(label, "parquet")
    
    # If a file was uploaded, read the Parquet file
    if uploaded_file:
        myFile.read_parquet(uploaded_file)

        # Get the DataFrame and display it
        df = myFile.get_dataframe()
        if df is not None:
            st.write("Quick View of the Uploaded File:")
            st.dataframe(df.head())  # Display the first few rows of the DataFrame

main()
