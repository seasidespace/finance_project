import streamlit as st
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

class SnowflakeUploader:
    def __init__(self, connection_params, dataframe):
        self.connection_params = connection_params
        self.dataframe = dataframe
        self.connection = None

    def connect_to_snowflake(self):
        self.connection = connect(**self.connection_params)

    def upload_dataframe(self):
        # UI components
        option = st.selectbox(
            'Choose your action:',
            ('create a new table and insert data', 'Insert the data into an existing table')
        )

        table_name = st.text_input('Enter the name of the table:')

        if st.button('Upload Data'):
            if not table_name:
                st.warning('Please enter the name of the table.')
                return

            self.connect_to_snowflake()

            if option == 'create a new table and insert data':
                self.create_table_and_insert_data(table_name)
            elif option == 'Insert the data into an existing table':
                self.insert_data_into_existing_table(table_name)

    def create_table_and_insert_data(self, table_name):
        # Here you would have logic to create a new table and insert data
        # For simplicity, we're using write_pandas which automatically creates the table
        write_pandas(self.connection, self.dataframe, table_name.upper())

    def insert_data_into_existing_table(self, table_name):
        # Here you would have logic to insert into an existing table
        # This includes checking if the table exists and if the columns match
        cursor = self.connection.cursor()
        cursor.execute(f"DESC TABLE {table_name.upper()}")
        table_description = cursor.fetchall()

        # Check if columns match
        table_columns = [desc[0] for desc in table_description]
        df_columns = self.dataframe.columns.tolist()

        if set(df_columns).issubset(set(table_columns)):
            write_pandas(self.connection, self.dataframe, table_name.upper())
        else:
            st.warning('The columns of the DataFrame do not match the columns of the existing table.')


