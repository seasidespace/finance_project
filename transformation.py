import pandas as pd
import json


class DataTransformer:
    def __init__(self, parquet_data, json_rules):
        self.df = parquet_data
        self.transformation_rules = json_rules

    def apply_transformations(self):
        try:
            for column, transformation in self.transformation_rules.items():
                if column in self.df.columns:
                    for operation, params in transformation.items():
                        if operation == 'astype':
                            self.df[column] = self.df[column].astype(params)
                        elif operation == 'map':
                            self.df[column] = self.df[column].map(params)
                        elif operation == 'to_datetime':
                            self.df[column] = pd.to_datetime(self.df[column], format=params['format'])
                        else:
                            st.error(f"Unsupported operation: {operation}")
                else:
                    st.warning(f"Column {column} not in DataFrame")
            st.success("Transformations applied successfully!")
        except Exception as e:
            st.error(f"An error occurred while applying transformations: {e}")

    def get_transformed_dataframe(self):
        return self.df
