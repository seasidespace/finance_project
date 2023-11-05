import pandas as pd
import json


class DataTransformer:
    def __init__(self, parquet_data, json_rules):
        self.df = parquet_data
        self.transformation_rules = json_rules

    def apply_transformations(self):
        if self.df is not None and self.transformation_rules is not None:
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
        else:
            st.error("Data or transformation rules are not loaded. Please check the files.")

    def get_transformed_dataframe(self):
        if self.df is not None:
            return self.df
        else:
            st.error("No transformed DataFrame is available. Please apply transformations first.")
            return None

