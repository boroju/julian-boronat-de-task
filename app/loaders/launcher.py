from app.loaders.nytimes_books_api.lists_overview_loader import NYTimesListsOverviewLoader
from app import ROOT_DIR
from dotenv import load_dotenv
import os

load_dotenv()

print(ROOT_DIR)

nytimes_loader = NYTimesListsOverviewLoader(load_type="lists",
                                            api_key=os.getenv('NYTIMES_API_KEY'),
                                            api_host=os.getenv('NYTIMES_HOST'))

response = nytimes_loader.get_lists_request_json("2021-01-01")

list_df, books_df, buy_links_df = nytimes_loader.parse_api_response(response)

print("=====================================")

print("Dataframe info:")
print(list_df.info())

print("Dataframe DTypes:")
data_types = list_df.dtypes
print(data_types)

print("Dataframe Columns:")
column_names = list_df.columns
print(column_names)

number_of_rows = len(list_df)
print(f"The DataFrame has {number_of_rows} rows.")

print("Dataframe Head(5):")
print(list_df.head(5))

print("=====================================")

print("Dataframe info:")
print(books_df.info())

print("Dataframe DTypes:")
data_types = books_df.dtypes
print(data_types)

print("Dataframe Columns:")
column_names = books_df.columns
print(column_names)

number_of_rows = len(books_df)
print(f"The DataFrame has {number_of_rows} rows.")

print("Dataframe Head(5):")
print(books_df.head(5))

print("=====================================")

print("Dataframe info:")
print(buy_links_df.info())

print("Dataframe DTypes:")
data_types = buy_links_df.dtypes
print(data_types)

print("Dataframe Columns:")
column_names = buy_links_df.columns
print(column_names)

number_of_rows = len(buy_links_df)
print(f"The DataFrame has {number_of_rows} rows.")

print("Dataframe Head(5):")
print(buy_links_df.head(5))
