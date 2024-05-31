from app.loaders.nytimes_books_api.lists_overview_loader import NYTimesListsOverviewLoader
from app.db.database import DuckDBDatabase
from app import ROOT_DIR
from dotenv import load_dotenv
import duckdb
import os

load_dotenv()

print(ROOT_DIR)

nytimes_loader = NYTimesListsOverviewLoader(load_type="lists",
                                            api_key=os.getenv('NYTIMES_API_KEY'),
                                            api_host=os.getenv('NYTIMES_HOST'))

response = nytimes_loader.get_lists_request_json("2021-01-01")
print(response)
bestsellers_df, list_df, books_df, buy_links_df = nytimes_loader.parse_api_response(response, "2021-01-01")

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

db = DuckDBDatabase(db_path=os.path.join(ROOT_DIR, "db", "nytimes_books.duckdb"))
con = db.connect()

con.sql("CREATE TABLE IF NOT EXISTS dim_bestsellers AS SELECT * FROM bestsellers_df")
con.sql("CREATE TABLE IF NOT EXISTS dim_lists AS SELECT * FROM list_df")
con.sql("CREATE TABLE IF NOT EXISTS dim_books AS SELECT * FROM books_df")
con.sql("CREATE TABLE IF NOT EXISTS dim_buy_links AS SELECT * FROM buy_links_df")
con.commit()

con.sql("INSERT INTO dim_bestsellers SELECT * FROM bestsellers_df")
con.sql("INSERT INTO dim_lists SELECT * FROM list_df")
con.sql("INSERT INTO dim_books SELECT * FROM books_df")
con.sql("INSERT INTO dim_buy_links SELECT * FROM buy_links_df")
con.commit()

db.disconnect()
print("Tables created and data inserted successfully!")

