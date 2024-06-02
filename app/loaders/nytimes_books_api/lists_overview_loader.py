from app.loaders.utils import ApiKeyAuthApi
from app.loaders.utils import get_list_of_dates
from app.loaders.utils import logging
from app.loaders.utils import Dict, List
from app.db.database import DuckDBDatabase

from app import ROOT_DIR

import os
import pandas as pd
import string
import random


class NYTimesListsOverviewLoader:
    name = "nytimes_lists_overview_loader"

    def __init__(self,
                 api_key: str = None,
                 api_host: str = None,
                 load_type: str = None):
        """ Init function to build NYTimesListsOverviewLoader
        Args:
            api_key: API key for authentication
            api_host: API host URL
            load_type: with the possible following options to load data --> lists
        Returns:
            Nothing
        """

        # Api Authorization
        self.api = ApiKeyAuthApi(
            api_key=api_key,
            host=api_host
        )
        self.load_type = load_type
        self.db = DuckDBDatabase(db_path=os.path.join(ROOT_DIR, "dbt/transform/nytimes_books_project", "nytimes_books.duckdb"))

        logging.info("NYTimesListsOverviewLoader initialized.")
        logging.info(f"Load type: {self.load_type}")
        logging.info(f"ROOT DIR: {ROOT_DIR}")
        logging.info(f"Database path (DBT): {os.path.join(ROOT_DIR, 'dbt/transform/nytimes_books_project', 'nytimes_books.duckdb')}")

    def load(self):

        if self.load_type == "lists":
            logging.info("Executing NYTimesListsOverviewLoader...")
            logging.info(f"Creating DataFrames...")
            logging.info("Getting list of dates to load...")
            # Default date range is from 2021-01-01 to 2023-01-01
            # from_date: datetime = datetime(2021, 1, 1), to_date: datetime = datetime(2023, 1, 1), frequency: str = "W") -> List[str]:
            dates_list = get_list_of_dates()
            logging.info(f"Executing nytimes api calls...")
            bestsellers_df, list_df, books_df, buy_links_df = self.get_lists_on_daterange(load_dates=dates_list)
            logging.info(f"Data fetched successfully for all dates.")

            logging.info("Dataframe bestsellers_df Columns:")
            logging.info(bestsellers_df.columns)
            logging.info("Dataframe bestsellers_df Head(5):")
            logging.info(bestsellers_df.head(5))

            logging.info("Dataframe list_df Columns:")
            logging.info(list_df.columns)
            logging.info("Dataframe list_df Head(5):")
            logging.info(list_df.head(5))

            logging.info("Dataframe books_df Columns:")
            logging.info(books_df.columns)
            logging.info("Dataframe books_df Head(5):")
            logging.info(books_df.head(5))

            logging.info("Dataframe buy_links_df Columns:")
            logging.info(buy_links_df.columns)
            logging.info("Dataframe buy_links_df Head(5):")
            logging.info(buy_links_df.head(5))

            logging.info(f"Dropping database files if exists...")
            self.drop_database_files()

            logging.info(f"Saving data into database...")
            self.save_into_db()
            logging.info(f"Data saved successfully.")

            logging.info(f"Closing connection to database...")
            self.db.disconnect()
            logging.info(f"Connection closed successfully.")

            logging.info(f"NYTimesListsOverviewLoader executed successfully.")

    def get_lists_request_json(self, published_date: str, format: str = ".json") -> Dict:
        """ Get Json content for an API Call

        Returns:
            json
        """

        response = self.api.get(
            endpoint="/lists/overview" + format,
            params={
                'published_date': published_date
            },
            extra_headers={"Content-Type": "application/json"}
        )
        if not response.ok:
            logging.error(
                f"There was an error with API endpoint ({response.status_code})."
            )
            logging.error(f"Response:\n {response.text}")
            raise Exception(f"API Error: error code {response.status_code}")

        return response.json()

    def get_lists_on_daterange(self, load_dates: List[str]):

        _bestsellers_df = pd.DataFrame()
        _list_df = pd.DataFrame()
        _books_df = pd.DataFrame()
        _buy_links_df = pd.DataFrame()

        for published_date in load_dates:
            logging.info(f"Getting lists for {published_date}")
            r = self.get_lists_request_json(published_date)
            if r.get("results"):
                bs_df, l_df, bks_df, bls_df = self.parse_api_response(response=r, published_date_req=published_date)
                _bestsellers_df = pd.concat([_bestsellers_df, bs_df])
                _list_df = pd.concat([_list_df, l_df])
                _books_df = pd.concat([_books_df, bks_df])
                _buy_links_df = pd.concat([_buy_links_df, bls_df])
                logging.info(f"Lists for {published_date} fetched successfully.")

        return _bestsellers_df, _list_df, _books_df, _buy_links_df

    @staticmethod
    def generate_random_str() -> str:
        """
        Generates a random string of 10 characters
        """
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))

    def parse_api_response(self, response, published_date_req):

        results = response.get("results")

        if results:

            lists = []
            books = []
            buy_links = []

            request_id = self.generate_random_str()

            bestsellers_row = {
                "request_id": request_id,
                "request_published_date": published_date_req,
                "bestsellers_date": results.get("bestsellers_date", None),
                "published_date": results.get("published_date", None),
                "published_date_description": results.get("published_date_description", None),
                "previous_published_date": results.get("previous_published_date", None),
                "next_published_date": results.get("next_published_date", None),
            }

            # List Data (as a DataFrame)

            if results.get("lists"):
                list_data = results.get("lists")
                for _lists in list_data:
                    list_row = {
                        "fk_request_id": request_id,
                        "list_id": _lists.get("list_id"),
                        "list_name": _lists.get("list_name", None),
                        "list_name_encoded": _lists.get("list_name_encoded", None),
                        "display_name": _lists.get("display_name", None),
                        "updated": _lists.get("updated", None),
                        "list_image": _lists.get("list_image", None),
                        "list_image_width": _lists.get("list_image_width", None),
                        "list_image_height": _lists.get("list_image_height", None),
                    }
                    lists.append(list_row)

                    # Book Data (as a DataFrame)
                    if _lists.get("books"):
                        books_data = _lists.get("books")
                        for book in books_data:
                            book_row = {
                                "fk_request_id": request_id,
                                "fk_list_id": _lists.get("list_id"),
                                "age_group": book.get("age_group", None),
                                "amazon_product_url": book.get("amazon_product_url", None),
                                "article_chapter_link": book.get("article_chapter_link", None),
                                "author": book.get("author", None),
                                "book_review_link": book.get("book_review_link", None),
                                "contributor": book.get("contributor", None),
                                "contributor_note": book.get("contributor_note", None),
                                "created_date": book.get("created_date", None),
                                "description": book.get("description", None),
                                "first_chapter_link": book.get("first_chapter_link", None),
                                "price": book.get("price", None),
                                "primary_isbn10": book.get("primary_isbn10", None),
                                "primary_isbn13": book.get("primary_isbn13", None),
                                "book_uri": book.get("book_uri", None),
                                "publisher": book.get("publisher", None),
                                "rank": book.get("rank", None),
                                "rank_last_week": book.get("rank_last_week", None),
                                "title": book.get("title", None),
                                "updated_date": book.get("updated_date", None),
                                "weeks_on_list": book.get("weeks_on_list", None)
                            }
                            books.append(book_row)

                            if book.get("buy_links"):
                                buy_links_data = book.get("buy_links")
                                for buy_link in buy_links_data:
                                    buy_link_row = {
                                        "fk_request_id": request_id,
                                        "fk_list_id": _lists.get("list_id"),
                                        "fk_primary_isbn10": book.get("primary_isbn10", None),
                                        "fk_primary_isbn13": book.get("primary_isbn13", None),
                                        "name": buy_link.get("name", None),
                                        "url": buy_link.get("url", None)
                                    }
                                    buy_links.append(buy_link_row)

            _bestsellers_df = pd.DataFrame([bestsellers_row])
            _list_df = pd.DataFrame(lists)
            _books_df = pd.DataFrame(books)
            _buy_links_df = pd.DataFrame(buy_links)

            # Return DataFrames
            return _bestsellers_df, _list_df, _books_df, _buy_links_df

        else:
            # Handle case where "results" is not found
            return None, None, None, None

    @staticmethod
    def drop_database_files():
        """
        Drops the database file if it exists.
        """
        db_path = os.path.join(ROOT_DIR, "dbt/transform/nytimes_books_project", "nytimes_books.duckdb")
        db_wal_path = os.path.join(ROOT_DIR, "dbt/transform/nytimes_books_project", "nytimes_books.duckdb.wal")
        if os.path.isfile(db_path):
            try:
                os.remove(db_path)
                logging.info(f"Database file {db_path} dropped successfully.")
            except OSError as e:
                logging.error(f"Failed to drop database file: {e}")

        if os.path.isfile(db_wal_path):
            try:
                os.remove(db_wal_path)
                logging.info(f"Database WAL file {db_wal_path} dropped successfully.")
            except OSError as e:
                logging.error(f"Failed to drop database file: {e}")

    def save_into_db(self):
        """
        Create tables in the database
        :return: None
        """

        # Connect to the database
        con = self.db.connect()

        logging.info("Creating table dim_bestsellers...")
        logging.info("Inserting data into dim_bestsellers...")
        con.sql("CREATE TABLE IF NOT EXISTS dim_bestsellers AS SELECT * FROM bestsellers_df")

        logging.info("Creating table dim_lists...")
        logging.info("Inserting data into dim_lists...")
        con.sql("CREATE TABLE IF NOT EXISTS dim_lists AS SELECT * FROM list_df")

        logging.info("Creating table dim_books...")
        logging.info("Inserting data into dim_books...")
        con.sql("CREATE TABLE IF NOT EXISTS dim_books AS SELECT * FROM books_df")

        logging.info("Creating table dim_buy_links...")
        logging.info("Inserting data into dim_buy_links...")
        con.sql("CREATE TABLE IF NOT EXISTS dim_buy_links AS SELECT * FROM buy_links_df")

        con.commit()
