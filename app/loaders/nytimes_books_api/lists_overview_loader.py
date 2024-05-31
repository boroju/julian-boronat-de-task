from app.loaders.utils import ApiKeyAuthApi
from app.loaders.utils import get_list_of_dates
from app.loaders.utils import logging
from app.loaders.utils import Dict, List
from datetime import datetime
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

    def load(self):

        if self.load_type == "lists":

            dates_list = get_list_of_dates()

            for published_date in dates_list:
                logging.info(f"Executing nytimes api (lists) call for {published_date}...")
                data = self.get_lists_request_json(published_date)
                logging.info("Storing data in the database...")
                self.process_and_store_data(data, published_date)

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

    def get_lists_on_daterange(self, load_dates: List[str]) -> List[Dict]:
        lists = []

        for published_date in load_dates:
            logging.info(f"Getting lists for {published_date}")
            r = self.get_lists_request_json(published_date)

            # Append parsed customers
            lists.extend(self.parse_api_response(response=r, published_date_req=published_date))

        return lists

    @staticmethod
    def generate_random_str() -> str:
        """
        Generates a random string of 10 characters
        """
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

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

            bestsellers_df = pd.DataFrame([bestsellers_row])
            list_df = pd.DataFrame(lists)
            books_df = pd.DataFrame(books)
            buy_links_df = pd.DataFrame(buy_links)

            # Return DataFrames
            return bestsellers_df, list_df, books_df, buy_links_df

        else:
            # Handle case where "results" is not found
            return None, None, None, None


