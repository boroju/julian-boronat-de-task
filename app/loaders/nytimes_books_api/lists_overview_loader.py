from app.loaders.utils import ApiKeyAuthApi
from app.loaders.utils import get_list_of_dates
from app.loaders.utils import logging
from app.loaders.utils import Dict, List
from datetime import datetime
import pandas as pd


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
            lists.extend(self.parse_api_response(r))

        return lists

    @staticmethod
    def parse_api_response(response):

        results = response.get("results")

        if results:
            # List Data (as a DataFrame)
            print("Hello!")
            print(results)
            print(results['list_id'])
            list_data = {
                "list_id": results.get("list_id"),
                "list_name": results.get("list_name"),
                "display_name": results.get("display_name", None)
            }
            list_df = pd.DataFrame([list_data])

            # Book Data (as a DataFrame)
            books_data = results.get("lists", [])
            books = []
            for book in books_data:
                book_data = {
                    "sk_list_id": results.get("list_id"),
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
                books.append(book_data)
            books_df = pd.DataFrame(books)

            # Buy Links Data (assuming buy_links are nested within books)
            buy_links = []
            for book in books_data:
                buy_links_data = book.get("buy_links", [])
                for buy_link in buy_links_data:
                    buy_link_data = {
                        "sk_list_id": results.get("list_id"),
                        "sk_book_primary_isbn10": book.get("primary_isbn10", None),
                        "sk_book_primary_isbn13": book.get("primary_isbn13", None),
                        "name": buy_link.get("name", None),
                        "url": buy_link.get("url", None)
                    }
                    buy_links.append(buy_link_data)
            buy_links_df = pd.DataFrame(buy_links)

            # Return DataFrames
            return list_df, books_df, buy_links_df

        else:
            # Handle case where "results" is not found
            return None, None, None


