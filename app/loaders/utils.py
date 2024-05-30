import backoff as backoff
import logging
import requests
from typing import Dict, List
from requests.exceptions import RequestException
import pandas as pd
from datetime import datetime, timedelta
import time

logging.basicConfig(level=logging.INFO)


class TooManyRequestsException(Exception):
    """
    Raised when too many requests are sent to the API.
    """


class ApiKeyAuthApi:
    """
    A class for making API requests using API Key based authentication.

    This class provides methods for making GET and POST requests with automatic
    backoff for handling rate limiting and retries on errors.

    Attributes:
        api_key (str): Your API key for authentication.
        host (str): The base URL of the API.
    """

    def __init__(self, api_key: str, host: str) -> None:
        """
        Initializes the ApiKeyAuthApi class.

        Args:
            api_key (str): Your API key for authentication.
            host (str): The base URL of the API.
        """
        self.api_key = api_key
        self.host = host

    def create_header(self, extra_headers: Dict[str, str] = None) -> Dict[str, str]:
        """
        Creates a dictionary containing the headers for the API request.

        This method creates a dictionary with the API key included as a query parameter
        based on the New York Times Books API requirements.

        Args:
            extra_headers (Dict[str, str], optional): A dictionary containing
                additional headers to include in the request. Defaults to None.

        Returns:
            Dict[str, str]: A dictionary containing the headers for the request.
        """
        return extra_headers  # No headers needed in this case

    @backoff.on_exception(backoff.expo, (RequestException, TooManyRequestsException))
    def get(self, endpoint: str, params: Dict[str, str] = None,
            extra_headers: Dict[str, str] = None, **kwargs) -> requests.Response:
        """
        Makes a GET request to the specified API endpoint.

        This method constructs a GET request URL with the provided endpoint
        and parameters, including the API key as a query parameter. It sets any
        additional headers and sends the request using the requests library.
        It also implements backoff logic to handle rate limiting and retries on errors.

        Args:
            endpoint (str): The API endpoint to make the request to.
            params (Dict[str, str], optional): A dictionary containing query
                parameters for the request, including "api-key". Defaults to None.
            extra_headers (Dict[str, str], optional): A dictionary containing
                additional headers to include in the request. Defaults to None.
            **kwargs: Additional keyword arguments to be passed to the requests.get() method.

        Returns:
            requests.Response: The response object from the API request.

        Raises:
            TooManyRequestsException: If the API returns a 429 Too Many Requests status code.
        """
        url = self.host + endpoint
        print(url)

        # Ensure params include the API key
        if not params:
            params = {}
        params["api-key"] = self.api_key

        r = requests.get(
            url=url,
            params=params,
            headers=self.create_header(extra_headers),
            **kwargs
        )
        if r.status_code == 429:
            raise TooManyRequestsException
        return r


# Since the majority of New York Times Books API best-seller lists are updated weekly, setting the frequency to "W" (weekly) would be a more efficient approach. Here's why:
# - Reduced API Calls: By fetching data weekly, you'll minimize unnecessary API calls, potentially avoiding rate limits and optimizing resource usage.
# - Data Relevancy: As you mentioned, most lists likely update weekly, so daily retrievals might lead to redundant data with minimal or no data changes.
# - Weekly fetches ensure you capture the latest updates without overwhelming the system.
def get_list_of_dates(from_date: datetime = datetime(2021, 1, 1), to_date: datetime = datetime(2023, 1, 1), frequency: str = "W") -> List[str]:
    """
    Generate a list of dates between the specified start and end dates.

    This function uses the pandas date_range function to generate a list of dates
    between the specified start and end dates with the specified frequency.

    Args:
        from_date (datetime): The start date for the date range.
        to_date (datetime): The end date for the date range.
        frequency (str, optional): The frequency of the dates (e.g., "D" for daily, "W" for weekly).
            Defaults to "W".

    Returns:
        List[str]: A list of date strings in the format "YYYY-MM-DD".
    """
    date_list = pd.date_range(from_date, to_date, freq=frequency)
    date_list_str = [x.strftime("%Y-%m-%d") for x in date_list]

    return date_list_str

