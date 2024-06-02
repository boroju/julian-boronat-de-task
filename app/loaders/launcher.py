from app.loaders.nytimes_books_api.lists_overview_loader import NYTimesListsOverviewLoader
from dotenv import load_dotenv
import os

load_dotenv()

nytimes_loader = NYTimesListsOverviewLoader(load_type="lists",
                                            api_key=os.getenv('NYTIMES_API_KEY'),
                                            api_host=os.getenv('NYTIMES_HOST'))

nytimes_loader.load()
