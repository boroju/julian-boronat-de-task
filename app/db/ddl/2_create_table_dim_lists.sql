CREATE TABLE IF NOT EXISTS dim_lists (
  list_id INTEGER PRIMARY KEY,
  list_name TEXT,
  display_name TEXT,
  updated TEXT,
  list_image TEXT,
  bestsellers_date DATE,
  published_date DATE,
  FOREIGN KEY (list_id) REFERENCES dim_books(book_id)
);
