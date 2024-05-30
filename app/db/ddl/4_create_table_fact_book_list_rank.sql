CREATE TABLE IF NOT EXISTS fact_book_list_rank (
  book_id INTEGER,
  list_id INTEGER,
  rank INTEGER,
  date_id INTEGER,
  FOREIGN KEY (book_id) REFERENCES dim_books(book_id),
  FOREIGN KEY (list_id) REFERENCES dim_lists(list_id),
  FOREIGN KEY (date_id) REFERENCES dim_time(date_id)
);
