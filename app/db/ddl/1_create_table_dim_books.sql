CREATE TABLE IF NOT EXISTS dim_books (
  book_id INTEGER PRIMARY KEY,
  age_group TEXT,
  author TEXT,
  contributor TEXT,
  contributor_note TEXT,
  created_date DATETIME,
  description TEXT,
  price DECIMAL,
  primary_isbn13 TEXT,
  primary_isbn10 INTEGER,
  publisher TEXT,
  title TEXT,
  updated_date DATETIME
);
