CREATE TABLE IF NOT EXISTS dim_time (
  date_id INTEGER PRIMARY KEY,
  date DATE,
  year INTEGER,
  quarter INTEGER,  -- Values 1 to 4
  week_of_year INTEGER,  -- Values 1 to 52
  day_of_week TEXT  -- Day name (e.g., "Monday")
);
