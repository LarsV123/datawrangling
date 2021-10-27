DROP TABLE trackpoints;
DROP TABLE activities;
DROP TABLE users;

CREATE TABLE IF NOT EXISTS users (
  id VARCHAR(3) PRIMARY KEY,
  has_labels BOOLEAN DEFAULT FALSE 
);

CREATE TABLE IF NOT EXISTS activities (
  id SERIAL PRIMARY KEY,
  user_id VARCHAR(3),
  transportation_mode VARCHAR(30),
  start_date_time timestamp,
  end_date_time timestamp,
  FOREIGN KEY (user_id) 
    REFERENCES users(id)
    ON DELETE CASCADE 
);

CREATE TABLE IF NOT EXISTS trackpoints (
  id SERIAL PRIMARY KEY,
  activity_id INTEGER,
  lat REAL,
  lon REAL,
  altitude REAL,
  date_time timestamp,
  FOREIGN KEY (activity_id) 
    REFERENCES activities(id)
    ON DELETE CASCADE 
);
