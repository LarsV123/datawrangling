CREATE INDEX start_date_index ON activities (start_date_time);
CREATE INDEX end_date_index ON activities (end_date_time);
CREATE INDEX mode_index ON activities (transportation_mode);

CREATE INDEX datetime_index ON trackpoints (date_time);
CREATE INDEX altitude_index ON trackpoints (altitude);

CREATE INDEX label_index ON users (has_labels);