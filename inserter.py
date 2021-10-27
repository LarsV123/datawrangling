import os
import re
from dotenv import load_dotenv, find_dotenv
from tqdm import tqdm
from connector import Connector
import time
from psycopg2.extras import execute_batch

load_dotenv(find_dotenv())


def read_all_users(max: int):
    """
    Read all users, or up to a specified max number of users (for testing).
    All user data will be inserted into the database.
    """
    # Set up database connection
    db = Connector()

    folder = os.environ.get("DATASET_PATH")
    subfolders = os.listdir(folder)
    folder_count = len(subfolders)
    progress_bar = tqdm(total=folder_count)

    # Clear all related data before inserting
    query = "TRUNCATE TABLE users CASCADE"
    db.cursor.execute(query)
    db.connection.commit()

    users = 0
    for subfolder in subfolders:
        read_user(db, folder, subfolder)
        progress_bar.update(1)
        users += 1
        if users >= max:
            break


def read_user(db: Connector, data_folder: str, user: str):
    user_folder = os.path.join(data_folder, user)

    # Find labels (if any)
    labels = None
    has_labels = False
    label_file = os.path.join(user_folder, "labels.txt")
    if os.path.exists(label_file):
        labels = read_labels(label_file)
        has_labels = True

    # Add user to database (must escape ID to keep it as string)
    query = f"INSERT INTO users(id, has_labels) VALUES ('{user}', {has_labels});"
    db.cursor.execute(query)
    db.connection.commit()

    # Find and insert all activities for this user
    tqdm.write(f"Inserting activities for user {user}")
    activity_folder = os.path.join(user_folder, "Trajectory")
    for root, _, files in os.walk(activity_folder):
        for file in files:
            plt_file = os.path.join(root, file)
            parse_trackpoints(db, user, plt_file, labels)


def read_labels(label_file: str) -> list:
    """
    Get all labels for a given user.

    Returns a list of labels, where each label is a list on the format
    [start_time, end_time, transportation_mode]
    """
    labels = []
    with open(label_file) as label_reader:
        next(label_reader)  # skips header
        file = label_reader.read()
        for row in file.splitlines():
            item = re.split("\t|\s", row.strip())

            # Parse dates to datetime
            item = [l.replace("/", "-") for l in item]
            start = f"{item[0]} {item[1]}"
            end = f"{item[2]} {item[3]}"
            mode = item[4]
            label = [start, end, mode]
            labels.append(label)
    return labels


def parse_trackpoints(db: Connector, user: str, file_path: str, labels: list):
    """
    Parse and insert a single .plt file.

    Data columns:
    0. Latitude
    1. Longitude
    2. 0 (skip)
    3. Altitude (-777 for invalid values)
    4. Date in days (skip)
    5. Date as string
    6. Time as string
    """
    # Data to insert
    activity = {}
    activity["user"] = user
    trackpoints = []

    """
    Read and parse .plt file.

    Skip the first 6 lines, remove whitespace and linebreaks, and split 
    comma-separated values into a list.
    Skip the file if it has more than 2500 entries.
    """
    file = open(file_path, "r")
    rows = file.readlines()
    rows = [x.strip().split(",") for x in rows[6:]]
    if len(rows) > 2500:
        return
    file.close()

    """
    Find a matching label (if any).
    If the user has labels, we check if the first and last trackpoint in
    the activity matches one of those labels. If we don't find a match,
    we don't label the activity.
    """
    activity["label"] = None
    first = rows[0]
    last = rows[-1]
    activity["start"] = f"{first[5]} {first[6]}"
    activity["end"] = f"{last[5]} {last[6]}"
    if labels:
        for item in labels:
            start, end, mode = item
            if start == activity["start"] and end == activity["end"]:
                activity["label"] = mode
                break

    """
    Insert the activity into the database and get its ID in return.
    """
    query = f"""
    INSERT INTO activities
    (user_id, transportation_mode, start_date_time, end_date_time) 
    VALUES (%(user)s, %(label)s, %(start)s, %(end)s)
    RETURNING id;
    """
    db.cursor.execute(query, activity)
    db.connection.commit()

    # ID of the activity record that was just inserted
    activity_id = db.cursor.fetchone()[0]

    for row in rows:
        point = {}
        point["activity_id"] = activity_id
        point["lat"] = float(row[0])
        point["lon"] = float(row[1])
        point["alt"] = float(row[3])
        if point["alt"] == -777:
            point["alt"] = None
        point["date_time"] = f"{row[5]} {row[6]}"
        trackpoints.append(point)

    query = f"""
    INSERT INTO trackpoints (activity_id, lat, lon, altitude, date_time)
    VALUES (%(activity_id)s, %(lat)s, %(lon)s, %(alt)s, %(date_time)s);
    """
    execute_batch(db.cursor, query, trackpoints, 1000)
    db.connection.commit()
