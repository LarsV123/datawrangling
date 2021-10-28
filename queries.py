from connector import Connector
from tqdm import tqdm
from tabulate import tabulate
from utils import estimate_distance, get_all_users, get_table_sample
from datetime import datetime, timedelta
import calendar
from haversine import haversine, Unit
from collections import defaultdict


def run_query(db: Connector, query: int):
    if query == 0:
        task0(db)
    elif query == 1:
        task1(db)
    elif query == 2:
        task2(db)
    elif query == 3:
        task3(db)
    elif query == 4:
        task4(db)
    elif query == 5:
        task5(db)
    elif query == 6:
        task6(db)
    elif query == 7:
        task7(db)
    elif query == 8:
        task8(db)
    elif query == 9:
        task9(db)
    elif query == 10:
        task10(db)
    elif query == 11:
        task11(db)
    elif query == 12:
        task12(db)
    else:
        tqdm.write(f"Invalid query specified: {query}")


def task0(db: Connector):
    task = """Task 0: 
    Show a sample of the database by fetching the first 10 rows from each table.
    """
    print(task)

    users = get_table_sample(db, "users")
    print("Table: users")
    print(tabulate(users, headers=["ID", "Has label"]))
    print()

    print("Table: activities")
    activities = get_table_sample(db, "activities")
    print(
        tabulate(
            activities,
            headers=[
                "ID",
                "User",
                "Transportation mode",
                "Start time",
                "End time",
            ],
        )
    )
    print()

    print("Table: trackpoints")
    trackpoints = get_table_sample(db, "trackpoints")
    print(
        tabulate(
            trackpoints,
            headers=["ID", "Activity", "Latitude", "Longitude", "Altitude", "Time"],
        )
    )


def task1(db: Connector):
    task = """Task 1:
    How many users, activities and trackpoints are there in the dataset
    (after it is inserted into the database)?
    """
    print(task)

    tables = ["users", "activities", "trackpoints"]
    counts = []
    for table in tables:
        item = dict()
        item["Table"] = table

        query = f"SELECT COUNT(*) FROM {table}"
        db.cursor.execute(query)
        count = db.cursor.fetchone()[0]
        item["Entries"] = count
        counts.append(item)
    print(tabulate(counts, headers="keys"))


def task2(db: Connector):
    task = """Task 2:
    Find the average, minimum and maximum number of activities per user.
    """
    print(task)
    inner_query = """
    SELECT user_id, COUNT(*) AS count
    FROM activities
    GROUP BY user_id
    """

    functions = ["AVG", "MIN", "MAX"]
    for func in functions:
        query = f"""SELECT {func}(nested.count) FROM ({inner_query}) AS nested;"""
        db.cursor.execute(query)
        data = db.cursor.fetchone()[0]
        print(f"{func}: {round(data, 2)}")


def task3(db: Connector):
    task = """Task 3:
    Find the top 10 users with the highest number of activities.
    """
    print(task)
    query = """
    SELECT user_id, COUNT(*) AS count
    FROM activities
    GROUP BY user_id
    ORDER BY count DESC
    LIMIT 10;
    """
    db.cursor.execute(query)
    data = db.cursor.fetchall()
    print(tabulate(data, headers=["User", "Activities"]))


def task4(db: Connector):
    task = """Task 4:
    Find out how many activities each user has which start on one day
    and end on another. Who has the most such multiday activities?
    """
    print(task)

    query = """
    SELECT user_id, COUNT(id) AS items
    FROM (
        SELECT id, user_id
        FROM activities
        WHERE start_date_time::date < end_date_time::date
        GROUP BY user_id, id
    ) AS a
    GROUP BY user_id
    ORDER BY items DESC
    """
    db.cursor.execute(query)
    data = db.cursor.fetchall()
    print(tabulate(data, headers=["User", "Activities"]))


def task5(db: Connector):
    task = """Task 5:
    Find activities that are registered multiple times. You should find the
    query even if you get zero results.
    """
    print(task)

    query = """
    SELECT user_id, start_date_time, end_date_time, COUNT(*)
    FROM activities
    GROUP BY user_id, start_date_time, end_date_time
    HAVING COUNT(*) > 1;
    """

    db.cursor.execute(query)
    data = db.cursor.fetchall()
    print(tabulate(data, headers=["User", "Start", "End", "Duplicates"]))


def task6(db: Connector):
    task = """Task 6:
    Find all users which have been close to each other in time
    and space (Covid-19 tracking). Close is defined as the same minute
    (60 seconds) and space (100 meters).
    """
    print(task)
    get_count = f"SELECT COUNT(*) FROM trackpoints"
    db.cursor.execute(get_count)
    trackpoint_count = db.cursor.fetchone()[0]
    progress_bar = tqdm(total=trackpoint_count)

    get_trackpoints = f"""
    SELECT user_id, date_time, lat, lon
    FROM users
    INNER JOIN activities
    ON users.id = user_id
    INNER JOIN trackpoints
    ON activities.id = activity_id
    ORDER BY date_time ASC;
    """
    db.cursor.execute(get_trackpoints)

    """
    Strategy:
    - Fetch all trackpoints, sorted on timestamp
    - For every point, compare it to all other points within 1 minute
    """
    users = defaultdict(set)
    points = db.cursor.fetchmany(100000)
    current_index = 0
    next_index = 0

    while trackpoint_count - 1 > current_index:
        next_index += 1
        if next_index >= len(points):
            new_points = db.cursor.fetchmany(100000)
            points.extend(new_points)

        if next_index > trackpoint_count - 1:
            # Completed all comparisons for the current point
            progress_bar.update(1)
            current_index += 1
            next_index = current_index + 1
            continue

        current_point = points[current_index]
        next_point = points[next_index]
        if current_point[0] == next_point[0]:
            # Same user
            continue

        # Find time difference
        time_delta: timedelta = next_point[1] - current_point[1]
        if time_delta.seconds > 60:
            # Completed all comparisons for the current point
            progress_bar.update(1)
            current_index += 1
            next_index = current_index + 1
            continue

        current_location = (current_point[2], current_point[3])
        next_location = (next_point[2], next_point[3])
        estimated_distance = estimate_distance(
            current_point[2], current_point[3], next_point[2], next_point[3]
        )
        if estimated_distance < 150:
            distance = haversine(current_location, next_location, unit=Unit.METERS)
            if distance < 100:
                users[current_point[0]].add(next_point[0])
                users[next_point[0]].add(current_point[0])

    # Finally done!
    progress_bar.update(1)
    progress_bar.close()
    print()
    for key in sorted(users):
        value = users[key]
        print(f"User {key} has met the following {len(value)} user(s): {value}")


def task7(db: Connector):
    task = """Task 7:
    Find all users who have never taken a taxi.
    """
    print(task)

    query = """ 
    SELECT DISTINCT(user_id) 
    FROM activities 
    WHERE user_id NOT IN 
        (SELECT DISTINCT(user_id) 
        FROM activities 
        WHERE transportation_mode = 'taxi')
    AND user_id IN
        (SELECT DISTINCT(user_id) 
        FROM activities 
        WHERE transportation_mode IS NOT NULL)
    ;
    """
    db.cursor.execute(query)
    data = db.cursor.fetchall()
    print(tabulate(data, headers=["User"]))


def task8(db: Connector):
    task = """Task 8:
    Find all types of transportation modes and count how many distinct 
    users that have used the different transportation modes. Do not count 
    the rows where the transportation mode is null.
    """
    print(task)

    query = """
    SELECT transportation_mode, COUNT(DISTINCT user_id) 
    FROM activities 
    WHERE transportation_mode IS NOT NULL 
    GROUP BY transportation_mode; 
    """
    db.cursor.execute(query)
    data = db.cursor.fetchall()
    print(tabulate(data, headers=["Transportation mode", "Users"]))


def task9(db: Connector):
    task = """Task 9:
    a) Find the year and month with the most activities.
    b) Find the user with the most activities this year and month, and how
    many recorded hours do they have? Do they have more hours recorded 
    than the user with the second most activities?

    Assumption: An activity "belongs" to the month it was started in.
    """
    print(task)
    get_top_month = """
    SELECT DATE_TRUNC('month', start_date_time) AS month, COUNT(*) AS count
    FROM activities
    GROUP BY month
    ORDER BY count DESC
    LIMIT 1;
    """
    db.cursor.execute(get_top_month)
    data = db.cursor.fetchone()
    date: datetime = data[0]
    year = date.year
    month = date.month
    month_name = calendar.month_name[month]
    count: int = data[1]
    print(f"Most active month: {month_name} {year} ({count} activities)")

    get_count_activities = f"""
    SELECT 
        user_id,
        COUNT(id) AS count
    FROM activities
    WHERE DATE_TRUNC('month', start_date_time) = '{year}-{month}-01'
    GROUP BY user_id
    ORDER BY count DESC
    LIMIT 2;
    """
    db.cursor.execute(get_count_activities)
    users = db.cursor.fetchall()

    results = []
    for user in users:
        username = user[0]
        count = user[1]
        get_time = f"""
        SELECT SUM(EXTRACT(EPOCH FROM (end_date_time - start_date_time))) AS diff
        FROM activities
        WHERE user_id = '{username}'
        AND DATE_TRUNC('month', start_date_time) = '{year}-{month}-01';
        """
        db.cursor.execute(get_time)
        seconds = db.cursor.fetchone()[0]
        hours = round(seconds / (60 * 60), 2)
        results.append([username, count, hours])

    print(tabulate(results, headers=["User", "Activities", "Hours"]))


def task10(db: Connector):
    task = """Task 10:
    Find the total distance (in km) walked in 2008, by user with id=112.
    """
    print(task)
    # Find all relevant activities
    get_activities = f"""
    SELECT id
    FROM activities
    WHERE user_id = '112'
    AND transportation_mode = 'walk';
    """
    db.cursor.execute(get_activities)
    activities = db.cursor.fetchall()

    # For each activity, find all trackpoints
    get_trackpoints = """
    SELECT lat, lon
    FROM trackpoints
    WHERE activity_id = %d
    AND EXTRACT(year FROM date_time) = 2008;
    """

    total_distance = 0
    for activity in activities:
        activity_id = activity[0]
        db.cursor.execute(get_trackpoints % activity_id)
        trackpoints = db.cursor.fetchall()

        activity_distance = 0
        for j in range(1, len(trackpoints)):
            previous = trackpoints[j - 1]
            current = trackpoints[j]
            distance = haversine(previous, current, unit=Unit.KILOMETERS)
            activity_distance += distance

        # Add distance for this activity to total distance
        total_distance += activity_distance

    print(f"User 112 walked {round(total_distance, 2)} km in 2008")


def task11(db: Connector):
    task = """Task 11:
    Find the top 20 users who have gained the most altitude meters.
    Output should be a table with (id, total meters gained per user).
    Remember that some altitude-values are invalid.
    """
    print(task)
    query = f"""
    SELECT user_id, activity_id, sum(altitude - previous_altitude) AS diff
    FROM
        (
        SELECT user_id, activity_id, altitude, LAG (altitude, 1, 0) OVER (
            PARTITION BY activity_id
            ORDER BY date_time
        ) as previous_altitude
        FROM trackpoints
        INNER JOIN activities
        ON activities.id = trackpoints.activity_id
        WHERE altitude != -777
        ) AS foo
    WHERE previous_altitude != -777
    AND previous_altitude < altitude
    GROUP BY user_id, activity_id;
    """

    db.cursor.execute(query)
    result = db.cursor.fetchall()
    users = defaultdict(lambda: 0)
    for row in result:
        distance = float(row[2]) * 0.3048  # Convert feet => meter
        users[row[0]] += distance

    sorted_users = sorted(users.items(), key=lambda item: item[1], reverse=True)

    print(tabulate(sorted_users[:20], headers=["User", "Altitude gained (meters)"]))


def task12(db: Connector):
    task = """Task 12:
    Find all users who have invalid activities, and the number of invalid
    activities per user.

    An invalid activity is defined as an activity with consecutive
    trackpoints where the timestamps deviate with at least 5 minutes.
    """
    print(task)
    all_users = get_all_users(db)
    count_per_user = defaultdict(lambda: 0)
    for i in tqdm(range(len(all_users))):
        user_id = all_users[i]

        get_trackpoints = f"""
        SELECT date_time, activity_id
        FROM trackpoints
        INNER JOIN activities
        ON activities.id = trackpoints.activity_id
        WHERE activities.user_id = '{user_id}'
        ORDER BY date_time ASC;
        """
        db.cursor.execute(get_trackpoints)
        points = ["not empty"]
        previous = None
        current = None
        is_invalid_activity = False
        while len(points) > 0:
            points = db.cursor.fetchmany(100000)
            for j in range(len(points)):
                # Find current pair of trackpoints to compare
                if previous is None:
                    previous = points[0]
                    continue
                current = points[j]

                if current[1] != previous[1]:
                    # We've reached a new activity
                    is_invalid_activity = False
                    previous = current
                    continue

                if is_invalid_activity:
                    previous = current
                    continue

                # Still inside the same valid activity
                delta = current[0] - previous[0]
                minutes = delta.seconds / 60
                previous = current

                if minutes > 5:
                    count_per_user[user_id] += 1
                    is_invalid_activity = True
    print(tabulate(count_per_user.items(), headers=["User", "Invalid activities"]))


if __name__ == "__main__":
    print("This should only be called directly when debugging")
    db = Connector()
