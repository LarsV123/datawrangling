from tqdm import tqdm
from math import cos, sqrt


def linebreak():
    """
    Print a newline and a separating line. It's split into a function to
    keep it standardized across the project.
    """
    tqdm.write("\n" + "-" * 80)


def get_table_sample(db, table: str):
    """
    Return the first 10 rows of a table.
    """
    query = f"""
        SELECT *
        FROM {table}
        ORDER BY id ASC
        LIMIT 10
        """
    db.cursor.execute(query)
    return db.cursor.fetchall()


def get_all_users(db) -> list[str]:
    """
    Get a list of all user IDs.
    """
    query = """
    SELECT id 
    FROM users;
    """
    db.cursor.execute(query)
    rows = db.cursor.fetchall()
    result = [x[0] for x in rows]
    return result


def get_activities(db, user_id: str) -> list[int]:
    """
    Get all activity IDs for a given user.
    """
    query = f"""
    SELECT id 
    FROM activities
    WHERE user_id = '{user_id}';
    """
    db.cursor.execute(query)
    rows = db.cursor.fetchall()
    result = [x[0] for x in rows]
    return result


def get_trackpoints(db, activity_id: int):
    """
    Get all trackpoints for a certain activity.
    """
    query = f"""
    SELECT *
    FROM trackpoints
    WHERE activity_id = {activity_id}
    ORDER BY date_time;
    """
    db.cursor.execute(query)
    rows = db.cursor.fetchall()
    return rows


def estimate_distance(lat0, lon0, lat1, lon1):
    """
    Returns the estimated distance in meters.
    Faster than haversine, not as accurate.
    """
    deglen = 110.25
    x = lat0 - lat1
    y = (lon0 - lon1) * cos(lat1)
    return deglen * sqrt(x * x + y * y) * 1000
