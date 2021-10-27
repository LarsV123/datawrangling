from connector import Connector
from inserter import read_all_users
from utils import linebreak
import argparse
from time import time


def execute_script(db: Connector, filename: str):
    file = open(filename, "r")

    # Read lines and strip line breaks
    lines = [x.strip().replace("\n", "") for x in file.read().split(";")]

    # Remove empty lines
    sql = [x + ";" for x in lines if x]
    file.close()

    # Execute every command from the input file
    for command in sql:
        # This will skip and report errors
        try:
            db.cursor.execute(command)
            db.connection.commit()
        except Exception as e:
            print("Command skipped: ", command)
            print("Error:", e)


def main(args):
    linebreak()
    db = Connector()
    start = time()

    if args.init:
        linebreak()
        print("Dropping and then creating all tables...")
        execute_script(db, "create_tables.sql")
        print("Successfully created tables")
    if args.index:
        linebreak()
        print("Creating all relevant indexes...")
        execute_script(db, "create_indexes.sql")
        print("Successfully created indexes")
    if args.fill:
        linebreak()
        print(f"Inserting the first {args.fill} users from the dataset...")
        read_all_users(db, args.fill)
    if args.query:
        print(f"{args.query=}")
    if args.queries:
        print(f"{args.queries=}")

    linebreak()
    end = time()
    elapsed = round(end - start, 2)
    print(f"Total time elapsed: {elapsed} seconds")
    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--init",
        dest="init",
        action="store_true",
        help="Initialize all relevant tables.",
    )
    parser.add_argument(
        "--index",
        dest="index",
        action="store_true",
        help="Create all relevant indexes.",
    )
    parser.add_argument(
        "--fill",
        choices=range(1, 182),
        metavar="[1-182]",
        type=int,
        help="Fill the database with sanitized data for up to the specified number of users.",
    )
    parser.add_argument(
        "-q",
        "--query",
        choices=range(0, 13),
        metavar="[0-12]",
        type=int,
        help="Execute a specific query.",
    )
    parser.add_argument(
        "--queries",
        nargs=2,
        choices=range(0, 13),
        metavar="[0-12]",
        type=int,
        help="Execute all queries in the specified range.",
    )

    args = parser.parse_args()
    main(args)
