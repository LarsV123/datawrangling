from connector import Connector
from utils import linebreak
import argparse


def execute_script(filename):
    db = Connector()
    file = open(filename, "r")

    # Read lines and strip line breaks
    lines = [x.strip().replace("\n", "") for x in file.read().split(";")]

    # Remove empty lines
    sql = [x + ";" for x in lines if x]
    file.close()

    # Execute every command from the input file
    linebreak()
    print("Executing command(s):")
    for command in sql:
        print(command)
        # This will skip and report errors
        try:
            db.cursor.execute(command)
            db.connection.commit()
        except Exception as e:
            print("Command skipped: ", command)
            print("Error:", e)
            break
    db.close()


def main(args):
    if args.init:
        execute_script("create_tables.sql")
    if args.index:
        execute_script("create_indexes.sql")
    if args.query:
        print(f"{args.query=}")
    if args.queries:
        print(f"{args.queries=}")


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
