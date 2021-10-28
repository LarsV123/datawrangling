import cProfile
import pstats
from connector import Connector
import queries
from time import time


def main():
    start = time()
    with cProfile.Profile() as pr:
        db = Connector()
        # Add the function to be profiled here
        queries.task6(db)
    end = time()
    elapsed = round(end - start, 2)
    stats = pstats.Stats(pr)
    stats.sort_stats(pstats.SortKey.TIME)
    stats.print_stats()
    stats.dump_stats(filename="profiling.prof")
    print(f"{elapsed=}")
    print("Inspect results with the command:")
    print("snakeviz profiling.prof")


if __name__ == "__main__":
    main()
