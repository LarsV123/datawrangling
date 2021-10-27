from connector import Connector
from tqdm import tqdm


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
    tqdm.write("Not implemented yet")


def task1(db: Connector):
    tqdm.write("Not implemented yet")


def task2(db: Connector):
    tqdm.write("Not implemented yet")


def task3(db: Connector):
    tqdm.write("Not implemented yet")


def task4(db: Connector):
    tqdm.write("Not implemented yet")


def task5(db: Connector):
    tqdm.write("Not implemented yet")


def task6(db: Connector):
    tqdm.write("Not implemented yet")


def task7(db: Connector):
    tqdm.write("Not implemented yet")


def task8(db: Connector):
    tqdm.write("Not implemented yet")


def task9(db: Connector):
    tqdm.write("Not implemented yet")


def task10(db: Connector):
    tqdm.write("Not implemented yet")


def task11(db: Connector):
    tqdm.write("Not implemented yet")


def task12(db: Connector):
    tqdm.write("Not implemented yet")
