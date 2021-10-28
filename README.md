# Datawrangling example

This repository is a toy example done for educational purposes,
in order to practice using Python, Docker and SQL.

My goals for the project were:

- Set up a local Postgres database with Docker
- Clean and insert a large dataset into the database
- Query the database to solve specific tasks
- Do all of the above with clean code, reproducible steps and a command-line utility (CLI)

## Setup

Below are the steps required to run this project. Prequisites:

- Python (v3.9.6 used during testing)
- Docker

### Virtual environment

Using a virtual environment (venv) is recommended, but not necessary:

1. `py -m venv venv`
2. Activate venv in IDE. Run `pip list` to check that you are in the correct venv, it should only have _pip_ and _setuptools_ installed by default.
3. `pip install -r requirements.txt`

### Dataset

1. Get a copy of the raw dataset:

   - Download data from https://www.microsoft.com/en-us/research/publication/geolife-gps-trajectory-dataset-user-guide/
   - Place the Data folder in the project root folder, or point the environment variable to its location

1. Extract `dataset.zip`, for example to `/Data`

### Environment variables

Make a copy of the `.env-template` file and rename it `.env`

1. Supply it with credentials. These will be used both when setting up the
   database and when accessing it.
2. Specify where you placed the dataset `DATASET_PATH` should point to the parent directory of `/000`, `/001` etc.

## Running queries

The data insertion and queries can be run with `main.py`.
Use `py main.py --help` for more detailed instructions.

## Code style

The project is set up to use _Black_ for automatic formatting.
Either set up your IDE to use this automatically, or run Black manually with `black`.
