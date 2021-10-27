# Datawrangling example

This repository is a toy example done for educational purposes,
in order to practice using Python, Docker and SQL.

My goals for the project were:

- Set up a local Postgres database with Docker
- Clean and insert a large dataset into the database
- Query the database to solve specific tasks
- Do all of the above with clean code, reproducible steps and a command-line utility (CLI)

## Virtual environment

Using a virtual environment (venv) is recommended, but not necessary:

1. `py -m venv venv`
2. Activate venv in IDE. Run `pip list` to check that you are in the correct venv, it should only have _pip_ and _setuptools_ installed by default.
3. `pip install -r requirements.txt`

## Setup project

1. Make a copy of the `.env-template` file and rename it `.env`
   - Supply it with credentials. These will be used both when setting up the
     database and when accessing it.
1. Get a copy of the raw dataset:

   - Download data from https://www.microsoft.com/en-us/research/publication/geolife-gps-trajectory-dataset-user-guide/
   - Place the Data folder in the project root folder, or point the environment variable to its location

1. Extract `dataset.zip`, for example to `/exercise2/dataset`
1. Make a copy of `.env.template` and rename it to `.env`. Then supply your credentials and specify where your folders should be.

## Running queries

The data insertion and queries can be run with `main.py`.
Use `py main.py --help` for more detailed instructions.

## Code style

The project is set up to use _Black_ for automatic formatting.
Either set up your IDE to use this automatically, or run Black manually with `black`.
