# Analyze the NFL with SQL and Python

This repository serves as a local data platform for working with NFL data. It encompasses the following key functionalities:

- **Data Ingestion**: Fetches data from [nflverse releases](https://github.com/nflverse/nflverse-data/releases).
- **Historical Data Joining**: Combines the last 10 years of nflverse pbp datasets into a single parquet file.
- **Seed Datset Joining**: Performs joins with other nflverse seed datasets to enchance dataset for analysis
- **SQL Analytics Pipeline**: Executes a series of SQL transformations to calculate metrics and stats.

Use this repo to design your own analytics pipeline on top of NFL play-by-play data.

All data is sourced from the great team at [nflverse](https://github.com/nflverse).
 
# Project Setup Guide

This project assumes you are using a code IDE, either locally such as with [VSCode](https://code.visualstudio.com/docs/setup/setup-overview) or with [Github Codespaces](
https://docs.github.com/en/codespaces/getting-started/quickstart). Codespaces can be run by first making a free Github account, clicking the green **Code** button at the top of this repo, and then selecting **Codespaces**.

If coding locally, [this guide](https://github.com/theopendatastackofficial/get-started-coding-locally) will help you get setup with VSCode(free code interface), WSL(for windows users), uv(for python) bun(for js/ts) and git/Github(for saving and sharing)

## 1. Install uv for python code

**uv** is an extremely fast Python package manager that simplifies creating and managing Python projects. We’ll install it first to ensure our Python environment is ready to go. uv makes working with python both faster and simpler.

### Install uv

In VSCode, at the top of the screen, you will see an option that says **Terminal**. Then, you should click the button, and then select **New Terminal**. Then, copy and paste the following commands to install uv. You can double check they are the correct scripts by going to the official uv site, maintained by the company astral.

[Install uv](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer)

```macOS, WSL, and Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

If you are using Windows and also not using WSL.

Windows Powershell:
```sh
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Test your install by running in the terminal:
```sh
uv
```

#### Expected output:
If uv was installed correctly, you should see a help message that starts with:

```sh
An extremely fast Python package manager.

Usage: uv [OPTIONS] <COMMAND>

Commands:
  run      Run a command or script
  init     Create a new project
  ...
  help     Display documentation for a command
```

#### If you get an error:
- Copy and paste the exact error message into ChatGPT and explain you’re having problems using uv.
- ChatGPT can help troubleshoot your specific error message.

If uv displays its usage information without an error, congratulations! You’re all set to work with Python in your local environment.

## 2. Clone the Repository

Open VScode. On the top of the screen, click **Terminal**->**New Terminal**
Then, copy and paste the following command into the terminal to git clone the repo:

```bash
git clone https://github.com/ChristianCasazza/nfl-analytics-pipeline 
```

In VSCode, on the left side of the screen, there will be a vertical bar, and the icon with two pieces of paper will be called **Explorer**. By clicking on this, you can click **open folder** and it will be called nfl-analytics-pipeline.

## 3. Setup the Project


This repository includes two setup scripts:
- **`setup.sh`**: For Linux/macOS
- **`setup.bat`**: For Windows

These scripts automate the following tasks:
1. Create and activate a virtual environment using `uv`.
2. Install project dependencies.
3. Creates a `.env` and appends `WAREHOUSE_PATH`(to tell DBT where your DuckDB file is) and `DAGSTER_HOME`(for your logs) to the .env file.  
4. Starts the Dagster development server.

If you do

### Run the Setup Script

In your VSCode window, click **Terminal** at the top, and then **New Terminal**. Then, copy and paste the following command: 

#### On Linux/macOS:
```bash
./setup.sh
```

If you encounter a `Permission denied` error, ensure the script is executable by running:
```bash
chmod +x setup.sh
```

#### On Windows:
```cmd
setup.bat
```

If PowerShell does not recognize the script, ensure you're in the correct directory and use `.\` before the script name:
```powershell
.\setup.bat
```

The script will guide you through the setup interactively. Once complete, your `.env` file will be configured, and the Dagster server will be running.

If you encounter an error with the scripts(typcally because of an older MacOS ), drop the error and the script into ChatGPT and have it debug it for you.

## 4. Access Dagster

After the setup script finishes, you can access the Dagster web UI. The script will display a URL in the terminal. Click on the URL or paste it into your browser to access the Dagster interface.

## 5. Materialize Assets

1. In the Dagster web UI, click on the **Assets** tab in the top-left corner.
2. Then, in the top-right corner, click on **View Global Asset Lineage**.
3. In the top-right corner, click **Materialize All** to start downloading and processing all of the data.

This will execute the data pipeline, and the 2024 and last 10 years datasets will be at your local path data/opendata/clean/nfl_pbp_2024 and data/opendata/clean/nfl_pbp_ten_years. The DuckDB file that contains the outputs of the SQL analytics pipeline is at data/duckdb/data.duckdb


# Querying the data for Ad-hoc analysis

## Querying the data with the Harlequin SQL Editor

### Step 1: Activating Harlequin

Harlequin is a terminal based local SQL editor.

To start it, open a new terminal, then, run the following command to use the Harlequin SQL editor with uv:


```bash
uvx harlequin
```
To open up the existing duckdb file, use the command 

```bash
harlequin data/duckdb/data.duckdb
```

### Step 2: Query the Data

The duckdb file will already have the views to the tables to query. it can be queried like

```sql
SELECT 
    COUNT(*) AS total_rows from 
FROM nfl_pbp_2024
```

## Working in a notebook

### Overview

The `DuckDBWrapper` class provides a simple interface to interact with DuckDB, allowing you to register data files (Parquet, CSV, JSON), execute queries, and export results in multiple formats. it also allows for quickly testing a sql analytics pipeline and converting the table names in your queries to the required DBT syntax. The `PolarsWrapper`offers a similar interface, but using the polars dataframe interface. Polars has a similar to interface to pandas, but is much faster and more memory efficient.

---


