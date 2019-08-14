# FSEC Schedule Normalizer

Frac Schedule Exchange Consortium


# Project Description

The purpose of this project is to parse frac schedules submitted to the FSEC FTP
site and aggregate them into a single file. The implementation seeks out the most
common deviations between schedules, aligning formatting and looking for obvious
typos.  Edge cases are bound to arise as bugs are discovered and new schedule
formats are submitted.  If you discover a bug, please submit it as an issue so it
can be investigated.

# Configuration

Requires Python 3.7+

You can choose one of two basic options for configuration:

1) Add a file named ".env" to the project's root directory.  The app will import
   this file at executiong time and use the values it contains to configure the
   FTP connection.

An example .env file might look like the following:

```env
    FSEC_URL="FTP_URL"
    FSEC_USERNAME="YOUR_USERNAME"
    FSEC_PASSWORD="YOUR_PASSWORD"
    FSEC_BASEPATH="FTP_DIRECTORY_PATH"
    FSEC_DESTINATION="./data"
```


2) Update config/config.yaml with your connection details.

```yaml
url: FTP_URL
username: YOUR_USERNAME
password: YOUR_PASSWORD
basepath: FTP_DIRECTORY_PATH
destination: ./data
```

Additionally, advanced settings can be adjusted in src/settings.py.

# Usage

Once configured, the project can be executed by simply calling the main function
on the command line from inside the project's root directory.

```bash
cd /PATH/TO/PROJECT # navigate to the project directory
pipenv shell # start the pipenv shell

python main.py # execute
```

Running main.py will do the following:

1) Remove existing files in the folder specified by the "destination" configuration
   parameter.  This is meant to keep previous downloads from becoming stale and
   continuing to be parsed into the final output.

2) Download all frac schedules currently on the FTP site into
   the folder specified by the "destination" parameter. By default,
   it will download the schedules into the project's data/ folder.

3) Parse each file to normalize formatting and standardize the values.

Standardization methods applied (in order):
 - normalize api numbers to api14 (api_n)
 - identify coordinate resource system
 - cast fracstartdate to datetime
 - cast fracenddate to datetime
 - add column for operator
 - add column for operator_alias
 - validate lat/lon
 - add SHL geometry

Of note, operator names are standardized:

```
Driftwood Energy Operating -> DRIFTWOOD
```

4) Merges the schedules into a single table

5) Saves the merged table as a csv to output/


# Installation on Windows

Installing the geo libraries on Windows can be... a pain.  Pip has a hard time
installing them due to their C dependencies.  As a work-around, you can download
the python wheels from https://www.lfd.uci.edu/~gohlke/pythonlibs/ and install
them with pip directly.

An example powershell script of installing the downloaded wheels can be found
in the scripts/ directory.  Be sure you have activated the pipenv shell before
running the script. Otherwise, it will install the packages into your system
python installation.


# Contributing

If you are interested in contributing, feel free to contact me or submit a pull
request.


# TODOs:

- Add CLI options
- Only remove known files during cleanup
- Document standardization specifics
- More documentation
- API14 and API10 checks
