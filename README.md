# permian-frac-exchange

<div style="text-align:center;">
  <table >
    <tr>
      <a href="https://pypi.python.org/pypi/fracx/">
        <img src="https://img.shields.io/pypi/pyversions/fracx.svg" />
      </a>
      <a href="https://codecov.io/gh/la-mar/permian-frac-exchange">
        <img src="https://codecov.io/gh/la-mar/permian-frac-exchange/branch/master/graph/badge.svg" />
      </a>
      <a href="https://circleci.com/gh/la-mar/permian-frac-exchange">
        <img src="https://circleci.com/gh/la-mar/permian-frac-exchange.svg?style=svg" />
      </a>
    </tr>

  </table>
</div>

<p>The FracX Python library is a tool to interface with [PDSEnergy's](https://pdsenergy.com/frac-interference-exchange/) FracX platform, used to submit and download frac schedules. Currently, the project interfaces with the FTP import/export service provided by PDS. As other means of integration become available from PDS, those means will be incorporated into this project as additional ways to import and export data from FracX.</p>

Available on PyPI and Docker

<br/>

## Installation

```bash
pipx install fracx
```

   <br/>

## Usage

#### Set environment variables:

```bash

export FRACX_FTP_USERNAME=my_fracx_username
export FRACX_FTP_PASSWORD=my_fracx_password
export FRACX_DATABASE_USERNAME=my_database_username
export FRACX_DATABASE_PASSWORD=my_database_passowrd
export FRACX_DATABASE_HOST=my.host.db
export FRACX_DATABASE_NAME=my_database
export FRACX_DATABASE_SCHEMA=my_schema
export FRACX_TABLE_NAME=frac_schedules

```

####

Initialize the database: `fracx db init`

Initialization creates one table and one view in the target database:

- frac_schedules (or the value of FRACX_TABLE_NAME)
- frac_schedules_by_api10 (or the value of FRACX_TABLE_NAME+"\_by_api10")

Re-running the db init command will attempt to create the table or view if one doesn't exist. It will NOT drop an existing table or view. This must be done manually.

####

Run the app: `fracx run collector`

A successful execution will yield the following output:

<pre>
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                app config: fracx.config.ProductionConfig
                flask app: fracx.manage.py
                flask env: production
                backend: postgres://fracx:***@db:5432/driftwood
                collector: sftp.pdswdx.com
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Starting download from sftp.pdswdx.com...
Download successful (download size: 70.58 KB, download_time: 0.0s)
frac_schedules.core_insert_update_on_conflict: inserted 625 records (1.28s)
</pre>
<br/>

## Configuration Reference

Environment variables reference:

| name                    | default         | description                               |
| ----------------------- | --------------- | ----------------------------------------- |
| FRACX_FTP_URL           | sftp.pdswdx.com | url to the remote ftp server              |
| FRACX_FTP_PORT          | 21              | port for the ftp server connection        |
| FRACX_FTP_INPATH        | /Inbound        | ftp directory where uploads will be saved |
| FRACX_FTP_OUTPATH       | /Outbound       | ftp directory to look for downloads in    |
| FRACX_FTP_USERNAME      | ""              | username for ftp authentication           |
| FRACX_FTP_PASSWORD      | ""              | password for ftp authentication           |
| FRACX_DATABASE_USERNAME | ""              | username for database authentication      |
| FRACX_DATABASE_PASSWORD | ""              | password for database authentication      |
| FRACX_DATABASE_DIALECT  | postgres        | database dialect ("postgres" or "mssql")  |
| FRACX_DATABASE_HOST     | localhost       | database host name                        |
| FRACX_DATABASE_PORT     | 5432            | database port                             |
| FRACX_DATABASE_NAME     | postgres        | database name                             |
| FRACX_TABLE_NAME        | frac_schedules  | database table to store frac schedules    |

<!--
The application can be configured with environment variables that are passed into the container at runtime. Environment variables can either be defined at the system level or in a file named '.env' in the project's root directory.
<br/>

### Saving output to a Postgres database

<br/>

##### Using a .env file

Example configuration with .env file:

.env

When specifying configuration in a .env file, include it in docker-compose so the configuration is passed into the container at runtime.

docker-compose.yml

```yaml
version: "3.7"

services:
  collector:
    image: driftwood/fracx
    env_file: .env
```

<br/>
<br/>
##### Using docker-compose.yml

Example configuration directly in the docker-compose.yml file:

```yaml
services:
  collector:
    image: driftwood/fracx
    environment:
      FRACX_FTP_USERNAME: YOUR_FRACX_FTP_UERNAME
      FRACX_FTP_PASSWORD: YOUR_FRACX_FTP_PASSWORD
      FRACX_DATABASE_USERNAME: YOUR_DATABASE_USERNAME
      FRACX_DATABASE_PASSWORD: YOUR_DATABASE_PASSWORD
      FRACX_DATABASE_HOST: YOUR_DATABASE_HOST
      FRACX_DATABASE_NAME: YOUR_DATABASE_NAME
      FRACX_DATABASE_SCHEMA: fracx
      FRACX_TABLE_NAME: frac_schedules
``` -->
