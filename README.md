# permian-frac-exchange

<div style="text-align:center;">
  <table >
    <tr>
      <a href="https://codecov.io/gh/la-mar/permian-frac-exchange">
        <img src="https://codecov.io/gh/la-mar/permian-frac-exchange/branch/master/graph/badge.svg" />
      </a>
      <a href="(https://circleci.com/gh/la-mar/permian-frac-exchange">
        <img src="https://circleci.com/gh/la-mar/permian-frac-exchange.svg?style=svg" />
      </a>
    </tr>
  </table>
</div>

This project is used to interface with [PDSEnergy's](https://pdsenergy.com/frac-interference-exchange/) FracX platform to programmatically submit and download frac schedules.

Currently, the project interfaces with the FTP import/export service provided by PDS. As other means of integration become available from PDS, those means will be incorporated into this project as additional ways to import and export data from FracX.

<br/>

## Getting Started

1. Initialize the destination table in the database. Example table definitions can be found in [scripts](scripts/)
2. Define the necessary environment variables in the docker-compose.yml file or in a .env file in the project root directory (examples below).
3. Run the container with docker-compose: `docker-compose up`
   <br/>

## Usage

- Run the container using docker-compose: `docker-compose up`

Example output:

<pre>
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                app config: fracx.config.ProductionConfig
                flask app: fracx.manage.py
                flask env: development
                backend: postgres://fracx:***@db:5432/driftwood
                collector: sftp.pdswdx.com
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Starting download from sftp.pdswdx.com...
Download successful (download size: 70.58 KB, download_time: 0.0s)
frac_schedules.core_insert_update_on_conflict: inserted 625 records (1.28s)
</pre>
<br/>

## Configuration

The application can be configured with environment variables that are passed into the container at runtime. Environment variables can either be defined at the system level or in a file named '.env' in the project's root directory.
<br/>

### Saving output to a Postgres database

<br/>

##### Using a .env file

Example configuration with .env file:

.env

```python

FRACX_FTP_USERNAME=YOUR_FRACX_FTP_UERNAME
FRACX_FTP_PASSWORD=YOUR_FRACX_FTP_PASSWORD
DATABASE_USERNAME=YOUR_DATABASE_USERNAME
DATABASE_PASSWORD=YOUR_DATABASE_PASSWORD
DATABASE_HOST=YOUR_DATABASE_HOST
DATABASE_NAME=YOUR_DATABASE_NAME
DATABASE_SCHEMA=fracx
FRAC_SCHEDULE_TABLE_NAME=frac_schedules

```

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
      DATABASE_USERNAME: YOUR_DATABASE_USERNAME
      DATABASE_PASSWORD: YOUR_DATABASE_PASSWORD
      DATABASE_HOST: YOUR_DATABASE_HOST
      DATABASE_NAME: YOUR_DATABASE_NAME
      DATABASE_SCHEMA: fracx
      FRAC_SCHEDULE_TABLE_NAME: frac_schedules
```
