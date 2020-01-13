import logging
import os
import shutil
import sys

import click
from flask.cli import AppGroup, FlaskGroup
import sqlalchemy

from collector import BytesFileHandler, Endpoint, FracScheduleCollector, Ftp
from config import get_active_config
from fracx import create_app


logger = logging.getLogger()


CONTEXT_SETTINGS = dict(
    help_option_names=["-h", "--help"], ignore_unknown_options=False
)

conf = get_active_config()

app = create_app()
cli = FlaskGroup(create_app=create_app, context_settings=CONTEXT_SETTINGS)
run_cli = AppGroup("run")
db_cli = AppGroup("db")
test_cli = AppGroup("test")


def get_terminal_columns():
    return shutil.get_terminal_size().columns


def hr():
    return "-" * get_terminal_columns()


@run_cli.command()
@click.option(
    "update_on_conflict",
    "--update-on-conflict",
    "-u",
    help="Prevent updating records that already exist",
    show_default=True,
    default=True,
)
@click.option(
    "ignore_on_conflict",
    "--ignore-conflict",
    "-i",
    help="Ignore records that already exist",
    show_default=True,
    is_flag=True,
)
@click.option(
    "use_existing",
    "--use-existing",
    "-e",
    help=f"Use a previously downloaded file",
    is_flag=True,
)
def collector(update_on_conflict, ignore_on_conflict, use_existing):
    "Run a one-off task to synchronize from the fracx data source"

    logger.info(conf)

    endpoint = Endpoint.load_from_config(conf)["frac_schedules"]
    collector = FracScheduleCollector(endpoint)

    ftp = Ftp.from_config()
    latest = ftp.get_latest()

    rows = BytesFileHandler.xlsx(
        latest.get("content"), date_columns=endpoint.mappings.get("dates")
    )
    collector.collect(rows, update_on_conflict, ignore_on_conflict)

    ftp.cleanup()


@cli.command()
def endpoints():
    from collector import Endpoint

    for name, ep in Endpoint.load_from_config(conf).items():
        click.secho(name)


@cli.command()
def show():
    import yaml

    yaml.safe_dump(conf.show, sys.stdout)


@db_cli.command()
def init(c=None):
    c = c or conf
    db_url = c.SQLALCHEMY_DATABASE_URI
    basepath = os.path.dirname(__file__)
    if c.DATABASE_DRIVER == "postgres":
        filename = f"{c.CONFIG_BASEPATH}/init.pgsql"
    elif "pymssql" in c.DATABASE_DRIVER:
        filename = f"{c.CONFIG_BASEPATH}/init.sql"
    else:
        raise Exception("Could not determine database provider")

    with open(os.path.join(basepath, filename)) as f:
        contents = f.read()

    engine = sqlalchemy.create_engine(db_url)

    contents = contents.format(
        TABLE_NAME=c.FRAC_SCHEDULE_TABLE_NAME, DATABASE_SCHEMA=c.DATABASE_SCHEMA
    )
    stmts = [sqlalchemy.text(x) for x in contents.split("--")]  # escaped text

    try:
        for s in stmts:
            engine.execute(s)
        logger.info("db init succeeded")
    except Exception as e:
        logger.error(f"db init failed -- {e}")


@db_cli.command()
@click.pass_context
def recreate(ctx):

    db_url = conf.SQLALCHEMY_DATABASE_URI
    engine = sqlalchemy.create_engine(db_url)
    engine.execute(
        f"drop view if exists {conf.FRAC_SCHEDULE_TABLE_NAME}_most_recent_by_api10;"
    )
    engine.execute(f"drop table if exists {conf.FRAC_SCHEDULE_TABLE_NAME};")
    ctx.invoke(init)


def main(argv=sys.argv):
    """
    Args:
        argv (list): List of arguments
    Returns:
        int: A return code
    Does stuff.
    """

    cli()
    return 0


cli.add_command(run_cli)
cli.add_command(db_cli)

if __name__ == "__main__":
    cli()
