import logging
import os
import shutil
import sys

import click
from flask.cli import FlaskGroup, AppGroup

from fracx import create_app
from config import get_active_config

from collector import Endpoint, FracScheduleCollector, Ftp, BytesFileHandler


logger = logging.getLogger()


CONTEXT_SETTINGS = dict(
    help_option_names=["-h", "--help"], ignore_unknown_options=False
)

conf = get_active_config()
app = create_app()
cli = FlaskGroup(create_app=create_app, context_settings=CONTEXT_SETTINGS)
run_cli = AppGroup("run")
test_cli = AppGroup("test")


def get_terminal_columns():
    return shutil.get_terminal_size().columns


def hr():
    return "-" * get_terminal_columns()


@cli.command()
def ipython_embed():
    """Runs a ipython shell in the app context."""
    try:
        import IPython
    except ImportError:
        click.echo("IPython not found. Install with: 'pip install ipython'")
        return
    from flask.globals import _app_ctx_stack

    app = _app_ctx_stack.top.app
    banner = "Python %s on %s\nIPython: %s\nApp: %s%s\nInstance: %s\n" % (
        sys.version,
        sys.platform,
        IPython.__version__,
        app.import_name,
        app.debug and " [debug]" or "",
        app.instance_path,
    )

    ctx = {}

    # Support the regular Python interpreter startup script if someone
    # is using it.
    startup = os.environ.get("PYTHONSTARTUP")
    if startup and os.path.isfile(startup):
        with open(startup, "r") as f:
            eval(compile(f.read(), startup, "exec"), ctx)

    ctx.update(app.make_shell_context())

    IPython.embed(banner1=banner, user_ns=ctx)


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

if __name__ == "__main__":
    cli()
