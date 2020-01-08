import logging
import os
import shutil
import sys
from collections import defaultdict
import subprocess

import click
from flask.cli import FlaskGroup, AppGroup

from api.models import *
from fsec import create_app, db
import loggers
from config import get_active_config
from collector import Endpoint, fsecCollector, ZipDownloader
import util

logger = logging.getLogger()

import metrics


CONTEXT_SETTINGS = dict(
    help_option_names=["-h", "--help"], ignore_unknown_options=False
)
STATUS_COLOR_MAP = defaultdict(
    lambda: "white",
    {"success": "green", "error": "red", "timeout": "yellow", "failed": "red"},
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


# @cli.command("urlmap")
# def urlmap():
#     """Prints out all routes"""
#     click.echo("{:50s} {:40s} {}".format("Endpoint", "Methods", "Route"))
#     for route in app.url_map.iter_rules():
#         methods = ",".join(route.methods)
#         click.echo("{:50s} {:40s} {}".format(route.endpoint, methods, route))


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
    help=f"Use previously downloaded files (must be located at {conf.COLLECTOR_DOWNLOAD_PATH})",
    is_flag=True,
)
def collector(update_on_conflict, ignore_on_conflict, use_existing):
    "Run a one-off task to synchronize from the fsec data source"
    logger.info(conf)

    endpoints = Endpoint.load_from_config(conf)
    coll = fsecCollector(endpoints["registry"])
    url = util.urljoin(conf.COLLECTOR_BASE_URL, conf.COLLECTOR_URL_PATH)
    if not use_existing:
        downloader = ZipDownloader(url)
        req = downloader.get()
        filelist = downloader.unpack(req).paths
    else:
        downloader = ZipDownloader.from_existing()
        filelist = downloader.paths

    coll.collect(filelist, update_on_conflict, ignore_on_conflict)


@run_cli.command(context_settings=dict(ignore_unknown_options=True))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def web(args):
    cmd = ["gunicorn", "wsgi",] + list(args)
    subprocess.call(cmd)


@cli.command()
def endpoints():
    tpl = "{name:>25} {value:<50}\n"
    for name, ep in load_from_config(conf).items():
        click.secho(tpl.format(name=f"{name}:", value=str(ep)))


@cli.command()
def recreate_db():
    db.drop_all()
    db.create_all()
    db.session.commit()


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


# @cli.command()
# def test():
#     """Runs the tests without code coverage"""
#     tests = unittest.TestLoader().discover('project/tests', pattern='test*.py')
#     result = unittest.TextTestRunner(verbosity=2).run(tests)
#     if result.wasSuccessful():
#         return 0
#     sys.exit(result)


# @cli.command()
# def cov():
#     """Runs the unit tests with coverage."""
#     tests = unittest.TestLoader().discover('project/tests')
#     result = unittest.TextTestRunner(verbosity=2).run(tests)
#     if result.wasSuccessful():
#         COV.stop()
#         COV.save()
#         print('Coverage Summary:')
#         COV.report()
#         COV.html_report()
#         COV.erase()
#         return 0
#     sys.exit(result)
cli.add_command(run_cli)

if __name__ == "__main__":
    cli()
