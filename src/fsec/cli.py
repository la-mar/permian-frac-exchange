import os
import sys
import click
from stringprocessor import StringProcessor as sp
import loggers

import app
from yammler import DownloadLog
from settings import (
    FSEC_DOWNLOAD_DIR,
    EXCLUDE_TOKENS,
    FSEC_FTP_URL,
    FSEC_OUTPUT_DIR,
    FSEC_TO_CSV,
    FSEC_TO_DATABASE,
    DATABASE_URI,
    FSEC_FRAC_SCHEDULE_TABLE,
    DOWNLOADLOGPATH,
)
from util import tokenize


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

OP_COLOR = "red"


def set_verbosity(level: int):
    if level is not None:
        loggers.standard_config(verbosity=level)


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass


@click.command()
@click.option(
    "--download_to",
    "-d",
    default=os.path.abspath(FSEC_DOWNLOAD_DIR or "") or os.path.abspath("./data"),
    show_default=True,
    help=f"Directory to save downloads from the FTP ({FSEC_FTP_URL}). If no location is specified here, the file will be downloaded to the value of the FSEC_DOWNLOAD_DIR environment variable. If that also doesn't exists, the schedule will be downloaded to the ./data directory in the project root.",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    envvar="FSEC_DOWNLOAD_DIR",
)
@click.option(
    "--output_to",
    "-out",
    default=os.path.abspath(FSEC_OUTPUT_DIR or "") or os.path.abspath("./output"),
    show_default=True,
    help=f"Directory to save the parsed output schedule. If no location is specified here, the output will be saved to the location specified in the FSEC_OUTPUT_DIR environment variable. If that also doesn't exists, the schedule will be saved to the ./output directory in the project root.",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    envvar="FSEC_OUTPUT_DIR",
)
@click.option(
    "--no-download",
    is_flag=True,
    show_default=True,
    help=f"Skips downloading any new files before initiating the parsing process. If enabled, only existing files will be parsed.",
)
@click.option("--no-parse", is_flag=True, help=f"Skips parsing the frac schedules.")
@click.option(
    "--no-cleanup",
    is_flag=True,
    show_default=True,
    help=f"Disables the removal of previously downloaded frac schedules.",
)
@click.option(
    "--verbose",
    "-v",
    help="Set the verbosity level. A higher verbosity level will generate more output during the process' execution. Repeat the flag to increase the verbosity level. (ex. -vvv)",
    show_default=True,
    count=True,
)
@click.pass_context
def run(ctx, download_to, no_download, no_parse, no_cleanup, verbose):
    """Run the app"""
    set_verbosity(verbose)

    app.run(to, no_cleanup=no_cleanup, no_download=no_download, no_parse=no_parse)


@click.command()
@click.option(
    "--path",
    "-p",
    default=os.path.abspath(FSEC_DOWNLOAD_DIR or "") or os.path.abspath("./data"),
    show_default=True,
    help="Specify an alternate filepath to the directory containing the frac schedules.",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    envvar=("FSEC_DOWNLOAD_DIR"),
)
@click.option(
    "--verbose",
    "-v",
    help="Set the verbosity level. A higher verbosity level will generate more output during the process' execution. Repeat the flag to increase the verbosity level. (ex. -vvv)",
    show_default=True,
    count=True,
)
def show(path, verbose):
    "List the currently downloaded frac schedules"
    click.secho(
        "\n" + f"Current download directory: {os.path.abspath(path)}",
        bold=True,
        fg="cyan",
    )
    paths = app.list_downloads(os.path.abspath(path))
    op_paths = app.operator_filenames(paths)
    click.secho(
        "\n"
        + f"{'operator alias':<25} {'original name':<25} {'conf':<10} {'filename':<75}",
        bold=True,
    )
    click.secho("-" * 125 + "\n", bold=True)
    for idx, x in enumerate(op_paths):
        path, op = x
        p = os.path.basename(path)

        click.secho(
            f"{op.alias:<25} {op.orig:<25} {op.pscore:<10} {p:<75}",
            dim=True if idx % 2 else False,
        )

    click.secho("\n" + "-" * 125 + "\n", bold=True)


@click.command()
@click.option(
    "--to",
    "-t",
    default=os.path.abspath(FSEC_DOWNLOAD_DIR or "") or os.path.abspath("./data"),
    show_default=True,
    help=f"Directory to save downloads from the FTP ({FSEC_FTP_URL}). If no location is specified here, the file will be downloaded to the value of the FSEC_DOWNLOAD_DIR environment variable. If that also doesn't exists, the schedule will be downloaded to the ./data directory in the project root.",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    envvar="FSEC_DOWNLOAD_DIR",
)
@click.option(
    "--verbose",
    "-v",
    help="Set the verbosity level. A higher verbosity level will generate more output during the process' execution. Repeat the flag to increase the verbosity level. (ex. -vvv)",
    show_default=True,
    count=True,
)
def download(to, verbose):
    "Download all frac schedules"

    set_verbosity(verbose)
    click.echo(
        click.style(
            "\n" + f"Downloading frac schedules to: {os.path.abspath(to)}",
            fg="cyan",
            bold=True,
        )
    )

    with DownloadLog.context(DOWNLOADLOGPATH) as dlog:
        for path, filename, status in app.download(to):
            click.secho(
                f"{filename:<60} {status:<25}",
                fg="green" if status == "success" else "red",
            )
            dlog.add(os.path.abspath(os.path.join(path, filename)))


@click.command()
@click.option(
    "--operator",
    "-o",
    help="Attempt to locate and parse an operator's latest frac schedule",
)
@click.option(
    "--filename",
    "-f",
    help="Parse a specific frac schedule",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
)
@click.option(
    "--directory",
    "-d",
    default=os.path.abspath(FSEC_DOWNLOAD_DIR or "") or os.path.abspath("./data"),
    show_default=True,
    help="Parse all frac schedules in a directory. The directory should ONLY include original frac schedules.",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    envvar="FSEC_DOWNLOAD_DIR",
)
@click.option(
    "--to",
    "-t",
    default=os.path.abspath(FSEC_OUTPUT_DIR or "") or os.path.abspath("./output"),
    show_default=True,
    help=f"Directory to save the parsed output schedule. If no location is specified here, the output will be saved to the location specified in the FSEC_OUTPUT_DIR environment variable.",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    envvar="FSEC_OUTPUT_DIR",
)
@click.option(
    "--merge",
    "-m",
    is_flag=True,
    default=True,
    help=f"Option to merge the parsed output into a single file.",
    show_default=True,
    envvar="FSEC_AUTO_MERGE",
)
@click.option(
    "--csv",
    "-c",
    is_flag=True,
    default=True,
    help=f"Option to save parsed output as a csv file",
    show_default=True,
    envvar="FSEC_TO_CSV",
)
@click.option(
    "--db",
    is_flag=True,
    default=FSEC_TO_DATABASE or False,
    help=f"Option to save the parsed output to the configured database.  The currently configured database is: {DATABASE_URI.split('@')[-1]}. The frac schedule table is set to {FSEC_FRAC_SCHEDULE_TABLE}",
    show_default=True,
    envvar="FSEC_TO_DATABASE",
)
@click.option(
    "--verbose",
    "-v",
    help="Set the verbosity level. A higher verbosity level will generate more output during the process' execution. Repeat the flag to increase the verbosity level. (ex. -vvv)",
    show_default=True,
    count=True,
)
@click.pass_context
def parse(ctx, to, operator, filename, directory, merge, csv, db, verbose):
    "Parse a frac schedule"

    set_verbosity(verbose)
    from operator_index import Operator
    import pandas as pd

    click.echo("")  # spacer
    path = None
    to_parse = []
    try:
        basepath = os.path.abspath(directory or FSEC_DOWNLOAD_DIR)
        paths: list = app.list_downloads(basepath)
        paths_with_ops: list = app.operator_filenames(paths, with_index=False)
    except Exception:
        raise Exception("Unable to load paths")

    if operator is not None:
        op = Operator(operator)
        # search in paths for resemblence to passed operator name
        to_parse = [f for f in paths_with_ops if f[1].alias == op.alias]

    elif filename is not None:
        # search in paths based on matching the given filename
        filename = os.path.basename(filename)
        to_parse = [f for f in paths_with_ops if os.path.basename(f[0]) == filename]

    elif directory is not None:
        # parse 'em all
        to_parse = paths_with_ops

    if len(to_parse) > 0:
        n = len(to_parse)
        click.secho(f"Found {n} schedule{'s' if n > 1 else ''} to parse", fg="green")
        click.secho("\n" + "-" * 125 + "\n", bold=True, dim=True)

    else:
        msg = f"Could not find any frac schedules matching the given criteria :("
        click.secho("\n" + msg, bold=True, fg="red")
        ctx.invoke(show, path=basepath)

    # print("\n".join([x[0] for x in to_parse]))
    # return 0
    # click.secho("\n" + f"parsing {n} schedules...")
    parsers = []
    to_save = []

    click.secho()  # spacer
    for p, op in to_parse:
        path = os.path.dirname(p)
        basename = os.path.basename(p)

        pl = click.style("(", dim=True)
        pr = click.style(")", dim=True)
        fs = click.style("/", dim=True)
        # s = click.style(f"{spath} {p1}{sop_a}{fs}{sop_n}{p2}", dim=True)

        try:
            parser = app.parse(filepath=p)

            color = "red"
            msgs = []
            dim = True

            if parser.status == "ok":
                color = "green"
                bold = False
            elif parser.status == "warning":
                color = "yellow"
                msgs = parser.status_messages["warning"]
                dim = False
                bold = True
            elif parser.status == "error":
                color = "red"
                msgs = parser.status_messages["error"]
                dim = False
                bold = True
            else:
                pass

            s_basename = click.style(
                basename,
                dim=True if parser.status == "ok" else False,
                bold=False if parser.status == "ok" else True,
                fg="white" if parser.status == "ok" else color,
            )
            s_status = click.style(
                f"{op.name:<20} {parser.status:<10}", fg=color, bold=bold
            )
            s_n_records = click.style(f"{len(parser.df)} records", dim=True)

            click.echo(f"{s_status} {s_basename} {pl}{s_n_records}{pr}")
            # print(["messages"] + msgs)
            if len(msgs) > 0:

                for m in msgs:
                    click.secho("\t" + m, bold=True, fg=color)
            # for m in msgs:
            #     for cats in m:
            #         for c in cats:
            #             click.secho(c, fg=color, bold=False)

            parsers.append(parser)

            if not merge:
                to_save.append((op.alias, p.df, None))

        except:
            s_basename = f"{click.style(basename, dim=False, bold=True, fg=color)}"
            s_status = click.style(
                f"{op.name:<20} {parser.status:<10}", fg=color, bold=True
            )
            s_n_records = click.style(f"{len(parser.df)} records", dim=True)

            click.secho(f"{s_status} {s_basename} {pl}{s_n_records}{pr}")

    click.secho("\n" + "-" * 125 + "\n", bold=True, dim=True)

    if merge:
        df = pd.concat([p.df for p in parsers], sort=False)
        to_save.append(("Combined frac schedule saved to:", df, "frac-schedules"))

    nframes = len(to_save)

    if db:
        # click.secho(f"Saving {nframes} schedule{'s' if nframes > 1 else ''}")

        for msg, frame, _ in to_save:
            try:
                n = frame.shape[0]
                loc = app.to_db(frame)
                click.secho(f"{msg} -> {loc} ({n} records)", bold=True)
            except Exception as e:
                click.secho(
                    f"{msg} -> Failed writing to database: {e}", fg="red", bold=True
                )
                csv = True
                click.secho(f"{msg} -> Trying csv instead...", fg="yellow")

    if csv:
        # click.secho(f"Saving {nframes} csv file{'s' if nframes > 1 else ''}")

        for msg, frame, prefix in to_save:
            s_n = click.style(f"({frame.shape[0]} records)", dim=True)
            s_loc = click.style(
                os.path.basename(app.to_csv(frame, to, prefix)), bold=True, fg="cyan"
            )
            try:
                if n > 0:
                    click.secho(f"{msg} -> {s_loc} {s_n}" + "\n\n")
            except Exception as e:
                click.secho(f"{msg} -> Failed writing to csv: {e}" + "\n\n", fg="red")


@click.command()
@click.option("--simulate", "-s", is_flag=True, default=False, show_default=True)
@click.option("--force", "-f", is_flag=True, default=False, show_default=True)
def cleanup(simulate, force):
    to_be_cleaned = os.path.abspath(FSEC_DOWNLOAD_DIR)
    ct = 0
    click.echo()  # spacer
    for fname, status, status_message in app.remove_previous_downloads(
        to_be_cleaned, simulate
    ):

        if status == "success":
            color = "green"
        elif status == "warning":
            color = "yellow"
        elif status == "failed":
            color = "red"
        else:
            color = "white"

        click.secho(f"{fname:<60} {status_message}", fg=color, bold=False)
        ct += 1
    click.secho(
        "\n" + f"Deleted {ct} frac schedules from {to_be_cleaned}" + "\n",
        fg="cyan",
        bold=True,
    )

    remaining = os.listdir(to_be_cleaned)
    n = len(remaining)

    if n > 0:
        click.secho(
            f"{n} lingering file{'s' if n > 0 else ''} in {to_be_cleaned}" + "\n",
            dim=False if n > 0 else True,
            fg="yellow" if n > 0 else "white",
            bold=True if n > 0 else False,
        )

        for f in remaining:
            click.secho("\t" + f"{f}", bold=False, dim=True)

        click.secho(
            "\n\trun 'fsec cleanup --force' to cleanup the remaining files\n\n"
            if n > 0
            else "",
            dim=False,
            fg="yellow",
            bold=True,
        )


# Error
# cli.add_command(initdb)
# cli.add_command(dropdb)
cli.add_command(run)
cli.add_command(download)
cli.add_command(parse)
cli.add_command(show)
cli.add_command(cleanup)


def main(argv=sys.argv):
    """
    Args:
        argv (list): List of arguments
    Returns:
        int: A return code
    Does stuff.
    """
    # print(argv)
    cli()
    return 0
