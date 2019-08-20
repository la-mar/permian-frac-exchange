import os
import sys
import click
from operator_index import Operator
from stringprocessor import StringProcessor as sp
import app
from settings import DATAPATH, EXCLUDE_TOKENS
from util import tokenize


@click.group()
def cli():
    pass


@click.command()
@click.option("--no-download", is_flag=True)
@click.option("--no-parse", is_flag=True)
def run(no_download, no_parse):
    """Simple program that greets NAME for a total of COUNT times."""
    click.echo(f"no-download = {no_download}")


@click.command()
@click.option(
    "--to", "-t", default="./data", help="Directory to save downloads from the FTP"
)
def download(to):
    click.echo(click.style(f"to = {to}", fg="red", bold=True))


@click.command()
@click.option(
    "--operator",
    "-o",
    help="Attempt to locate and parse an operator's latest frac schedule",
)
@click.option(
    "--filename",
    help="Parse a specific frac schedule",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
)
def parse(operator, filename):
    click.echo(click.style(f"filename = {filename}", fg="red", bold=True))
    click.echo(click.style(f"operator = {operator}", fg="red", bold=True))

    path = None
    try:
        paths: list = app.list_downloads(os.path.abspath(DATAPATH))
    except Exception:
        raise Exception("Unable to load paths")
    if operator is not None:
        op = Operator(operator)

        # search in paths for resemblence to passed operator name
        matches = []
        for idx, path in enumerate(paths):
            tokens: list = tokenize(
                sp.normalize(os.path.basename(path), lower=True), sep="_"
            )
            tokens = [t for t in tokens if t not in EXCLUDE_TOKENS]
            if len(tokens):
                token = tokens[0]
            else:
                token = None

            matches.append((Operator(token), idx))

        matches = [f for f in matches if f[0].alias == op.alias]

        if len(matches) > 0:
            mop, fidx = matches[0]
            path = paths.pop(fidx)
            print(f"Found file for {operator}: {path}")
        else:
            print(f"Could not find a frac schedule for operator: {operator}")
            print(f"Current download directory: {os.path.dirname(filename)}")
            print("\nFiles in download directory:\n")
            for p in paths:
                print("{p:>15}".format(p=os.path.basename(p)))
            return None

    elif filename is not None:
        path = filename

    if path is not None:
        print(f"parsing file: {os.path.basename(path)} ({op.name}/{op.alias})")
        app.parse(path)

        # TODO: Show summary


# Error
# cli.add_command(initdb)
# cli.add_command(dropdb)
cli.add_command(run)
cli.add_command(download)
cli.add_command(parse)


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
