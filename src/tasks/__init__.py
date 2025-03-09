import click

from . import build


@click.group()
def cli():
    """Crunching data in the Paramilitary Leaks datasets"""
    pass


@cli.command()
@click.argument("dataset_path")
@click.option(
    "-a",
    "--absolute-paths",
    is_flag=True,
    help="Store absolute file paths in the database. By default, relative paths are stored.",
)
def build_db(dataset_path, absolute_paths):
    """
    Build a SQLite3 database based on the Paramilitary Leaks dataset
    """
    build.build(dataset_path, absolute_paths)


if __name__ == "__main__":
    cli()
