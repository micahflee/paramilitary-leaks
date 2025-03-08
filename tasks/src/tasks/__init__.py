import click

from . import telegram
from . import files_table


@click.group()
def cli():
    """Crunching data in the Paramilitary Leaks datasets"""
    pass


@cli.command()
@click.argument("dataset_path")
@click.argument("db_path")
@click.option(
    "-a",
    "--absolute-paths",
    is_flag=True,
    help="Store absolute file paths in the database. By default, relative paths are stored.",
)
def build_files_table(dataset_path: str, db_path: str, absolute_paths) -> None:
    "Build a table containing all files in the dataset"
    files_table.build(dataset_path, db_path, absolute_paths)


@cli.command()
@click.argument("dataset_path")
@click.argument("output_path")
@click.option(
    "-a",
    "--absolute-paths",
    is_flag=True,
    help="Store absolute file paths in the database. By default, relative paths are stored.",
)
def build_telegram_db(dataset_path, output_path, absolute_paths):
    """
    Build a SQLite3 database of Telegram chats
    """
    telegram.build(dataset_path, output_path, absolute_paths)


if __name__ == "__main__":
    cli()
