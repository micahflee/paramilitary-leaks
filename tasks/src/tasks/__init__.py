import click

from . import telegram


@click.group()
def cli():
    """Crunching data in the Paramilitary Leaks datasets"""
    pass


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
