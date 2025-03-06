import click

from . import telegram


@click.group()
def cli():
    """Crunching data in the Paramilitary Leaks datasets"""
    pass


@cli.command()
@click.argument("dataset_path")
@click.argument("output_path")
def build_telegram_db(dataset_path, output_path):
    """
    Build a SQLite3 database of Telegram chats
    """
    telegram.build(dataset_path, output_path)


if __name__ == "__main__":
    cli()
