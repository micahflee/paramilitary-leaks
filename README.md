# Paramilitary Leaks

Exploring the [Paramilitary Leaks](https://ddosecrets.com/article/paramilitary-leaks) dataset, 200 GB of chat logs from AP III and the Oath Keepers.

## Getting started

`tasks` is a Python project for doing data-related tasks with the Paramility Leaks dataset. To begin with, it will be used to build a SQLite3 database of Telegram chats from the database.

You need Python and [Poetry](https://python-poetry.org/) installed.

```sh
# Move to the tasks folder
cd tasks

# Install deps
poetry install

# Run the tasks script
poetry run tasks --help
```

```
$ poetry run tasks --help
Usage: tasks [OPTIONS] COMMAND [ARGS]...

  Crunching data in the Paramilitary Leaks datasets

Options:
  --help  Show this message and exit.

Commands:
  build-telegram-db  Build a SQLite3 database of Telegram chats
```

```
$ poetry run tasks build-telegram-db --help
Usage: tasks build-telegram-db [OPTIONS] DATASET_PATH OUTPUT_PATH

  Build a SQLite3 database of Telegram chats

Options:
  --help  Show this message and exit.
```

## Blog posts

- March 5, 2025: [Exploring the Paramilitary Leaks](https://micahflee.com/exploring-the-paramilitary-leaks/)
