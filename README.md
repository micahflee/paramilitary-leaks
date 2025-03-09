# Paramilitary Leaks

Exploring the Paramilitary Leaks dataset, 200 GB of chat logs from AP III and the Oath Keepers.

## Getting started

To use this repo, you need to download a local copy of the [Paramilitary Leaks dataset](https://ddosecrets.com/article/paramilitary-leaks).

You can then use the code in this repo to loop through the dataset and build a database of Telegram chat messages, files, etc., which you can then query.

You need Python and [Poetry](https://python-poetry.org/) installed. After cloning this repo, install the dependencies like this:

```sh
poetry install
```

### Building your database

Build a SQLite3 database of the Paramilitary Leaks like this: 

```sh
poetry run tasks build-db [/path/to/dataset]
```

You will end up with `output/data.db`.

### Exploring with [Datasette](https://datasette.io/)

Datasette is a tool for exploring data. After you've built your database, launch it like this:

```sh
poetry run datasette ./output/data.db
```

It should show output like this:

```
$ poetry run datasette ./output/data.db 
INFO:     Started server process [1268148]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8001 (Press CTRL+C to quit)
```

Load http://127.0.0.1:8001 in a browser to access the data.

## Blog posts

- March 5, 2025: [Exploring the Paramilitary Leaks](https://micahflee.com/exploring-the-paramilitary-leaks/)
