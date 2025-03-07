# Paramilitary Leaks

Exploring the [Paramilitary Leaks](https://ddosecrets.com/article/paramilitary-leaks) dataset, 200 GB of chat logs from AP III and the Oath Keepers.

Telegram HTML export to .csv tool forked from <https://github.com/gabekanegae/telegram-export-converter/tree/master>

## Getting started

`tasks` is a Python project for data journalism tasks on the Paramilitary Leaks dataset. It processes Telegram HTML exports with full provenance tracking:

1. **Find**: Recursively locates all message*.html files
2. **Convert**: Transforms HTML to CSV (saved alongside originals + copied to central directory)
3. **Database**: Creates a SQLite database with comprehensive source attribution
4. **Analytics**: Generates optimized formats for data analysis:
   - CSV exports of all database tables
   - Parquet files for high-performance analytics
   - Partitioned datasets optimized by date, chat, and sender
   - Summary statistics for quick insights
5. **Provenance**: Generates detailed manifests including:
   - JSONL manifest with complete processing metadata
   - README documenting the data pipeline
   - Full source path tracking in database tables and output files

The project also includes tools for analyzing media assets:

1. **Media Indexing**: Finds and catalogs all media files (images, videos, audio, documents)
2. **Metadata Extraction**: Extracts technical metadata (dimensions, file sizes, etc.)
3. **AI Descriptions**: Generates natural language descriptions of images using Gemini (via OpenRouter)
4. **Provenance Linking**: Connects media files to their original chat messages
5. **Searchable Database**: Creates a SQLite database of all media with full-text search

After processing, you can explore the data through multiple pathways:
- Use `datasette` for interactive database queries with full provenance tracking
- Load the CSV files in spreadsheet tools for simple analysis
- Use the Parquet files in data visualization tools for high-performance interactive dashboards
- Search media content through AI-generated descriptions

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
  media             Commands for working with media files (images, videos, etc.)
```

```
$ poetry run tasks build-telegram-db --help
Usage: tasks build-telegram-db [OPTIONS] DATASET_PATH OUTPUT_PATH

  Build a SQLite3 database of Telegram chats

Options:
  --help  Show this message and exit.
```

## Media Processing Commands

The media processing commands are separate from the Telegram processing. They allow you to index and analyze media files (images, videos, documents) from the dataset.

```
$ poetry run tasks media --help
Usage: tasks media [OPTIONS] COMMAND [ARGS]...

  Commands for working with media files (images, videos, etc.)

Options:
  --help  Show this message and exit.

Commands:
  index     Index all media files without generating AI descriptions
  describe  Generate AI descriptions for media files using OpenRouter
```

### Basic Media Indexing

```
$ poetry run tasks media index DATASET_PATH OUTPUT_PATH [OPTIONS]
```

This command indexes all media files, extracts metadata, and creates a searchable database. It doesn't require any external APIs or authentication.

**Options:**
```
--telegram-db FILE        Path to Telegram database for finding related messages
--max-files INTEGER       Maximum number of files to process (for testing)
--batch-size INTEGER      Number of files to process in each batch
```

**Example Usage:**
```bash
# Basic indexing of all media in the dataset
poetry run tasks media index /path/to/Paramilitary_Leaks /path/to/output

# Index with a limit (for testing)
poetry run tasks media index /path/to/Paramilitary_Leaks /path/to/output --max-files 100

# Index with connection to Telegram database for provenance tracking
poetry run tasks media index /path/to/Paramilitary_Leaks /path/to/output --telegram-db /path/to/telegram_chats.db
```

### AI Descriptions with Gemini (via OpenRouter)

```
$ poetry run tasks media describe DATASET_PATH OUTPUT_PATH [OPTIONS]
```

This command generates AI descriptions of media files (currently images) using Google's Gemini via OpenRouter. Each image is analyzed with a structured format including visual content, text transcription, classification, and investigative notes.

**Options:**
```
--telegram-db FILE          Path to Telegram database for finding related messages
--model TEXT                Model to use for descriptions (default: google/gemini-flash-1.5-8b)
--max-files INTEGER         Maximum number of files to process (for testing)
--batch-size INTEGER        Number of files to process in each batch
--media-type [image|all]    Type of media to generate descriptions for
--save-txt / --no-save-txt  Save descriptions as .txt files alongside original media
```

**Setup:**
1. Create a `.env` file in the tasks directory with your OpenRouter API key:
   ```
   OPENROUTER_API_KEY=your_key_here
   OPENROUTER_MODEL=google/gemini-flash-1.5-8b  # Optional, to override default model
   ```

2. Install dependencies if needed:
   ```
   pip install python-dotenv openai pillow requests tqdm
   ```

**Example Usage:**
```bash
# Process all images in dataset (will take time with large datasets)
poetry run tasks media describe /path/to/Paramilitary_Leaks /path/to/output

# Process a limited number of files (good for testing)
poetry run tasks media describe /path/to/Paramilitary_Leaks /path/to/output --max-files 10

# Use a different model
poetry run tasks media describe /path/to/Paramilitary_Leaks /path/to/output --model "anthropic/claude-3-haiku-vision:latest"

# Don't save descriptions as separate .txt files
poetry run tasks media describe /path/to/Paramilitary_Leaks /path/to/output --no-save-txt
```

**Output:**
- Creates a SQLite database with all media metadata and descriptions
- Saves descriptions as .txt files alongside original media files
- Generates CSV and Parquet versions of the data
- Creates detailed manifest and README

**Note:** For large datasets, start with a small `--max-files` value to test. The default batch size (5) is optimized for API rate limits.

### Searching and Analyzing Media

After processing, you can use Datasette to search and analyze the media:

```bash
# Install Datasette if needed
pip install datasette

# Start Datasette to explore the data
datasette /path/to/output/media_assets.db
```

Example queries:
- Find images containing specific text: `SELECT * FROM media_assets WHERE description LIKE '%weapon%'`
- Filter by classification: `SELECT * FROM media_assets WHERE description LIKE '%CLASSIFICATION: Weapon/Tactical%'`
- Find images from specific chats: `SELECT * FROM media_assets WHERE source_chat LIKE '%leaders%'`

## Blog posts

- March 5, 2025: [Exploring the Paramilitary Leaks](https://micahflee.com/exploring-the-paramilitary-leaks/)
