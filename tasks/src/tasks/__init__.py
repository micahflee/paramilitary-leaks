import click
import os

from . import telegram
from . import media


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
    
    This command finds all Telegram HTML export files in DATASET_PATH,
    converts them to a structured format, and creates a comprehensive
    SQLite database at OUTPUT_PATH.
    """
    telegram.build(dataset_path, output_path)


# Create a media group for more explicit separation of commands
@cli.group(name="media")
def media_group():
    """
    Commands for working with media files (images, videos, etc.)
    
    These commands analyze and process media files in the dataset.
    They run as separate, explicit operations from the Telegram chat processing.
    """
    pass


@media_group.command(name="index")
@click.argument("dataset_path")
@click.argument("output_path")
@click.option(
    "--telegram-db", 
    help="Path to Telegram database for finding related messages",
    type=click.Path(exists=True, file_okay=True, dir_okay=False)
)
@click.option(
    "--max-files", 
    type=int,
    help="Maximum number of files to process (for testing)"
)
@click.option(
    "--batch-size", 
    type=int,
    default=50,
    help="Number of files to process in each batch"
)
def index_media(
    dataset_path, 
    output_path, 
    telegram_db=None,
    max_files=None,
    batch_size=50
):
    """
    Index all media files without generating AI descriptions
    
    This command finds all media files (images, videos, audio, documents)
    in DATASET_PATH, extracts metadata, and creates a searchable database.
    This is the basic indexing command that doesn't use any external APIs.
    
    It creates a comprehensive SQLite database at OUTPUT_PATH containing
    all media file metadata.
    
    If --telegram-db is provided, it will attempt to link media files to
    their original chat messages for provenance tracking.
    
    Example:
        tasks media index /path/to/dataset /path/to/output \\
            --telegram-db /path/to/telegram_chats.db
    """
    # Run the main build function
    from . import media as media_lib
    media_lib.build_media_db(
        dataset_path=dataset_path,
        output_path=output_path,
        telegram_db_path=telegram_db,
        openrouter_api_key=None,  # No descriptions in basic indexing
        max_files=max_files,
        batch_size=batch_size
    )


@media_group.command(name="describe")
@click.argument("dataset_path")
@click.argument("output_path")
@click.option(
    "--telegram-db", 
    help="Path to Telegram database for finding related messages",
    type=click.Path(exists=True, file_okay=True, dir_okay=False)
)
@click.option(
    "--model", 
    envvar="OPENROUTER_MODEL",
    default="google/gemini-flash-1.5-8b",
    help="Model to use for descriptions"
)
@click.option(
    "--max-files", 
    type=int,
    help="Maximum number of files to process (for testing)"
)
@click.option(
    "--batch-size", 
    type=int,
    default=5,
    help="Number of files to process in each batch"
)
@click.option(
    "--media-type",
    type=click.Choice(["image", "all"]),
    default="image",
    help="Type of media to generate descriptions for"
)
@click.option(
    "--save-txt/--no-save-txt",
    default=True,
    help="Save descriptions as .txt files alongside original media"
)
def describe_media(
    dataset_path, 
    output_path, 
    telegram_db=None,
    model="google/gemini-flash-1.5-8b",
    max_files=None,
    batch_size=5,
    media_type="image",
    save_txt=True
):
    """
    Generate AI descriptions for media files using OpenRouter
    
    This command explicitly requires an OpenRouter API key in your .env file.
    It will use Gemini or other models via OpenRouter to generate natural
    language descriptions of media files (currently only images are supported).
    
    The API key must be set in the OPENROUTER_API_KEY environment variable.
    
    Example:
        tasks media describe /path/to/dataset /path/to/output \\
            --telegram-db /path/to/telegram_chats.db \\
            --model "google/gemini-pro-vision:latest"
    """
    from dotenv import load_dotenv
    
    # Try to load .env file for API keys
    dotenv_path = os.path.join(os.getcwd(), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
    
    # Check for API key
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        click.echo("Error: OpenRouter API key is required for media descriptions")
        click.echo("Create a .env file with OPENROUTER_API_KEY=your_key")
        return 1
    
    # Run the main build function
    from . import media as media_lib
    media_lib.build_media_db(
        dataset_path=dataset_path,
        output_path=output_path,
        telegram_db_path=telegram_db,
        openrouter_api_key=api_key,
        model=model,
        max_files=max_files,
        batch_size=batch_size,
        media_type_filter=media_type,
        save_txt=save_txt
    )


if __name__ == "__main__":
    cli()
