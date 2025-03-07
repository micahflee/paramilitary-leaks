import os
import json
import time
import sqlite3
import base64
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass

import requests
from tqdm import tqdm
from PIL import Image
import io
import re
import random

# Try to import OpenAI for OpenRouter compatibility
try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# Media types we want to index
MEDIA_EXTENSIONS = {
    'image': ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff', '.svg'],
    'video': ['.mp4', '.mov', '.avi', '.mkv', '.webm', '.flv', '.wmv', '.m4v'],
    'audio': ['.mp3', '.wav', '.ogg', '.m4a', '.flac', '.aac'],
    'document': ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.txt', '.rtf'],
}

# Flatten all extensions into one list
ALL_MEDIA_EXTENSIONS = [ext for exts in MEDIA_EXTENSIONS.values() for ext in exts]


@dataclass
class MediaAsset:
    """Represents a media asset in the dataset"""
    file_path: str
    file_name: str
    file_size: int
    media_type: str
    extension: str
    creation_time: Optional[str] = None
    modified_time: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    duration: Optional[float] = None
    description: Optional[str] = None
    source_chat: Optional[str] = None
    source_message: Optional[str] = None
    source_media_path: Optional[str] = None
    openai_model: Optional[str] = None
    process_time: Optional[str] = None
    error: Optional[str] = None


class OpenRouterClient:
    """Client for interacting with OpenRouter to access Gemini and other models"""
    
    def __init__(self, api_key: str, default_model: str = "google/gemini-flash-1.5-8b"):
        """
        Initialize the OpenRouter client
        
        Args:
            api_key: OpenRouter API key
            default_model: Default model to use (gemini-pro-vision by default)
        """
        if not OPENAI_AVAILABLE:
            raise ImportError("OpenAI package is required for OpenRouter integration")
        
        self.client = openai.OpenAI(
            api_key=api_key,
            base_url="https://openrouter.ai/api/v1",
        )
        self.default_model = default_model
    
    def encode_image(self, image_path: str) -> str:
        """Encode an image as base64 for API submission"""
        try:
            with open(image_path, "rb") as image_file:
                return base64.b64encode(image_file.read()).decode('utf-8')
        except Exception as e:
            raise ValueError(f"Error encoding image: {str(e)}")
    
    def describe_image(self, image_path: str, model: Optional[str] = None) -> Tuple[str, str]:
        """
        Get a description of an image using vision model
        
        Args:
            image_path: Path to the image file
            model: Model to use (defaults to self.default_model)
            
        Returns:
            Tuple of (description, model_used)
        """
        if not model:
            model = self.default_model
            
        # Encode the image
        try:
            base64_image = self.encode_image(image_path)
        except Exception as e:
            raise ValueError(f"Failed to encode image: {str(e)}")
        
        # Prepare the API call
        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an investigative analyst examining media from paramilitary group chats. Your task is to systematically document the content of each image with extreme accuracy."
                    },
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": """Analyze this image systematically in these sections:

1. VISUAL CONTENT: Describe what is visually shown in the image (people, scenes, objects).

2. TEXT TRANSCRIPTION: Transcribe ALL text visible in the image as accurately as possible. Include usernames, timestamps, captions, signs, etc.

3. CLASSIFICATION: Categorize this image as one of: [Screenshot, Meme, Selfie, Group Photo, Document/Text, Map/Location, Weapon/Tactical Equipment, Political Content, News Media, Propaganda, Other (specify)].

4. INVESTIGATIVE NOTES: Any content of potential interest for investigation of extremist activity. Note any symbols, coded language, threats, or concerning elements.

Be thorough, objective, and precise in your analysis - this is for a formal investigation. Respond in only plaintext, never markdown. This description will be added to a database, try to keep newlines to a minimum."""},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{base64_image}"
                                }
                            }
                        ]
                    }
                ],
                max_tokens=1000
            )
            
            description = response.choices[0].message.content
            model_used = response.model
            
            return description, model_used
            
        except Exception as e:
            raise Exception(f"API call failed: {str(e)}")


def is_media_file(file_path: str) -> bool:
    """Check if a file is a supported media type"""
    ext = os.path.splitext(file_path)[1].lower()
    result = ext in ALL_MEDIA_EXTENSIONS
    
    # Debug: Print the first few files that are checked
    if not result and random.random() < 0.001:  # Only print a small sample to avoid flooding
        print(f"DEBUG: File not recognized as media: {file_path} (extension: {ext})")
    
    return result


def get_media_type(file_path: str) -> str:
    """Determine the media type based on file extension"""
    ext = os.path.splitext(file_path)[1].lower()
    
    for media_type, extensions in MEDIA_EXTENSIONS.items():
        if ext in extensions:
            return media_type
    
    return "unknown"


def extract_image_metadata(file_path: str) -> Dict[str, Any]:
    """Extract metadata from an image file"""
    metadata = {}
    
    try:
        with Image.open(file_path) as img:
            metadata['width'] = img.width
            metadata['height'] = img.height
            metadata['format'] = img.format
            
            # Extract EXIF data if available
            if hasattr(img, '_getexif') and img._getexif():
                exif = img._getexif()
                if exif:
                    # Extract creation date if available (EXIF tag 36867)
                    if 36867 in exif:
                        metadata['creation_time'] = exif[36867]
    except Exception as e:
        metadata['error'] = f"Failed to extract image metadata: {str(e)}"
    
    return metadata


def find_related_chat_message(media_file_path: str, db_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Try to find the chat message that references this media file
    This helps establish provenance for media files
    """
    result = {
        'source_chat': None,
        'source_message': None,
        'source_media_path': None,
    }
    
    # If no DB provided, we can't find related messages
    if not db_path or not os.path.exists(db_path):
        return result
    
    try:
        # Get just the filename part (no path)
        filename = os.path.basename(media_file_path)
        
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # First try to find messages that have this file in the media_file field
        cursor.execute("""
        SELECT chat_name, content, media_path
        FROM messages
        WHERE media_file = ?
        LIMIT 1
        """, (filename,))
        
        row = cursor.fetchone()
        if row:
            result['source_chat'] = row[0]
            result['source_message'] = row[1]
            result['source_media_path'] = row[2] if len(row) > 2 else None
        else:
            # If not found by filename, try to find by path
            cursor.execute("""
            SELECT chat_name, content, media_path
            FROM messages
            WHERE media_path LIKE ?
            LIMIT 1
            """, (f'%{media_file_path}%',))
            
            row = cursor.fetchone()
            if row:
                result['source_chat'] = row[0]
                result['source_message'] = row[1]
                result['source_media_path'] = row[2] if len(row) > 2 else None
            else:
                # If still not found, fall back to looking for mentions in content
                cursor.execute("""
                SELECT chat_name, content
                FROM messages
                WHERE content LIKE ?
                LIMIT 1
                """, (f'%{filename}%',))
                
                row = cursor.fetchone()
                if row:
                    result['source_chat'] = row[0]
                    result['source_message'] = row[1]
        
        conn.close()
    except Exception as e:
        # If anything goes wrong, just return empty result
        print(f"Error finding related chat message: {e}")
        pass
    
    return result


def find_media_files(dataset_path: str, media_type_filter: Optional[str] = None) -> List[str]:
    """
    Find all media files in the dataset
    
    Args:
        dataset_path: Path to search for media files
        media_type_filter: Optional filter for specific media types
                          ('image', 'video', 'audio', 'document')
    """
    media_files = []
    
    # Debug: Print the dataset path being searched
    print(f"DEBUG: Searching in dataset_path: '{dataset_path}'")
    print(f"DEBUG: Path exists: {os.path.exists(dataset_path)}")
    print(f"DEBUG: Path is directory: {os.path.isdir(dataset_path)}")
    
    # UI elements to skip - these are Telegram UI images
    ui_elements = [
        "back", "section_", "media_", 
        "round_", "stickers", "voice_messages"
    ]
    
    # Debug counter for files checked
    files_checked = 0
    files_skipped_ui = 0
    files_skipped_not_media = 0
    
    for root, dirs, files in os.walk(dataset_path):
        # Skip the /images/ folder which contains UI elements
        if "/images/" in root or "\\images\\" in root:
            continue
            
        for file in files:
            # Debug: increment counter
            files_checked += 1
            
            # Skip thumb files and UI elements
            if "_thumb" in file or ".thumb." in file:
                continue
                
            # Skip UI elements
            skip_file = False
            for ui_element in ui_elements:
                if ui_element in file:
                    skip_file = True
                    files_skipped_ui += 1
                    break
                    
            if skip_file:
                continue
                
            file_path = os.path.join(root, file)
                
            # Check if it's a media file
            if is_media_file(file_path):
                # Apply media type filter if specified
                if media_type_filter and media_type_filter != "all":
                    file_media_type = get_media_type(file_path)
                    if file_media_type != media_type_filter:
                        continue
                        
                media_files.append(file_path)
            else:
                files_skipped_not_media += 1
    
    # Debug: Print summary of files checked
    print(f"DEBUG: Total files checked: {files_checked}")
    print(f"DEBUG: Files skipped (UI elements): {files_skipped_ui}")
    print(f"DEBUG: Files skipped (not media): {files_skipped_not_media}")
    print(f"DEBUG: Media files found: {len(media_files)}")
    
    return media_files


def save_description_as_txt(file_path: str, description: str) -> str:
    """
    Save description as a .txt file alongside the original media file
    
    Args:
        file_path: Path to the media file
        description: Text description to save
        
    Returns:
        Path to the saved txt file
    """
    # Create the .txt file path with the same name as the original file
    base_path = os.path.splitext(file_path)[0]
    txt_path = f"{base_path}.description.txt"
    
    # Write the description to the .txt file
    with open(txt_path, 'w', encoding='UTF-8') as f:
        f.write(description)
    
    return txt_path


def analyze_media_asset(
    file_path: str, 
    openrouter_client: Optional[OpenRouterClient] = None,
    db_path: Optional[str] = None,
    save_txt: bool = True,
    force_regenerate: bool = False  # Always false by default - will only regenerate if explicitly set
) -> MediaAsset:
    """
    Analyze a media asset to extract metadata and optionally generate a description
    
    Args:
        file_path: Path to the media file
        openrouter_client: OpenRouter client for generating descriptions
        db_path: Path to the chat database for finding related messages
        save_txt: Whether to save descriptions as .txt files alongside media
        
    Returns:
        MediaAsset object with metadata
    """
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    extension = os.path.splitext(file_path)[1].lower()
    media_type = get_media_type(file_path)
    
    # Get file timestamps
    creation_time = datetime.fromtimestamp(os.path.getctime(file_path)).isoformat()
    modified_time = datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
    
    # Create base asset
    asset = MediaAsset(
        file_path=file_path,
        file_name=file_name,
        file_size=file_size,
        media_type=media_type,
        extension=extension,
        creation_time=creation_time,
        modified_time=modified_time
    )
    
    # Extract media-specific metadata
    if media_type == 'image':
        try:
            metadata = extract_image_metadata(file_path)
            asset.width = metadata.get('width')
            asset.height = metadata.get('height')
            if 'error' in metadata:
                asset.error = metadata['error']
        except Exception as e:
            asset.error = f"Failed to extract image metadata: {str(e)}"
    
    # Find related chat message if possible
    if db_path:
        related = find_related_chat_message(file_path, db_path)
        asset.source_chat = related.get('source_chat')
        asset.source_message = related.get('source_message')
        asset.source_media_path = related.get('source_media_path')
    
    # Check if description already exists as a .txt file
    desc_txt_path = f"{os.path.splitext(file_path)[0]}.description.txt"
    has_existing_description = False
    
    if os.path.exists(desc_txt_path):
        try:
            with open(desc_txt_path, 'r', encoding='UTF-8') as f:
                existing_desc = f.read().strip()
                if existing_desc:
                    asset.description = existing_desc
                    asset.openai_model = "Previously generated description found"
                    print(f"  Using existing description for {file_name}")
                    has_existing_description = True
        except Exception as e:
            print(f"  Warning: Could not read existing description file {desc_txt_path}: {e}")
            # Don't continue with API generation if the file exists but couldn't be read
            has_existing_description = True
    
    # Generate description ONLY if OpenRouter client is provided AND no existing description file
    if openrouter_client and media_type == 'image' and not has_existing_description:
        try:
            description, model = openrouter_client.describe_image(file_path)
            asset.description = description
            asset.openai_model = model
            asset.process_time = datetime.now().isoformat()
            
            # Save description as .txt file alongside the original media
            if save_txt and description:
                txt_path = save_description_as_txt(file_path, description)
                print(f"  Saved description to {txt_path}")
                
        except Exception as e:
            asset.error = f"Failed to generate description: {str(e)}"
    
    return asset


def create_media_db_schema(db_path: str):
    """Create the SQLite database schema for media assets"""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # Create tables
    c.execute('''
    CREATE TABLE IF NOT EXISTS media_assets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_path TEXT NOT NULL,
        file_name TEXT NOT NULL,
        file_size INTEGER,
        media_type TEXT,
        extension TEXT,
        creation_time TEXT,
        modified_time TEXT,
        width INTEGER,
        height INTEGER,
        duration REAL,
        description TEXT,
        source_chat TEXT,
        source_message TEXT,
        source_media_path TEXT,
        openai_model TEXT,
        process_time TEXT,
        error TEXT
    )
    ''')
    
    # Create indexes for common queries
    c.execute('CREATE INDEX IF NOT EXISTS idx_media_file_path ON media_assets(file_path)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_media_type ON media_assets(media_type)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_source_chat ON media_assets(source_chat)')
    
    # Create a metadata table for processing info
    c.execute('''
    CREATE TABLE IF NOT EXISTS media_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        process_timestamp TEXT,
        version TEXT,
        dataset_path TEXT,
        output_path TEXT,
        command TEXT,
        total_files INTEGER,
        openrouter_model TEXT
    )
    ''')
    
    conn.commit()
    conn.close()


def add_media_to_db(db_path: str, asset: MediaAsset):
    """Add a media asset to the database"""
    conn = sqlite3.connect(db_path)
    add_media_to_db_with_connection(conn, asset)
    conn.commit()
    conn.close()


def add_media_to_db_with_connection(conn: sqlite3.Connection, asset: MediaAsset):
    """Add a media asset to the database using an existing connection"""
    c = conn.cursor()
    
    c.execute('''
    INSERT INTO media_assets (
        file_path, file_name, file_size, media_type, extension,
        creation_time, modified_time, width, height, duration,
        description, source_chat, source_message, source_media_path, openai_model, process_time,
        error
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        asset.file_path, asset.file_name, asset.file_size, asset.media_type, asset.extension,
        asset.creation_time, asset.modified_time, asset.width, asset.height, asset.duration,
        asset.description, asset.source_chat, asset.source_message, asset.source_media_path, asset.openai_model, asset.process_time,
        asset.error
    ))


def add_metadata_to_media_db(db_path: str, dataset_path: str, output_path: str, command: str, 
                            total_files: int, openrouter_model: Optional[str] = None):
    """Add process metadata to the media database"""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    version = "1.0.0"  # You can update this with proper versioning
    
    c.execute('''
    INSERT INTO media_metadata 
    (process_timestamp, version, dataset_path, output_path, command, total_files, openrouter_model) 
    VALUES (datetime('now'), ?, ?, ?, ?, ?, ?)
    ''', (version, dataset_path, output_path, command, total_files, openrouter_model))
    
    conn.commit()
    conn.close()


def build_media_db(dataset_path: str, output_path: str, telegram_db_path: Optional[str] = None, 
                  openrouter_api_key: Optional[str] = None, model: Optional[str] = None,
                  max_files: Optional[int] = None, batch_size: int = 10,
                  media_type_filter: Optional[str] = None, save_txt: bool = True):
    """
    Build a database of media assets with optional AI-generated descriptions
    
    Args:
        dataset_path: Path to the dataset containing media files
        output_path: Path to save output files
        telegram_db_path: Path to the Telegram database for linking media to chats
        openrouter_api_key: OpenRouter API key for generating descriptions
        model: Model to use for descriptions (defaults to gemini-pro-vision)
        max_files: Maximum number of files to process (for testing)
        batch_size: Number of files to process before committing to DB
        media_type_filter: Optional filter for specific media types (image, video, etc.)
    """
    # Record command reconstruction for metadata
    if openrouter_api_key:
        command = f"tasks media describe '{dataset_path}' '{output_path}'"
    else:
        command = f"tasks media index '{dataset_path}' '{output_path}'"
        
    if telegram_db_path:
        command += f" --telegram-db '{telegram_db_path}'"
    if media_type_filter:
        command += f" --media-type {media_type_filter}"
    
    # Find all media files
    print(f"Finding media files in {dataset_path}...")
    if media_type_filter:
        print(f"Filtering for media type: {media_type_filter}")
    
    # Find files recursively
    print("Searching recursively through all subdirectories...")
    media_files = find_media_files(dataset_path, media_type_filter)
    
    # Shuffle files to get a random sample if limiting
    if max_files:
        random.shuffle(media_files)
    
    # Limit the number of files if requested
    if max_files and len(media_files) > max_files:
        print(f"Limiting to {max_files} files (out of {len(media_files)} found)")
        media_files = media_files[:max_files]
    else:
        print(f"Found {len(media_files)} media files")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)
    
    # Create SQLite database
    db_path = os.path.join(output_path, "media_assets.db")
    create_media_db_schema(db_path)
    
    print(f"Created media database at {db_path}")
    
    # Initialize OpenRouter client if API key is provided
    openrouter_client = None
    if openrouter_api_key and OPENAI_AVAILABLE:
        try:
            openrouter_client = OpenRouterClient(
                api_key=openrouter_api_key,
                default_model=model or "google/gemini-pro-vision:latest"
            )
            print(f"OpenRouter client initialized for generating descriptions")
            print(f"Using model: {openrouter_client.default_model}")
        except Exception as e:
            print(f"Failed to initialize OpenRouter client: {str(e)}")
            print(f"Continuing without descriptions")
    
    # Get the system's file descriptor limit
    try:
        import resource
        soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        print(f"System file descriptor limits: soft={soft_limit}, hard={hard_limit}")
        # Try to increase the soft limit if possible
        if soft_limit < hard_limit:
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (hard_limit, hard_limit))
                print(f"Increased file descriptor limit to {hard_limit}")
                soft_limit = hard_limit
            except Exception as e:
                print(f"Could not increase file descriptor limit: {e}")
    except ImportError:
        # resource module not available on Windows
        soft_limit = 1024  # Default conservative estimate
        print("Could not determine system file descriptor limits (Windows?)")
    
    # Calculate a safe batch size (leaving some headroom for other file operations)
    # Each file processing opens at least 2 files (media file, description file)
    safe_batch_size = min(batch_size, max(1, (soft_limit - 100) // 3))
    print(f"Processing in batches of {safe_batch_size} files to avoid 'Too many open files' error")
    
    # Process media files
    print(f"Processing media files...")
    processed_count = 0
    error_count = 0
    description_count = 0
    
    # Process files in batches with progress bar
    from tqdm import tqdm
    
    # Calculate number of batches
    num_batches = (len(media_files) + safe_batch_size - 1) // safe_batch_size
    
    for batch_idx in tqdm(range(num_batches), desc="Processing media batches", unit="batch"):
        batch_start = batch_idx * safe_batch_size
        batch_end = min(batch_start + safe_batch_size, len(media_files))
        batch = media_files[batch_start:batch_end]
        
        conn = sqlite3.connect(db_path)
        
        for file_path in batch:
            try:
                # Analyze the media asset
                asset = analyze_media_asset(
                    file_path=file_path,
                    openrouter_client=openrouter_client,
                    db_path=telegram_db_path,
                    save_txt=save_txt
                )
                
                # Add to database
                add_media_to_db_with_connection(conn, asset)
                
                # Update counters
                processed_count += 1
                if asset.description:
                    description_count += 1
                if asset.error:
                    error_count += 1
                    
                # Throttle API calls if using OpenRouter
                if openrouter_client and asset.description:
                    time.sleep(0.5)  # Prevent rate limiting
                    
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")
                error_count += 1
        
        # Commit the batch and close connection
        conn.commit()
        conn.close()
        
        # Force garbage collection after each batch
        try:
            import gc
            gc.collect()
        except:
            pass
    
    # Add final metadata to database
    add_metadata_to_media_db(
        db_path=db_path,
        dataset_path=dataset_path,
        output_path=output_path,
        command=command,
        total_files=len(media_files),
        openrouter_model=model or "google/gemini-flash-1.5-8b" if openrouter_api_key else None
    )
    
    # Export media table to CSV
    print(f"Exporting media assets to CSV...")
    from .telegram import export_table_to_csv
    media_csv = export_table_to_csv(db_path, "media_assets", output_path)
    
    # Export to Parquet if supported
    try:
        print(f"Exporting media assets to Parquet...")
        from .telegram import export_table_to_parquet
        media_parquet = export_table_to_parquet(db_path, "media_assets", output_path)
    except Exception as e:
        print(f"Error exporting to Parquet: {e}")
    
    # Create a README file
    readme_path = os.path.join(output_path, "MEDIA_README.md")
    try:
        with open(readme_path, 'w', encoding='UTF-8') as f:
            f.write(f"""# Media Assets Database

Generated on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Processing Summary
* Command: `{command}`
* Dataset path: `{dataset_path}`
* Output path: `{output_path}`
* Processed {processed_count} media files
* Generated {description_count} AI descriptions
* Encountered {error_count} errors

## Output Files
* **SQLite database**: `media_assets.db`
  * Contains all media files with metadata
  * Linked to Telegram messages where possible
* **CSV export**: `media_assets.csv`
  * Complete export of the database table
* **Description files**: `.description.txt` files alongside media
  * Plain text files with AI-generated descriptions

## Schema
* `id`: Unique identifier
* `file_path`: Path to the media file
* `file_name`: Name of the media file
* `file_size`: Size of the file in bytes
* `media_type`: Type of media (image, video, audio, document)
* `extension`: File extension
* `creation_time`: File creation timestamp
* `modified_time`: File modification timestamp
* `width`: Width in pixels (images only)
* `height`: Height in pixels (images only)
* `duration`: Duration in seconds (audio/video only)
* `description`: AI-generated description (if available)
* `source_chat`: Name of the chat where this media was shared
* `source_message`: Content of the message that shared this media
* `source_media_path`: Path to the media file from the message
* `openai_model`: Model used for description generation
* `process_time`: When the file was processed
* `error`: Any errors encountered during processing
""")
    except Exception as e:
        print(f"Error writing README file: {e}")
    
    print(f"\nCompleted processing {processed_count} media files")
    print(f"Summary:")
    print(f"  - Processed {processed_count} media files")
    print(f"  - Generated {description_count} AI descriptions")
    print(f"  - Encountered {error_count} errors")
    print(f"  - Output files:")
    print(f"    1. SQLite database: {db_path}")
    print(f"    2. CSV export: {media_csv}")
    print(f"    3. README: {readme_path}")
    print(f"\nTo explore the data with Datasette:")
    print(f"  datasette {db_path}")
