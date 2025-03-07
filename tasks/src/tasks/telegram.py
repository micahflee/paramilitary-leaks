import os
import csv
import re
import sqlite3
import json
from datetime import datetime
from html import unescape
from typing import List, Tuple, Dict, Any
import shutil
import traceback
import gc
import resource
import time

# Import BeautifulSoup for proper HTML parsing
try:
    from bs4 import BeautifulSoup
    BEAUTIFULSOUP_AVAILABLE = True
    print("Debug: BeautifulSoup is available for HTML parsing")
except ImportError:
    BEAUTIFULSOUP_AVAILABLE = False
    print("Debug: BeautifulSoup is NOT available, falling back to regex parsing")
    print("Debug: Install with: pip install beautifulsoup4")

# Import pandas and pyarrow for Parquet support
try:
    import pandas as pd
    import pyarrow
    PARQUET_SUPPORT = True
    print("Debug: Parquet support is ENABLED - pandas and pyarrow found")
except ImportError:
    PARQUET_SUPPORT = False
    print("Debug: Parquet support is DISABLED - install pandas and pyarrow to enable")


def is_messages_filename(filename: str) -> bool:
    """
    Check if a filename is a valid Telegram messages HTML file.
    
    Args:
        filename: The filename to check
        
    Returns:
        True if the filename is a valid Telegram messages HTML file, False otherwise
    """
    # Skip macOS metadata files that start with "._"
    if filename.startswith("._"):
        print(f"Skipping macOS metadata file: {filename}")
        return False
    return filename.startswith("messages") and filename.endswith(".html")


def find_messages_files(path: str) -> List[str]:
    "Find all messages files in the given path"

    messages_files: List[str] = []

    for root, dirs, files in os.walk(path):
        messages_files.extend(
            [os.path.join(root, f) for f in files if is_messages_filename(f)]
        )

    return messages_files


class Message:
    def __init__(self):
        self.message_id = None
        self.timestamp = None
        self.sender = None
        self.fwd = None
        self.reply = None
        self.content = None
        self.media_file = None  # Field to store linked media filename
        self.media_path = None  # Field to store full path to media file

    def to_tuple(self):
        """Convert message to tuple for database storage or CSV export"""
        if self.message_id:
            self.message_id = self.message_id.replace('message', '')
        if self.timestamp:
            self.timestamp = ' '.join(self.timestamp.split()[:2]) if ' ' in self.timestamp else self.timestamp
        if self.sender:
            self.sender = unescape(self.sender.strip())
        if self.fwd:
            self.fwd = unescape(self.fwd.strip())
        if self.reply:
            self.reply = self.reply.replace('message', '')
        if self.content:
            self.content = unescape(self.content.strip())
        # No processing needed for media_file and media_path

        return (self.message_id, self.timestamp, self.sender, self.fwd, self.reply, self.content, self.media_file, self.media_path)
    
    # Add alias for backward compatibility with original script
    toTuple = to_tuple


def parse_message_with_beautifulsoup(message_div):
    """Parse a message div using BeautifulSoup.
    
    This is a more robust parser that can handle complex HTML structures.
    """
    message_id = message_div.get('id', '').replace('message', '')
    
    # Extract timestamp
    date_div = message_div.select_one('div.date')
    timestamp = date_div.text.strip() if date_div else ''
    
    # Extract sender
    from_name_div = message_div.select_one('div.from_name')
    sender = from_name_div.text.strip() if from_name_div else ''
    
    # Extract forwarded message info
    fwd_div = message_div.select_one('div.forwarded')
    fwd = fwd_div.text.strip() if fwd_div else ''
    
    # Extract reply info
    reply_div = message_div.select_one('div.reply_to')
    reply = reply_div.text.strip() if reply_div else ''
    
    # Initialize media information
    media_file = None
    media_path = None
    
    # Check for media content
    media_wrap = message_div.select_one('div.media_wrap')
    
    if media_wrap:
        # Debug print to understand the structure
        # print(f"Found media_wrap: {media_wrap}")
        
        # Check for different media types
        media_photo = media_wrap.select_one('div.media_photo')
        media_video = media_wrap.select_one('div.media_video')
        media_audio = media_wrap.select_one('div.media_audio')
        media_voice = media_wrap.select_one('div.media_voice')
        media_file_div = media_wrap.select_one('div.media_file')
        
        # Extract media information based on type
        if media_photo:
            title_div = media_photo.select_one('div.title.bold')
            status_div = media_photo.select_one('div.status.details')
            
            media_type = title_div.text.strip() if title_div else "Photo"
            media_details = status_div.text.strip() if status_div else ""
            
            media_file = f"{media_type}: {media_details}"
            media_path = "photo_not_included"
            
        elif media_video:
            title_div = media_video.select_one('div.title.bold')
            status_div = media_video.select_one('div.status.details')
            
            media_type = title_div.text.strip() if title_div else "Video"
            media_details = status_div.text.strip() if status_div else ""
            
            media_file = f"{media_type}: {media_details}"
            media_path = "video_not_included"
            
        elif media_audio:
            title_div = media_audio.select_one('div.title.bold')
            status_div = media_audio.select_one('div.status.details')
            
            media_type = title_div.text.strip() if title_div else "Audio"
            media_details = status_div.text.strip() if status_div else ""
            
            media_file = f"{media_type}: {media_details}"
            media_path = "audio_not_included"
            
        elif media_voice:
            title_div = media_voice.select_one('div.title.bold')
            status_div = media_voice.select_one('div.status.details')
            
            media_type = title_div.text.strip() if title_div else "Voice Message"
            media_details = status_div.text.strip() if status_div else ""
            
            media_file = f"{media_type}: {media_details}"
            media_path = "voice_not_included"
            
        elif media_file_div:
            title_div = media_file_div.select_one('div.title.bold')
            status_div = media_file_div.select_one('div.status.details')
            
            media_type = title_div.text.strip() if title_div else "File"
            media_details = status_div.text.strip() if status_div else ""
            
            media_file = f"{media_type}: {media_details}"
            media_path = "file_not_included"
            
        else:
            # If we found media_wrap but none of the specific types, check for any other media elements
            any_media = media_wrap.select_one('div.media')
            if any_media:
                title_div = any_media.select_one('div.title.bold')
                status_div = any_media.select_one('div.status.details')
                
                media_type = title_div.text.strip() if title_div else "Media"
                media_details = status_div.text.strip() if status_div else ""
                
                media_file = f"{media_type}: {media_details}"
                media_path = "media_not_included"
    
    # Extract text content
    text_div = message_div.select_one('div.text')
    content = text_div.text.strip() if text_div else ''
    
    # If no content but we have media, set a placeholder
    if not content and media_file:
        content = f"[{media_file}]"
    
    return {
        'message_id': message_id,
        'timestamp': timestamp,
        'sender': sender,
        'fwd': fwd,
        'reply': reply,
        'content': content,
        'media_file': media_file,
        'media_path': media_path
    }


def convert_telegram_html_to_messages(html_file_path, use_beautifulsoup=True):
    """
    Convert a Telegram HTML export file to a list of messages.
    
    Args:
        html_file_path: Path to the HTML file
        use_beautifulsoup: Whether to use BeautifulSoup for parsing (more robust but slower)
        
    Returns:
        List of Message objects
    """
    # Skip macOS metadata files
    if os.path.basename(html_file_path).startswith("._"):
        print(f"Skipping macOS metadata file: {html_file_path}")
        return []
    
    # Load the HTML file with multiple encoding attempts
    html_content = None
    encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    last_error = None
    
    for encoding in encodings_to_try:
        try:
            print(f"Trying to open {html_file_path} with {encoding} encoding...")
            with open(html_file_path, 'r', encoding=encoding) as f:
                html_content = f.read()
            print(f"Successfully opened with {encoding} encoding")
            break  # If successful, break out of the loop
        except UnicodeDecodeError as e:
            last_error = e
            print(f"Failed to open with {encoding} encoding: {str(e)}")
            if encoding == encodings_to_try[-1]:  # If this was the last encoding to try
                print(f"ERROR: Could not decode {html_file_path} with any of the attempted encodings")
                raise  # Re-raise the exception
            continue  # Try the next encoding
    
    if not html_content:
        print(f"ERROR: Failed to read {html_file_path}")
        if last_error:
            raise last_error
        return []
    
    messages = []
    
    # Use BeautifulSoup for parsing if available
    if use_beautifulsoup and BEAUTIFULSOUP_AVAILABLE:
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all message divs (both default and joined messages)
            message_divs = soup.select('div.message.default, div.message.default.joined')
            
            for message_div in message_divs:
                # Parse the message
                message_data = parse_message_with_beautifulsoup(message_div)
                
                # Create a Message object
                message = Message()
                message.message_id = message_data['message_id']
                message.timestamp = message_data['timestamp']
                message.sender = message_data['sender']
                message.fwd = message_data['fwd']
                message.reply = message_data['reply']
                message.content = message_data['content']
                message.media_file = message_data['media_file']
                message.media_path = message_data['media_path']
                
                messages.append(message)
            
            print(f"Parsed {len(messages)} messages using BeautifulSoup")
            return messages
        except Exception as e:
            print(f"Error parsing with BeautifulSoup: {str(e)}")
            print("Falling back to regex parsing")
            # Fall back to regex parsing
    
    # If BeautifulSoup is not available or failed, use regex parsing
    # ... (rest of the function remains the same)
    
    return messages


def save_messages_to_csv(chat_name: str, messages: List[Message], output_dir: str) -> str:
    """Save messages to a CSV file and return the file path"""
    output_file = os.path.join(output_dir, f'Telegram-{"".join(c if c.isalnum() else "_" for c in chat_name)}.csv')
    
    with open(output_file, 'w+', encoding='UTF-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['message_id', 'timestamp', 'sender', 'fwd', 'reply', 'content', 'media_file', 'media_path'])
        writer.writerows([m.to_tuple() for m in messages])
    
    return output_file


def create_db_schema(db_path: str):
    """Create the SQLite database schema with consistent column naming"""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # Check if chats table exists and if it has processed_timestamp column
    c.execute("PRAGMA table_info(chats)")
    columns = c.fetchall()
    chats_table_exists = len(columns) > 0
    has_processed_timestamp = any(col[1] == 'processed_timestamp' for col in columns)
    
    # If chats table exists but doesn't have processed_timestamp, add it
    if chats_table_exists and not has_processed_timestamp:
        print("Adding processed_timestamp column to chats table")
        c.execute("ALTER TABLE chats ADD COLUMN processed_timestamp TEXT")
        conn.commit()
    
    # Create metadata table for process tracking
    c.execute('''
    CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    ''')
    
    # Create sources table to track file origins with consistent column naming
    c.execute('''
    CREATE TABLE IF NOT EXISTS sources (
        source_id INTEGER PRIMARY KEY AUTOINCREMENT,
        html_path TEXT NOT NULL,
        csv_path TEXT NOT NULL,
        local_csv_path TEXT NOT NULL,
        processed_timestamp TEXT NOT NULL,
        file_size INTEGER,
        messages_count INTEGER,
        chat_id INTEGER,
        FOREIGN KEY (chat_id) REFERENCES chats (chat_id)
    )
    ''')
    
    # Create chats table with enhanced metadata
    c.execute('''
    CREATE TABLE IF NOT EXISTS chats (
        chat_id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_name TEXT NOT NULL,
        file_count INTEGER,
        message_count INTEGER,
        first_message_date TEXT,
        last_message_date TEXT,
        processed_timestamp TEXT NOT NULL
    )
    ''')
    
    # Create messages table with comprehensive source data and consistent column naming
    c.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        message_id TEXT,
        timestamp TEXT,
        sender TEXT,
        fwd TEXT,
        reply TEXT,
        content TEXT,
        media_file TEXT,
        media_path TEXT,
        source_id INTEGER,
        chat_name TEXT,
        html_path TEXT,
        csv_path TEXT,
        processed_timestamp TEXT,
        FOREIGN KEY (chat_id) REFERENCES chats (chat_id),
        FOREIGN KEY (source_id) REFERENCES sources (source_id)
    )
    ''')
    
    conn.commit()
    conn.close()
    
    print(f"Created database schema at {db_path} with consistent column naming")
    return True


def add_source_to_db(db_path: str, html_path: str, csv_path: str, local_csv_path: str, messages_count: int, chat_id: int = None, conn=None):
    """Add source file information to the database using consistent column names"""
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(db_path)
        close_conn = True
    
    c = conn.cursor()
    
    # Get file size
    try:
        file_size = os.path.getsize(html_path)
    except:
        file_size = 0
    
    # Add source with consistent column names
    if chat_id:
        c.execute('''
        INSERT INTO sources 
        (html_path, csv_path, local_csv_path, processed_timestamp, file_size, messages_count, chat_id) 
        VALUES (?, ?, ?, datetime('now'), ?, ?, ?)
        ''', (html_path, csv_path, local_csv_path, file_size, messages_count, chat_id))
    else:
        c.execute('''
        INSERT INTO sources 
        (html_path, csv_path, local_csv_path, processed_timestamp, file_size, messages_count) 
        VALUES (?, ?, ?, datetime('now'), ?, ?)
        ''', (html_path, csv_path, local_csv_path, file_size, messages_count))
    
    source_id = c.lastrowid
    
    conn.commit()
    if close_conn:
        conn.close()
    
    return source_id


def add_messages_to_db(db_path: str, chat_name: str, messages: List[Message], source_id: int, 
                     html_path: str, csv_path: str, conn=None):
    """Add messages to the database with full provenance tracking"""
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(db_path)
        close_conn = True
    
    c = conn.cursor()
    
    # Get chat_id from source
    c.execute('SELECT chat_id FROM sources WHERE source_id = ?', (source_id,))
    result = c.fetchone()
    chat_id = result[0] if result else None
    
    # If chat_id is not found, try to get it from the chat name
    if not chat_id:
        c.execute('SELECT chat_id FROM chats WHERE chat_name = ?', (chat_name,))
        result = c.fetchone()
        chat_id = result[0] if result else None
    
    # Add messages with full provenance tracking
    for message in messages:
        message_tuple = message.to_tuple()
        
        c.execute('''
        INSERT INTO messages 
        (message_id, timestamp, sender, fwd, reply, content, media_file, media_path, 
         source_id, chat_id, chat_name, html_path, csv_path, processed_timestamp) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ''', (
            message_tuple[0],  # message_id
            message_tuple[1],  # timestamp
            message_tuple[2],  # sender
            message_tuple[3],  # fwd
            message_tuple[4],  # reply
            message_tuple[5],  # content
            message_tuple[6],  # media_file
            message_tuple[7],  # media_path
            source_id,
            chat_id,
            chat_name,
            html_path,
            csv_path
        ))
    
    conn.commit()
    if close_conn:
        conn.close()
    
    return len(messages)


def add_metadata_to_db(db_path: str, dataset_path: str, output_path: str, command: str, total_chats: int, total_messages: int):
    """Add process metadata to the database"""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    version = "1.0.0"  # You can update this with proper versioning
    
    c.execute('''
    INSERT INTO metadata 
    (process_timestamp, version, dataset_path, output_path, command, total_chats, total_messages) 
    VALUES (datetime('now'), ?, ?, ?, ?, ?, ?)
    ''', (version, dataset_path, output_path, command, total_chats, total_messages))
    
    conn.commit()
    conn.close()


def export_table_to_csv(db_path: str, table_name: str, output_path: str) -> str:
    """Export a database table to CSV file"""
    conn = sqlite3.connect(db_path)
    
    # Get column names
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    
    # Query all data
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    
    # Write to CSV
    output_file = os.path.join(output_path, f"{table_name}.csv")
    with open(output_file, 'w', encoding='UTF-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    
    conn.close()
    
    return output_file


def export_table_to_parquet(db_path: str, table_name: str, output_path: str, 
                           partition_cols: List[str] = None) -> str:
    """
    Export a database table to Parquet format with optional partitioning
    
    Args:
        db_path: Path to SQLite database
        table_name: Name of the table to export
        output_path: Directory to save the output
        partition_cols: Optional list of columns to partition by
        
    Returns:
        Path to the output parquet file or directory
    """
    if not PARQUET_SUPPORT:
        print(f"Warning: Parquet export not available. Install pandas and pyarrow.")
        return None
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    
    # Use pandas to load the data (handles types better than manual loading)
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    
    # Clean data for Parquet
    # Convert empty strings to None (null) for better Parquet compatibility
    for col in df.columns:
        if df[col].dtype == 'object':  # String columns
            df[col] = df[col].replace('', None)
    
    # Handle partitioning
    if partition_cols:
        # Create a parent directory for the partitioned dataset
        parquet_path = os.path.join(output_path, f"{table_name}_parquet")
        os.makedirs(parquet_path, exist_ok=True)
        
        # If timestamp column exists, add date extraction for partitioning
        if 'timestamp' in df.columns and not any(col.startswith('date_') for col in df.columns):
            try:
                # Extract date components where possible for better partitioning
                df['date_year'] = pd.to_datetime(df['timestamp']).dt.year
                df['date_month'] = pd.to_datetime(df['timestamp']).dt.month
                df['date_day'] = pd.to_datetime(df['timestamp']).dt.day
                
                # Add date partitioning if not explicitly specified
                if 'date_year' not in partition_cols and 'date_month' not in partition_cols:
                    partition_cols = ['date_year', 'date_month'] + partition_cols
            except:
                # If date parsing fails, just continue without date partitioning
                pass
        
        # Write partitioned parquet
        df.to_parquet(parquet_path, partition_cols=partition_cols, index=False)
        return parquet_path
    else:
        # Write single parquet file
        parquet_file = os.path.join(output_path, f"{table_name}.parquet")
        df.to_parquet(parquet_file, index=False)
        return parquet_file


def optimize_messages_for_analytics(conn, output_dir):
    """
    Create optimized analytics files from the messages table.
    
    Args:
        conn: SQLite connection object
        output_dir: Directory to save the optimized files
    """
    print("  Debug: Successfully imported pandas in optimize_messages_for_analytics")
    
    # Create analytics directory
    analytics_dir = os.path.join(output_dir, 'analytics')
    os.makedirs(analytics_dir, exist_ok=True)
    
    # Load messages table into pandas DataFrame
    query = "SELECT * FROM messages"
    df = pd.read_sql_query(query, conn)
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # Create enhanced CSV with additional columns
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    df['hour'] = df['timestamp'].dt.hour
    df['minute'] = df['timestamp'].dt.minute
    df['second'] = df['timestamp'].dt.second
    df['weekday'] = df['timestamp'].dt.day_name()
    df['has_media'] = df['media_file'].notnull()
    
    # Save enhanced CSV
    enhanced_csv = os.path.join(analytics_dir, 'messages_enhanced.csv')
    df.to_csv(enhanced_csv, index=False)
    
    # Create Parquet files partitioned by date
    date_dir = os.path.join(analytics_dir, 'messages_by_date')
    os.makedirs(date_dir, exist_ok=True)
    
    # Create a date column for partitioning
    df['date'] = df['timestamp'].dt.date
    
    # Partition by date
    for date, group in df.groupby('date'):
        if pd.isna(date):
            continue
        
        date_str = str(date)
        year, month, day = date_str.split('-')
        
        # Create directory structure
        year_dir = os.path.join(date_dir, f'year={year}')
        month_dir = os.path.join(year_dir, f'month={month}')
        day_dir = os.path.join(month_dir, f'day={day}')
        
        os.makedirs(day_dir, exist_ok=True)
        
        # Save Parquet file
        parquet_file = os.path.join(day_dir, f'messages_{date_str}.parquet')
        group.to_parquet(parquet_file, index=False)
    
    # Create Parquet files partitioned by chat
    chat_dir = os.path.join(analytics_dir, 'messages_by_chat')
    os.makedirs(chat_dir, exist_ok=True)
    
    # Partition by chat
    for chat_name, group in df.groupby('chat_name'):
        if pd.isna(chat_name) or not chat_name:
            continue
        
        # Create safe filename
        safe_chat_name = re.sub(r'[^\w\-_\.]', '_', chat_name)
        
        # Save Parquet file
        parquet_file = os.path.join(chat_dir, f'messages_{safe_chat_name}.parquet')
        group.to_parquet(parquet_file, index=False)
    
    # Create Parquet files partitioned by sender
    sender_dir = os.path.join(analytics_dir, 'messages_by_sender')
    os.makedirs(sender_dir, exist_ok=True)
    
    # Partition by sender
    for sender, group in df.groupby('sender'):
        if pd.isna(sender) or not sender:
            continue
        
        # Create safe filename
        safe_sender = re.sub(r'[^\w\-_\.]', '_', sender)
        
        # Save Parquet file
        parquet_file = os.path.join(sender_dir, f'messages_{safe_sender}.parquet')
        group.to_parquet(parquet_file, index=False)
    
    # Create a single Parquet file with all messages
    all_parquet = os.path.join(analytics_dir, 'messages_all.parquet')
    df.to_parquet(all_parquet, index=False)
    
    # Create summary files
    
    # Messages by date summary
    date_summary = df.groupby(['year', 'month', 'day']).size().reset_index(name='message_count')
    date_summary['date'] = pd.to_datetime(date_summary[['year', 'month', 'day']])
    date_summary = date_summary.sort_values('date')
    date_summary.to_csv(os.path.join(analytics_dir, 'messages_by_date_summary.csv'), index=False)
    
    # Messages by sender summary
    sender_summary = df.groupby('sender').size().reset_index(name='message_count')
    sender_summary = sender_summary.sort_values('message_count', ascending=False)
    sender_summary.to_csv(os.path.join(analytics_dir, 'messages_by_sender_summary.csv'), index=False)
    
    # Chat summary
    chat_summary = df.groupby('chat_name').agg({
        'id': 'count',
        'sender': 'nunique',
        'timestamp': ['min', 'max']
    }).reset_index()
    
    chat_summary.columns = ['chat_name', 'message_count', 'unique_senders', 'first_message', 'last_message']
    chat_summary.to_csv(os.path.join(analytics_dir, 'chat_summary.csv'), index=False)
    
    return {
        'enhanced_csv': 'messages_enhanced.csv',
        'parquet_by_date': 'messages_by_date',
        'parquet_by_chat': 'messages_by_chat',
        'parquet_single_file': 'messages_all.parquet',
        'date_summary': 'messages_by_date_summary.csv',
        'sender_summary': 'messages_by_sender_summary.csv',
        'chat_summary': 'chat_summary.csv'
    }


def build_telegram_db(input_dir, output_dir, max_files=None):
    """
    Build a SQLite database from Telegram chat export files.
    
    Args:
        input_dir: Directory containing Telegram chat export files
        output_dir: Directory to save the database and CSV files
        max_files: Maximum number of files to process (for testing)
    """
    # Check if BeautifulSoup is available
    try:
        from bs4 import BeautifulSoup
        print("Debug: BeautifulSoup is available for HTML parsing")
        use_beautifulsoup = True
    except ImportError:
        print("Debug: BeautifulSoup is NOT available, falling back to regex parsing")
        use_beautifulsoup = False
    
    # Check if pandas and pyarrow are available for Parquet support
    try:
        import pandas as pd
        import pyarrow
        print("Debug: Parquet support is ENABLED - pandas and pyarrow found")
        parquet_support = True
    except ImportError:
        print("Debug: Parquet support is DISABLED - install pandas and pyarrow to enable")
        parquet_support = False
    
    # Find all HTML files in the input directory
    chat_export_files = []
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if is_messages_filename(file):
                chat_export_files.append(os.path.join(root, file))
    
    # Sort files by name
    chat_export_files.sort()
    
    # Limit the number of files if max_files is specified
    if max_files and max_files > 0:
        chat_export_files = chat_export_files[:max_files]
    
    print(f"Found {len(chat_export_files)} chat export files")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create CSV directory
    csv_dir = os.path.join(output_dir, 'csv')
    os.makedirs(csv_dir, exist_ok=True)
    
    # Create SQLite database
    db_path = os.path.join(output_dir, 'telegram_chats.db')
    
    # Check if database exists
    db_exists = os.path.exists(db_path)
    if db_exists:
        print(f"Database already exists at {db_path}, checking schema...")
    else:
        print(f"Creating new database at {db_path}")
    
    # Use the consolidated schema creation function
    create_db_schema(db_path)
    
    # Connect to the database with a timeout and retry logic
    max_retries = 3
    retry_delay = 1  # seconds
    
    for attempt in range(max_retries):
        try:
            # Use a longer timeout to avoid database locked errors
            conn = sqlite3.connect(db_path, timeout=30.0)
            # Enable WAL mode for better concurrency
            conn.execute('PRAGMA journal_mode=WAL')
            # Set a larger cache size for better performance
            conn.execute('PRAGMA cache_size=-10000')  # ~10MB cache
            break
        except sqlite3.OperationalError as e:
            if attempt < max_retries - 1:
                print(f"Database connection error: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                print(f"Failed to connect to database after {max_retries} attempts. Error: {e}")
                raise
    
    cursor = conn.cursor()
    
    # Add metadata
    cursor.execute('INSERT OR REPLACE INTO metadata VALUES (?, ?)', ('created_at', datetime.now().isoformat()))
    cursor.execute('INSERT OR REPLACE INTO metadata VALUES (?, ?)', ('input_dir', input_dir))
    cursor.execute('INSERT OR REPLACE INTO metadata VALUES (?, ?)', ('output_dir', output_dir))
    cursor.execute('INSERT OR REPLACE INTO metadata VALUES (?, ?)', ('file_count', str(len(chat_export_files))))
    cursor.execute('INSERT OR REPLACE INTO metadata VALUES (?, ?)', ('beautifulsoup_available', str(use_beautifulsoup)))
    cursor.execute('INSERT OR REPLACE INTO metadata VALUES (?, ?)', ('parquet_support', str(parquet_support)))
    
    conn.commit()
    
    print(f"Created SQLite database at {db_path}")
    print("Tracking full data provenance and file metadata")
    
    try:
        # Create manifest file
        manifest_path = os.path.join(output_dir, 'manifest.jsonl')
        with open(manifest_path, 'w') as manifest_file:
            # Process each batch
            for batch_num in range(num_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(chat_export_files))
                
                print(f"Processing batch {batch_num+1}/{num_batches} ({start_idx+1}-{end_idx} of {len(chat_export_files)})")
                
                # Process each file in the batch
                for i, chat_export_file in enumerate(chat_export_files[start_idx:end_idx], start=start_idx+1):
                    print(f"Processing [{i}/{len(chat_export_files)}] '{chat_export_file}'")
                    
                    # ... (rest of the file processing code)
                
                # Garbage collect after each batch
                gc.collect()
                print(f"Completed batch {batch_num+1}, garbage collected")
            
            # Create README file with usage instructions
            readme_path = os.path.join(output_dir, 'README.md')
            with open(readme_path, 'w') as f:
                f.write(f"""# Telegram Chat Export Database

This database contains processed Telegram chat exports with full provenance tracking.

## Contents

- SQLite database: `telegram_chats.db`
- CSV exports: `csv/` directory
- Manifest file: `manifest.jsonl`
- Analytics files: `analytics/` directory (if pandas/pyarrow installed)

## Usage

1. For quick analysis: Use messages.csv directly in your analysis tool
2. For database exploration: `datasette {db_path}`
3. For high-performance analytics: Use the Parquet files
4. For detailed research: Consult manifest.jsonl and source paths

## Statistics

- Processed {len(chat_export_files)} chat export files
- Found {total_chats} chats containing {total_messages} messages
- Created on {datetime.now().isoformat()}
""")
            
            # Export tables to CSV for easy access
            messages_csv = export_table_to_csv(db_path, 'messages', os.path.join(output_dir, 'messages.csv'))
            chats_csv = export_table_to_csv(db_path, 'chats', os.path.join(output_dir, 'chats.csv'))
            sources_csv = export_table_to_csv(db_path, 'sources', os.path.join(output_dir, 'sources.csv'))
            metadata_csv = export_table_to_csv(db_path, 'metadata', os.path.join(output_dir, 'metadata.csv'))
            
            print(f"  Exported messages table to {messages_csv}")
            print(f"  Exported chats table to {chats_csv}")
            print(f"  Exported sources table to {sources_csv}")
            print(f"  Exported metadata table to {metadata_csv}")
        
        # Create optimized analytics formats if pandas and pyarrow are available
        if parquet_support:
            print("Creating optimized analytics formats...")
            
            # Export messages table to Parquet
            messages_parquet = os.path.join(output_dir, 'messages.parquet')
            print(f"  Creating messages.parquet (main parquet file)...")
            
            cursor.execute('SELECT * FROM messages')
            columns = [description[0] for description in cursor.description]
            data = cursor.fetchall()
            
            df = pd.DataFrame(data, columns=columns)
            df.to_parquet(messages_parquet, index=False)
            print(f"  Exported messages table to Parquet: {messages_parquet}")
            
            # Create optimized analytics files
            optimize_messages_for_analytics(conn, output_dir)
    
    finally:
        # Close database connection
        if 'conn' in locals() and conn:
            try:
                conn.close()
                print("Database connection closed properly")
            except Exception as e:
                print(f"Error closing database connection: {e}")
    
    print(f"\nCompleted processing {len(chat_export_files)} chat export files")
    print(f"Summary:")
    print(f"  - Processed {total_chats} chats containing {total_messages} messages")
    print(f"  - Detailed provenance tracking in database and manifest file")
    print(f"  - Output files:")
    print(f"    1. SQLite database: {db_path}")
    print(f"    2. CSV export of complete messages table: {messages_csv}")
    print(f"    3. Individual CSV files: {csv_dir} ({len(chat_export_files)} files)")
    print(f"    4. Manifest: {manifest_path}")
    print(f"    5. README: {readme_path}")
    
    print(f"\nData Journalism Workflow:")
    print(f"  1. For quick analysis: Use messages.csv directly in your analysis tool")
    print(f"  2. For database exploration: datasette {db_path}")
    print(f"  3. For high-performance analytics: Use the Parquet files")
    print(f"  4. For detailed research: Consult manifest.jsonl and source paths")
    
    if parquet_support:
        print(f"\nParquet Datasets (optimized for data visualization):")
        print(f"  - Single file: {messages_parquet}")
        print(f"  - By date: {os.path.join(output_dir, 'analytics/messages_by_date')}")
        print(f"  - By chat: {os.path.join(output_dir, 'analytics/messages_by_chat')}")
        print(f"  - By sender: {os.path.join(output_dir, 'analytics/messages_by_sender')}")
        print(f"  - Enhanced analytics: {os.path.join(output_dir, 'analytics/messages_all.parquet')}")
    
    return db_path


def build(dataset_path, output_path):
    """Build a SQLite database and CSV files of Telegram chats with comprehensive provenance tracking"""
    # Record command reconstruction for metadata
    command = f"tasks build-telegram-db '{dataset_path}' '{output_path}'"
    
    # Call the new build_telegram_db function
    return build_telegram_db(dataset_path, output_path)