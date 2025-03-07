# Media Assets Processing Output

Generated on 2025-03-07 14:45:37

## Processing Summary
* Command: `tasks media describe '/Volumes/X10' 'Pro/PARA_LEAKS/ParamilitaryLeaks/' --media-type image`
* Dataset path: `/Volumes/X10`
* Output path: `Pro/PARA_LEAKS/ParamilitaryLeaks/`
* Processed 0 media files out of 0 found
* Generated 0 AI descriptions using google/gemini-flash-1.5-8b
* Encountered 0 errors during processing

## Output Files
* **SQLite database**: `media_assets.db`
  * Contains media asset metadata and AI-generated descriptions
  * Links to related chat messages where possible
  * Full provenance tracking
  
* **CSV exports**:
  * `media_assets.csv`: Complete export of all media metadata and descriptions

## Media Type Breakdown
* Images: 0
* Videos: 0
* Audio: 0
* Documents: 0

## Explore the Data
```bash
# Browse with Datasette
datasette Pro/PARA_LEAKS/ParamilitaryLeaks/media_assets.db

# Example queries:
# - All images with descriptions:
#   SELECT * FROM media_assets WHERE media_type = 'image' AND description IS NOT NULL
# 
# - Media files mentioned in specific chats:
#   SELECT * FROM media_assets WHERE source_chat LIKE '%chat_name%'
```
