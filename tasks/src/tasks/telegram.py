import os
from typing import List


def is_messages_filename(filename: str) -> bool:
    return filename.startswith("messages") and filename.endswith(".html")


def find_messages_files(path: str) -> List[str]:
    "Find all messages files in the given path"

    messages_files: List[str] = []

    for root, dirs, files in os.walk(path):
        messages_files.extend(
            [os.path.join(root, f) for f in files if is_messages_filename(f)]
        )

    return messages_files


def build(dataset_path, output_path):
    chat_export_files = find_messages_files(dataset_path)
    print(f"Found {len(chat_export_files)} chat export files")
    for chat_export_file in chat_export_files:
        print(f"Processing '{chat_export_file}'")

    # TODO: finish
