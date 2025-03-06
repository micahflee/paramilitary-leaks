import os


def find_chat_export_dirs(path):
    chat_export_dirs = []
    for root, dirs, files in os.walk(path):
        for dir_name in dirs:
            if dir_name.startswith("ChatExport_"):
                relative_path = os.path.relpath(os.path.join(root, dir_name), path)
                chat_export_dirs.append(relative_path)
    return chat_export_dirs


def build(dataset_path, output_path):
    chat_export_dirs = find_chat_export_dirs(dataset_path)
    print(f"Found {len(chat_export_dirs)} chat export directories")
    for chat_export_dir in chat_export_dirs:
        print(f"Processing '{chat_export_dir}'")

    # TODO: finish
