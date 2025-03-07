import os
import re
from bs4 import BeautifulSoup, Tag
from dataclasses import dataclass, field
from datetime import datetime, UTC, timezone, timedelta, tzinfo
from typing import List, Optional, Set
from zoneinfo import ZoneInfo

from .telegram_datatypes import Message
from .telegram_db import db_connect, insert_group_chats, insert_messages

# TODO Handle signatures, which some messages have. Example:
# <div class="signature details">
#  trooper
# </div>


@dataclass
class MessagesFile:
    filename: str
    chat_titles: Set[str] = field(default_factory=set)
    messages: List[Message] = field(default_factory=list)


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


def parse_date_str(date_str: str) -> str:
    """Parse a timestamp from the export's format to ISO8601

    Example inputs:
      - "10.03.2023 07:57:38 MST"
      - "15.09.2024 13:14:54 UTC-07:00"
    """

    matcher = r"^(?P<day>\d{2})\.(?P<month>\d{2})\.(?P<year>\d{4}) (?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}) (?P<tz>.+)$"

    match = re.match(matcher, date_str)
    if not match:
        raise Exception(f"Failed to extract timestamp from {date_str}")

    year = int(match.group("year"))
    month = int(match.group("month"))
    day = int(match.group("day"))
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    second = int(match.group("second"))

    # This is not robust, but seems to be enough for this dataset
    tz: tzinfo
    tz_match = re.match(r"^UTC([+-]\d{2}):\d{2}", match.group("tz"))
    if tz_match:
        hours_offset = int(tz_match.group(1))
        tz = timezone(timedelta(hours=hours_offset))
    else:
        tz = ZoneInfo(match.group("tz"))

    local_datetime = datetime(year, month, day, hour, minute, second, tzinfo=tz)
    utc_datetime = local_datetime.astimezone(UTC)

    return utc_datetime.isoformat()[:18] + "Z"


def text_from_tag(tag: Tag) -> str:
    message_text = "".join(tag.strings).strip()
    # Remove extra whitespace in text
    return " ".join(message_text.split())


def is_system_timestamp_message(div: Tag) -> bool:
    if not div.attrs["id"].startswith("message-"):
        return False

    text = text_from_tag(div)
    if re.match(r"^\d+ [A-Z][a-z]+ \d{4}$", text):
        return True
    else:
        return False


def is_group_title_change_message(div: Tag) -> bool:
    classes = div.attrs["class"]
    text = text_from_tag(div)

    return "service" in classes and " changed group title to " in text


def parse_messages_file(filename: str) -> MessagesFile:
    messages_file = MessagesFile(filename=filename)

    with open(filename) as f:
        html = f.read()

    html_doc = BeautifulSoup(html, "html.parser")

    # Extract the chat name
    chat_title_div = html_doc.find("div", class_="page_header")
    if chat_title_div is None:
        raise Exception(f"Failed to find chat title in {filename}")
    chat_title = "".join(chat_title_div.strings).strip()

    messages_file.chat_titles.add(chat_title)

    for message_div in html_doc.find_all("div", class_="message"):
        message_id = message_div.attrs["id"]

        # Skip system timestamp messages
        if is_system_timestamp_message(message_div):
            continue

        # Handle chat title changes
        if is_group_title_change_message(message_div):
            new_title = "".join(message_div.strings).strip().split("«")[1][:-1]
            messages_file.chat_titles.add(new_title)
            continue

        date_div = message_div.find("div", class_="date")
        if not date_div:
            # TODO May want to handle these
            # Example:
            # <div class="message service" id="message1">
            #   <div class="body details">
            #    Channel «TEST - CHAT» created
            #   </div>
            # </div>
            continue

        raw_timestamp = message_div.find("div", class_="date").attrs["title"]
        iso_timestamp = parse_date_str(raw_timestamp)

        from_name_div = message_div.find("div", class_="from_name")

        if from_name_div:
            sender = text_from_tag(from_name_div)
        else:
            # If the same sender posts multiple messages in a row,
            # "from_name" will be omitted and should be fetched from the
            # previous message
            sender = messages_file.messages[-1].sender

        forwarded_div = message_div.find("div", class_="forwarded")
        media_wrap_div = message_div.find("div", class_="media_wrap")
        reply_to_div = message_div.find("div", class_="reply_to")
        text_div = message_div.find("div", class_="text")

        if forwarded_div:
            # TODO I'm not sure how to handle forwarded messages
            message_text = "Forwarded message"
            # TODO Forwarded messages don't seem to have a "from_name" indicating who forwarded it
            # Example: 2022 midterm election terrorism/america first precinct project/America first precinct project ChatExport_2022-10-24/messages2.html
            sender = "Unknown"
        elif reply_to_div:
            in_reply_to_a = reply_to_div.find("a")

            if in_reply_to_a:
                other_message_id = in_reply_to_a.attrs["href"].split("_")[-1]
                message_text = f"In reply to {other_message_id}: "
            else:
                message_text = text_from_tag(reply_to_div) + ": "
            # TODO There may be cases where there are both?
            if text_div:
                message_text += text_from_tag(text_div)
            elif media_wrap_div:
                message_text += "Media message"
        elif media_wrap_div:
            # TODO I'm not sure how media messages should be handled
            # TODO Do some media messages also include text?
            message_text = "Media message"
            if text_div:
                text = text_from_tag(text_div)
                message_text += f" {text}"
        elif text_div:
            message_text = text_from_tag(text_div)
        else:
            # Empty messages are a thing apparently
            message_text = ""

        message = Message(
            id=message_id, timestamp=iso_timestamp, sender=sender, text=message_text
        )
        messages_file.messages.append(message)

    return messages_file


def build(dataset_path: str, output_path: str) -> None:
    cur = db_connect(output_path)

    chat_export_files = find_messages_files(dataset_path)
    print(f"Found {len(chat_export_files)} chat export files")

    for chat_export_file in chat_export_files:
        print(f"Processing '{chat_export_file}'")
        try:
            messages_file = parse_messages_file(chat_export_file)
        except Exception as e:
            print("Failed parsing", chat_export_file)
            raise e

        group_chat_id = insert_group_chats(cur, list(messages_file.chat_titles))
        insert_messages(cur, group_chat_id, messages_file.messages)

    # TODO: finish

    cur.connection.close()
