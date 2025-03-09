import os
import os.path
import re
import sqlite3
from bs4 import BeautifulSoup, Tag
from datetime import datetime, UTC, timezone, timedelta, tzinfo
from typing import Callable, List
from zoneinfo import ZoneInfo

from .datatypes import Message, MessagesFile
from .db import insert_group_chats, insert_messages
from .utils import make_rel_path

# TODO Handle signatures, which some messages have. Example:
# <div class="signature details">
#  trooper
# </div>


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


def is_service_message(div: Tag) -> bool:
    return "service" in div.attrs["class"]


def service_message_contains(*args: str) -> Callable[[Tag], bool]:
    def f(tag: Tag) -> bool:
        text = text_from_tag(tag)
        return is_service_message(tag) and all([term in text for term in args])

    return f


def is_channel_title_changed_message(div: Tag) -> bool:
    text = text_from_tag(div)
    return is_service_message(div) and (
        "changed group title to «" in text or "Channel title changed to «" in text
    )


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

        # Handle chat title changes
        if is_channel_title_changed_message(message_div):
            new_title = "".join(message_div.strings).strip().split("«")[1][:-1]
            messages_file.chat_titles.add(new_title)
            continue

        # Messages to skip
        messages_to_skip = [
            is_system_timestamp_message,
            service_message_contains("changed group photo"),
            service_message_contains("changed topic icon to"),
            service_message_contains("changed topic title to"),
            service_message_contains("Channel photo changed"),
            service_message_contains("Channel", "created"),
            service_message_contains("converted a basic group to this supergroup "),
            service_message_contains("converted this group to a supergroup"),
            service_message_contains("created topic"),
            service_message_contains("has set messages to auto-delete"),
            service_message_contains("invited "),
            service_message_contains("joined group by link from"),
            service_message_contains("joined group by request"),
            service_message_contains("pinned ", "this message"),
            service_message_contains("removed"),
            service_message_contains("scheduled a voice chat for"),
            service_message_contains("started voice chat"),
            service_message_contains("Voice chat"),
        ]
        if any([f(message_div) for f in messages_to_skip]):
            continue

        date_div = message_div.find("div", class_="date")
        if not date_div:
            print(message_div.prettify())
            raise Exception("Unable to find date div")

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

        message_text = ""
        media_note = ""
        media_filename = ""

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
            # If there's a text div, add the text
            if text_div:
                message_text = text_from_tag(text_div)
            elif media_wrap_div:
                message_text = "Media message"

            # Photos
            photo_a = media_wrap_div.find("a", class_="media_photo")
            if photo_a:
                media_filename = photo_a.attrs["href"]
                media_note = "photo"

            # Images
            photo_wrap_a = media_wrap_div.find("a", class_="photo_wrap")
            if photo_wrap_a:
                media_filename = photo_wrap_a.attrs["href"]
                media_note = "image"

            # Video files
            video_file_wrap_a = media_wrap_div.find("a", class_="video_file_wrap")
            if video_file_wrap_a:
                media_filename = video_file_wrap_a.attrs["href"]
                media_note = "video"

            # Video messages
            media_video_a = media_wrap_div.find("a", class_="media_video")
            if media_video_a:
                media_filename = media_video_a.attrs["href"]
                media_note = "video_message"

            # Polls
            poll_div = media_wrap_div.find("div", class_="media_poll")
            if poll_div:
                media_note = "poll: " + text_from_tag(
                    poll_div.find("div", class_="question")
                )

            # Files
            file_a = media_wrap_div.find("a", class_="media_file")
            if file_a:
                media_filename = file_a.attrs["href"]
                media_note = "file"

            # Audio files
            media_audio_file_a = media_wrap_div.find("a", class_="media_audio_file")
            if media_audio_file_a:
                media_filename = media_audio_file_a.attrs["href"]
                media_note = "audio"

            # Voice messages
            voice_a = media_wrap_div.find("a", class_="media_voice_message")
            if voice_a:
                media_filename = voice_a.attrs["href"]
                media_note = "voice_message"

            # Animated GIF (technically MP4s though)
            animated_a = media_wrap_div.find("a", class_="animated_wrap")
            if animated_a:
                media_filename = animated_a.attrs["href"]
                media_note = "animated_gif"

            # Stickers
            sticker_a = media_wrap_div.find("a", class_="sticker_wrap")
            if sticker_a:
                media_filename = sticker_a.attrs["href"]
                media_note = "sticker"

            # Contact
            contact_a = media_wrap_div.find("a", class_="media_contact")
            if contact_a:
                media_filename = contact_a.attrs["href"]
                media_note = "contact"

            # Is the media not included?
            if media_filename == "":
                description_div = media_wrap_div.find("div", class_="description")
                if description_div:
                    if (
                        "Not included, change data exporting settings to download"
                        in text_from_tag(description_div)
                    ):
                        media_note = "Media not included"
                    elif (
                        "Exceeds maximum size, change data exporting settings to download"
                        in text_from_tag(description_div)
                    ):
                        media_note = "Media exceeds maximum size"

            if media_note == "" and poll_div is None:
                print(media_wrap_div.prettify())
                raise Exception("Unknown media message type")
        elif text_div:
            message_text = text_from_tag(text_div)
        else:
            # Empty messages are a thing apparently
            message_text = ""

        unique_message_id = f"{message_id}_{iso_timestamp}"

        message = Message(
            id=unique_message_id,
            timestamp=iso_timestamp,
            sender=sender,
            text=message_text,
            media_note=media_note,
            media_filename=media_filename,
        )
        messages_file.messages.append(message)

    return messages_file


def messages_file_path_for_db(
    store_abs: bool, dataset_path: str, messages_file_path: str
) -> str:
    abs_dataset_path = os.path.abspath(dataset_path)
    abs_messages_file_path = os.path.abspath(messages_file_path)

    if store_abs:
        return abs_messages_file_path

    return abs_messages_file_path.removeprefix(abs_dataset_path).removeprefix("/")


def build_telegram_db(
    cur: sqlite3.Cursor, dataset_path: str, absolute_paths: bool
) -> None:
    chat_export_files = find_messages_files(dataset_path)
    print(f"Found {len(chat_export_files)} chat export files")

    for chat_export_file in sorted(chat_export_files):
        print(f"Processing '{chat_export_file}'")
        try:
            messages_file = parse_messages_file(chat_export_file)
        except Exception as e:
            print("Failed parsing", chat_export_file)
            raise e

        group_chat_id = insert_group_chats(cur, list(messages_file.chat_titles))

        if absolute_paths:
            messages_file_path = os.path.abspath(chat_export_file)
        else:
            messages_file_path = make_rel_path(dataset_path, chat_export_file)

        insert_messages(cur, group_chat_id, messages_file_path, messages_file.messages)
