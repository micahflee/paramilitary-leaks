from dataclasses import dataclass, field
from typing import List, Set


@dataclass
class Message:
    id: str
    timestamp: str
    sender: str
    text: str
    media_note: str
    media_filename: str


@dataclass
class MessagesFile:
    filename: str
    chat_titles: Set[str] = field(default_factory=set)
    messages: List[Message] = field(default_factory=list)
