from dataclasses import dataclass


@dataclass
class Message:
    id: str
    timestamp: str
    sender: str
    text: str
