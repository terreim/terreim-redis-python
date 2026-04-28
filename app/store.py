from dataclasses import dataclass
from datetime import datetime

sg_dict = {}

@dataclass
class RedisValue:
    data: any
    expires_at: datetime | None = None