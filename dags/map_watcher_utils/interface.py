
from dataclasses import dataclass
from typing import Union


@dataclass
class ClientConfig:
    client_id: str
    database_id: Union[str, None]
    is_active: bool
