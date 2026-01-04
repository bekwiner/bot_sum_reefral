import os
from dotenv import load_dotenv

load_dotenv()


OWNER_ID = int(os.getenv("OWNER_ID", "5023757011"))
OWNER2_ID = int(os.getenv("OWNER2_ID", "6320742043"))

_raw = os.getenv("REQUIRED_CHANNELS", "")
REQUIRED_CHANNELS_DEFAULT = [s.strip() for s in _raw.split(",") if s.strip()]
