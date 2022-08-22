from ast import Str
import secrets
from pytz import timezone
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseSettings

class Settings(BaseSettings):
    KAFKA_BROKER_URL: str
    APP_NAME: str
    SCHEMA_REGISTRY_URL: str
    METRIC_TOPIC: str
    METRIC_VALUE: str
    THRESHOLD_TOPIC: str
    OUTPUT_TOPIC: str
    PARTITIONS: int

  
settings = Settings()