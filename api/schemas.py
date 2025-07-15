# api/schemas.py

from pydantic import BaseModel
from typing import List, Optional
from datetime import date, datetime

# Schema for a single message from fct_messages
class Message(BaseModel):
    message_sk: str
    message_id: int
    channel_sk: str
    channel_username: str
    message_date_sk: int
    scrape_date_sk: int
    message_text: Optional[str] = None
    message_length: Optional[int] = None
    views: Optional[int] = None
    forwards: Optional[int] = None
    has_photo: Optional[bool] = None
    photo_path: Optional[str] = None
    is_urgent_message: Optional[bool] = None
    is_vacancy_message: Optional[bool] = None
    message_count: Optional[int] = None

    class Config:
        orm_mode = True # Enable ORM mode for easy conversion from DB rows

# Schema for a single channel from dim_channels
class Channel(BaseModel):
    channel_sk: str
    channel_username: str
    channel_name: str

    class Config:
        orm_mode = True

# Schema for a single date from dim_dates
class Date(BaseModel):
    date_sk: int
    full_date: date
    year: int
    month: int
    month_name: str
    day_of_month: int
    day_of_week_num: int
    day_of_week_name: str
    quarter: int
    week_of_year: int
    day_of_year: int
    is_weekend: bool
    incremental_num: int

    class Config:
        orm_mode = True

# Schema for top products report
class TopProduct(BaseModel):
    product_name: str
    mention_count: int

    class Config:
        orm_mode = True

# Schema for channel activity report
class ChannelActivity(BaseModel):
    activity_date: date
    message_count: int

    class Config:
        orm_mode = True

# Schema for image detection results
class ImageDetection(BaseModel):
    image_detection_sk: str
    message_sk: Optional[str] = None
    detected_object_class: str
    confidence_score: float
    image_path: Optional[str] = None
    detection_timestamp: datetime

    class Config:
        orm_mode = True
