from datetime import datetime

from pydantic import BaseModel


class TokenCreate(BaseModel):
    token: str


class QueueItemRead(BaseModel):
    id: int
    token: str
    status: str
    created_at: datetime
    processed_at: datetime | None = None

    class Config:
        from_attributes = True
