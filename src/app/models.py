from datetime import datetime, timezone
from sqlalchemy import DateTime, String, BigInteger
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


class QueueProcessed(Base):
    """Tabela de processados - só INSERT (fila agora é no Redis)"""
    __tablename__ = "queue_processed"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    token: Mapped[str] = mapped_column(String, nullable=False, index=True)
    enqueued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
