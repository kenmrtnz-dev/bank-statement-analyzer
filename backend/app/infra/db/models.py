from __future__ import annotations

import datetime as dt

from sqlalchemy import Boolean, DateTime, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class JobTransactionRecord(Base):
    __tablename__ = "job_transactions"
    __table_args__ = (
        UniqueConstraint("job_id", "page_key", "row_index", name="uq_job_transactions_job_page_row_index"),
        Index("ix_job_transactions_job_id", "job_id"),
        Index("ix_job_transactions_job_page", "job_id", "page_key"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(36), nullable=False)
    page_key: Mapped[str] = mapped_column(String(32), nullable=False)
    row_index: Mapped[int] = mapped_column(Integer, nullable=False)
    row_id: Mapped[str] = mapped_column(String(64), nullable=False)
    rownumber: Mapped[int | None] = mapped_column(Integer, nullable=True)
    row_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    date: Mapped[str | None] = mapped_column(String(32), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    debit: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    credit: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    balance: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    row_type: Mapped[str] = mapped_column(String(32), nullable=False, default="transaction")
    x1: Mapped[float | None] = mapped_column(Numeric(10, 6), nullable=True)
    y1: Mapped[float | None] = mapped_column(Numeric(10, 6), nullable=True)
    x2: Mapped[float | None] = mapped_column(Numeric(10, 6), nullable=True)
    y2: Mapped[float | None] = mapped_column(Numeric(10, 6), nullable=True)
    is_manual_edit: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class BankCodeFlagRecord(Base):
    __tablename__ = "bank_code_flags"
    __table_args__ = (
        Index("ix_bank_code_flags_bank_id", "bank_id"),
        Index("ix_bank_code_flags_bank_name", "bank_name"),
    )

    bank_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tx_code: Mapped[str] = mapped_column(String(64), primary_key=True)
    particulars: Mapped[str] = mapped_column(String(255), primary_key=True)
    bank_name: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)


__all__ = ["Base", "BankCodeFlagRecord", "JobTransactionRecord"]
