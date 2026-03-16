from __future__ import annotations

import datetime as dt

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class JobTransactionRecord(Base):
    __tablename__ = "job_transactions"
    __table_args__ = (
        UniqueConstraint("job_id", "row_index", name="uq_job_transactions_job_row_index"),
        Index("ix_job_transactions_job_id", "job_id"),
        Index("ix_job_transactions_job_page", "job_id", "page_key"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(36), ForeignKey("jobs.job_id"), nullable=False)
    page_key: Mapped[str] = mapped_column(String(32), nullable=False)
    page_number: Mapped[int] = mapped_column(Integer, nullable=False)
    row_index: Mapped[int] = mapped_column(Integer, nullable=False)
    row_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    date: Mapped[str | None] = mapped_column(String(32), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    debit: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    credit: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    balance: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    row_number_bounds: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    date_bounds: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    debit_bounds: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    credit_bounds: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    balance_bounds: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    row_type: Mapped[str] = mapped_column(String(32), nullable=False, default="transaction")
    is_flagged: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_disbalanced: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    disbalance_expected_balance: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    disbalance_delta: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    is_new_row: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_modified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
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


class JobRecord(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        Index("ix_jobs_processing_status", "processing_status"),
    )

    job_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    file_name: Mapped[str] = mapped_column(String(512), nullable=False)
    file_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    processing_status: Mapped[str] = mapped_column(String(32), nullable=False)
    process_started: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    process_end: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_reversed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class JobResultRawRecord(Base):
    __tablename__ = "job_results_raw"
    __table_args__ = (
        UniqueConstraint("job_id", name="uq_job_results_raw_job_id"),
        Index("ix_job_results_raw_job_id", "job_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(36), nullable=False)
    is_ocr: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    raw_xml: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)


__all__ = ["Base", "BankCodeFlagRecord", "JobRecord", "JobResultRawRecord", "JobTransactionRecord"]
