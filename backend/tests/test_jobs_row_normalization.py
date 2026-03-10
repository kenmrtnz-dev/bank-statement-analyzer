from app.jobs import service as jobs_service


def test_normalize_rows_by_page_for_output_preserves_extracted_amount_columns():
    normalized = jobs_service._normalize_rows_by_page_for_output(
        {
            "page_001": [
                {
                    "row_id": "001",
                    "date": "2026-02-01",
                    "description": "Deposit",
                    "debit": "0.00",
                    "credit": "500.00",
                    "balance": "1500.00",
                },
                {
                    "row_id": "002",
                    "date": "2026-02-02",
                    "description": "ATM Withdrawal",
                    "debit": "50.00",
                    "credit": "20.00",
                    "balance": "1450.00",
                },
                {
                    "row_id": "003",
                    "date": "2026-02-03",
                    "description": "UNKNOWN ENTRY",
                    "debit": "40.00",
                    "credit": "60.00",
                    "balance": "1450.00",
                },
            ]
        }
    )

    rows = normalized["page_001"]

    assert rows[0]["debit"] == 0.0
    assert rows[0]["credit"] == 500.0
    assert rows[1]["debit"] == 50.0
    assert rows[1]["credit"] == 20.0
    assert rows[2]["debit"] == 40.0
    assert rows[2]["credit"] == 60.0
