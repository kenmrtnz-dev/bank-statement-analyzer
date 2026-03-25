from app.jobs.service import compute_summary


def test_compute_summary_includes_monthly_credit_and_disposable_income_and_average_balance_adb():
    rows = [
        {"date": "01/01/2026", "debit": 100, "credit": None, "balance": 900},
        {"date": "01/02/2026", "debit": None, "credit": 300, "balance": 1200},
        {"date": "01/03/2026", "debit": 50, "credit": None, "balance": 1150},
        {"date": "01/04/2026", "debit": None, "credit": 200, "balance": 1350},
    ]

    summary = compute_summary(rows)

    expected_adb = round((900 + 1200 + 1150 + 1350) / 4, 2)

    assert summary["total_credit"] == 500.0
    assert summary["monthly_credit_average"] == 500.0
    assert summary["monthly_disposable_income"] == 150.0
    assert summary["summary_version"] == 2
    assert summary["adb"] == expected_adb

    assert len(summary["monthly"]) == 1
    jan = summary["monthly"][0]
    assert jan["month"] == "2026-01"
    assert jan["debit_count"] == 2
    assert jan["credit_count"] == 2
    assert jan["adb"] == expected_adb


def test_compute_summary_excludes_opening_balance_rows_from_transaction_counts():
    rows = [
        {
            "date": "01/01/2026",
            "description": "Beginning balance",
            "debit": None,
            "credit": 1000,
            "balance": 1000,
            "row_type": "opening_balance",
        },
        {
            "date": "01/02/2026",
            "description": "Deposit",
            "debit": None,
            "credit": 300,
            "balance": 1300,
            "row_type": "transaction",
        },
        {
            "date": "01/03/2026",
            "description": "Withdrawal",
            "debit": 50,
            "credit": None,
            "balance": 1250,
            "row_type": "transaction",
        },
    ]

    summary = compute_summary(rows)

    assert summary["total_transactions"] == 2
    assert summary["credit_transactions"] == 1
    assert summary["debit_transactions"] == 1
    assert summary["total_credit"] == 300.0
    assert summary["total_debit"] == 50.0
    assert summary["ending_balance"] == 1250.0
    assert summary["adb"] == round((1000 + 1300 + 1250) / 3, 2)
