from app.modules.jobs.service import compute_summary


def test_compute_summary_includes_credit_monthly_average_and_monthly_counts():
    rows = [
        {"date": "01/01/2026", "debit": 100, "credit": None, "balance": 900},
        {"date": "01/02/2026", "debit": None, "credit": 300, "balance": 1200},
        {"date": "01/03/2026", "debit": 50, "credit": None, "balance": 1150},
        {"date": "01/04/2026", "debit": None, "credit": 200, "balance": 1350},
    ]

    summary = compute_summary(rows)

    assert summary["total_credit"] == 500.0
    # (Total Credit / 6) * 30% => (500/6) * 0.3 = 25.0
    assert summary["total_credit_monthly_average"] == 25.0

    assert len(summary["monthly"]) == 1
    jan = summary["monthly"][0]
    assert jan["month"] == "2026-01"
    assert jan["debit_count"] == 2
    assert jan["credit_count"] == 2
