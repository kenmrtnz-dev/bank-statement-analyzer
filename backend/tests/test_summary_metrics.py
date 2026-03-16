from app.jobs.service import compute_summary


def test_compute_summary_includes_monthly_credit_and_disposable_income_and_month_end_adb():
    rows = [
        {"date": "01/01/2026", "debit": 100, "credit": None, "balance": 900},
        {"date": "01/02/2026", "debit": None, "credit": 300, "balance": 1200},
        {"date": "01/03/2026", "debit": 50, "credit": None, "balance": 1150},
        {"date": "01/04/2026", "debit": None, "credit": 200, "balance": 1350},
    ]

    summary = compute_summary(rows)

    assert summary["total_credit"] == 500.0
    assert summary["monthly_credit_average"] == 500.0
    assert summary["monthly_disposable_income"] == 150.0
    assert summary["adb"] == round(1350 / 31, 2)

    assert len(summary["monthly"]) == 1
    jan = summary["monthly"][0]
    assert jan["month"] == "2026-01"
    assert jan["debit_count"] == 2
    assert jan["credit_count"] == 2
    assert jan["adb"] == round(1350 / 31, 2)
