import { expect, test } from '@playwright/test';

test.describe('Processing summary smoke', () => {
  test.beforeEach(async ({ page }) => {
    await page.route('**/crm/attachments**', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          items: [],
          lead_count: 0,
          account_count: 0,
          attachment_count: 0,
          offset: 0,
          limit: 12,
          next_offset: 0,
          has_more: false,
          probe_mode: 'lazy',
        }),
      });
    });

    await page.route('**/jobs/test-job', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          status: 'done',
          step: 'completed',
          progress: 100,
          parse_mode: 'text',
        }),
      });
    });

    await page.route('**/jobs/test-job/cleaned', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ pages: [] }),
      });
    });

    await page.route('**/jobs/test-job/summary', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          total_transactions: 1,
          debit_transactions: 0,
          credit_transactions: 1,
          total_debit: 0,
          total_credit: 100,
          total_credit_monthly_average: 5,
          adb: 100,
          monthly: [
            {
              month: '2026-02',
              debit: 0,
              credit: 100,
              debit_count: 0,
              credit_count: 1,
              avg_debit: 0,
              avg_credit: 100,
              adb: 100,
            },
          ],
        }),
      });
    });
  });

  test('first click unchecks include checkbox and updates summary with one-row monthly data', async ({ page }) => {
    await page.goto('/processing?job-id=test-job');

    const checkbox = page.locator('input.summary-monthly-checkbox').first();
    await expect(checkbox).toBeChecked();

    await checkbox.click();
    await expect(checkbox).not.toBeChecked();

    const metricCards = page.locator('.summary-metric-card');
    const totalCreditCard = metricCards.filter({ hasText: 'Total Credit' }).first();
    await expect(totalCreditCard).toContainText('₱0.00');
  });
});
