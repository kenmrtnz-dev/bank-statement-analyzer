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

    await page.route('**/jobs/test-job/parsed/page_001', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify([
          {
            row_id: '001',
            date: '2026-02-01',
            description: 'Transfer AB1 received',
            debit: '',
            credit: '100.00',
            balance: '100.00',
          },
        ]),
      });
    });

    await page.route('**/jobs/test-job/parsed', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          page_001: [
            {
              row_id: '001',
              date: '2026-02-01',
              description: 'Transfer AB1 received',
              debit: '',
              credit: '100.00',
              balance: '100.00',
              is_flagged: false,
            },
          ],
        }),
      });
    });

    await page.route('**/jobs/test-job/rows/page_001/bounds', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify([]),
      });
    });

    await page.route('**/jobs/test-job/bounds', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          page_001: [],
        }),
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

  test('flagged transactions refresh after delayed ui settings load', async ({ page }) => {
    await page.route('**/ui/settings', async (route) => {
      await new Promise((resolve) => setTimeout(resolve, 400));
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          ok: true,
          upload_testing_enabled: false,
          bank_code_flags: [
            {
              bank: 'TEST BANK',
              codes: ['AB1'],
              profile_aliases: ['TEST_BANK'],
            },
          ],
          bank_code_flag_rows: [
            {
              bank_id: 'TEST_BANK',
              bank_name: 'TEST BANK',
              tx_code: 'AB1',
              particulars: '',
            },
          ],
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
        body: JSON.stringify({ pages: ['page_001.png'] }),
      });
    });

    await page.goto('/processing?job-id=test-job');

    await page.getByRole('tab', { name: /Flagged Transactions/ }).click();
    await expect(page.getByRole('tab', { name: 'Flagged Transactions (1)' })).toBeVisible();
    await expect(page.locator('#flaggedRowsBody')).toContainText('Transfer AB1 received');
    await expect(page.locator('#flaggedRowsBody')).toContainText('AB1');
  });
});
