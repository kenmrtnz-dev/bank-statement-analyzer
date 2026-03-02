import { expect, test } from '@playwright/test';

const RUN_LIVE = process.env.E2E_RUN_LIVE === '1';
const SAMPLE_JOB_ID = String(process.env.E2E_SAMPLE_JOB_ID || '').trim();

test.describe('Live integration @live', () => {
  test.beforeEach(async () => {
    test.skip(!RUN_LIVE, 'Set E2E_RUN_LIVE=1 to run live integration tests.');
  });

  test('uploads page renders core evaluator UI', async ({ page }) => {
    const pageErrors: string[] = [];
    page.on('pageerror', (err) => pageErrors.push(err.message));

    await page.goto('/uploads');

    await expect(page).toHaveURL(/\/uploads$/);
    await expect(page.locator('#menuUploads')).toHaveAttribute('aria-current', 'page');
    await expect(page.locator('#crmAttachmentsSection')).toBeVisible();
    await expect(page.locator('#crmSearch')).toBeVisible();
    await expect(page.locator('#crmRefreshBtn')).toBeVisible();
    await expect(page.locator('#logoutBtn')).toBeVisible();

    expect(pageErrors, `Unexpected page errors: ${pageErrors.join(' | ')}`).toHaveLength(0);
  });

  test('CRM search sends q query to backend', async ({ page }) => {
    const crmRequests: string[] = [];
    page.on('request', (req) => {
      const url = req.url();
      if (url.includes('/crm/attachments?')) crmRequests.push(url);
    });

    await page.goto('/uploads');
    await expect(page.locator('#crmRefreshBtn')).toHaveText('Refresh CRM Files');
    await page.fill('#crmSearch', 'alpha');
    await page.waitForTimeout(700);

    const hasQuery = crmRequests.some((raw) => {
      const u = new URL(raw);
      return (u.searchParams.get('q') || '') === 'alpha';
    });

    expect(hasQuery).toBeTruthy();
  });

  test('processing page renders core controls for a real job id', async ({ page }) => {
    test.skip(!SAMPLE_JOB_ID, 'Set E2E_SAMPLE_JOB_ID to validate processing job UI against a real job.');

    await page.goto(`/processing?job-id=${encodeURIComponent(SAMPLE_JOB_ID)}`);

    await expect(page).toHaveURL(new RegExp(`/processing\\?job-id=${SAMPLE_JOB_ID}`));
    await expect(page.locator('#jobId')).toContainText(SAMPLE_JOB_ID);
    await expect(page.locator('#exportPdf')).toBeVisible();
    await expect(page.locator('#exportExcel')).toBeVisible();
    await expect(page.locator('#exportCrm')).toBeVisible();

    const checkbox = page.locator('input.summary-monthly-checkbox').first();
    if ((await checkbox.count()) > 0) {
      const before = await checkbox.isChecked();
      await checkbox.click();
      if (before) await expect(checkbox).not.toBeChecked();
      else await expect(checkbox).toBeChecked();
    }
  });

  test('logout routes back to login', async ({ page }) => {
    await page.goto('/uploads');
    await page.click('#logoutBtn');
    await expect(page).toHaveURL(/\/login$/);
  });
});
