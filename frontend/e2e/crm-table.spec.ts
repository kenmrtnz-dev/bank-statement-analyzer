import { expect, test } from '@playwright/test';

type Row = {
  id: string;
  type: string;
  created_at: string;
  account_name: string;
  assigned_user: string;
  attachment_id: string;
  filename: string;
  content_type: string;
  size_bytes: number;
  status: string;
  error: string;
  download_url: string;
  process_job_id: string;
  process_status: string;
  process_step: string;
  process_progress: number;
};

const rows: Row[] = Array.from({ length: 25 }, (_, i) => {
  const n = i + 1;
  return {
    id: `lead-${n}`,
    type: 'Lead',
    created_at: `2026-02-${String((n % 28) + 1).padStart(2, '0')}`,
    account_name: n === 17 ? 'Target Business Name' : `Business ${n}`,
    assigned_user: 'QA User',
    attachment_id: `att-${n}`,
    filename: `statement-${n}.pdf`,
    content_type: 'application/pdf',
    size_bytes: 1234,
    status: 'available',
    error: '',
    download_url: `/crm/attachments/att-${n}/file`,
    process_job_id: '',
    process_status: 'not_started',
    process_step: '',
    process_progress: 0,
  };
});

test.describe('CRM table smoke', () => {
  test.beforeEach(async ({ page }) => {
    await page.route('**/crm/attachments**', async (route) => {
      const url = new URL(route.request().url());
      const limit = Number(url.searchParams.get('limit') || '12');
      const offset = Number(url.searchParams.get('offset') || '0');
      const q = (url.searchParams.get('q') || '').toLowerCase();
      const filtered = q
        ? rows.filter((r) => `${r.account_name} ${r.filename} ${r.id} ${r.attachment_id}`.toLowerCase().includes(q))
        : rows;
      const pageRows = filtered.slice(offset, offset + limit);
      const hasMore = offset + pageRows.length < filtered.length;
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          items: pageRows,
          lead_count: filtered.length,
          account_count: 0,
          attachment_count: pageRows.length,
          offset,
          limit,
          next_offset: offset + pageRows.length,
          has_more: hasMore,
          probe_mode: 'lazy',
        }),
      });
    });
  });

  test('supports pagination and global search', async ({ page }) => {
    await page.goto('/uploads');

    const crmSection = page.locator('#crmAttachmentsSection');
    await expect(crmSection).toBeVisible();

    const bodyRows = page.locator('#crmAttachmentsRowsBody tr');
    await expect(bodyRows).toHaveCount(12);

    await expect(page.locator('#crmPageInfo')).toContainText('Showing 1-12');
    await page.click('#crmNextBtn');
    await expect(page.locator('#crmPageInfo')).toContainText('Showing 13-24');

    await page.fill('#crmSearch', 'Target Business Name');
    await expect(page.locator('#crmPageInfo')).toContainText('Showing 1-1');
    await expect(page.locator('#crmAttachmentsRowsBody tr')).toHaveCount(1);
    await expect(page.locator('#crmAttachmentsRowsBody')).toContainText('Target Business Name');
  });

  test('retains CRM table area height on low-row pages', async ({ page }) => {
    test.fail(true, 'Known bug: CRM table area shrinks on last page when row count is below page size.');
    await page.goto('/uploads');
    const wrap = page.locator('#crmAttachmentsTableWrap');
    const h1 = await wrap.evaluate((el) => Math.round(el.getBoundingClientRect().height));

    await page.click('#crmNextBtn');
    await page.click('#crmNextBtn');
    await expect(page.locator('#crmPageInfo')).toContainText('Showing 25-25');

    const h2 = await wrap.evaluate((el) => Math.round(el.getBoundingClientRect().height));
    expect(Math.abs(h1 - h2)).toBeLessThanOrEqual(2);
  });
});
