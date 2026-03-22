import { expect, test } from '@playwright/test';

test.describe('Auth smoke', () => {
  test.use({ storageState: { cookies: [], origins: [] } });

  test('shows error for invalid credentials', async ({ page }) => {
    await page.goto('/login');
    await page.fill('#username', 'invalid_user');
    await page.fill('#password', 'invalid_pass');
    await page.click('button:has-text("Login")');
    await expect(page.locator('#loginError')).toBeVisible();
  });

  test('admin login redirects to admin console', async ({ page }) => {
    await page.goto('/login');
    await page.fill('#username', 'admin');
    await page.fill('#password', 'admin123');
    await page.click('button:has-text("Login")');
    await expect(page).toHaveURL(/\/admin$/);
    await expect(page.locator('#activeTabLabel')).toContainText('Accounts');
  });
});
