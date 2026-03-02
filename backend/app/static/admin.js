(() => {
  const UPLOADS_CACHE_KEY = 'bsa_uploaded_jobs_v1';
  const msg = document.getElementById('adminMessage');
  const tabButtons = Array.from(document.querySelectorAll('.admin-nav-btn[data-tab-target]'));
  const tabPanels = Array.from(document.querySelectorAll('.admin-panel[data-tab-panel]'));
  const activeTabLabel = document.getElementById('activeTabLabel');
  const activeTabTitle = document.getElementById('activeTabTitle');
  const activeTabDescription = document.getElementById('activeTabDescription');
  const adminUserBadge = document.getElementById('adminUserBadge');
  const sessionUserLabel = document.getElementById('sessionUserLabel');
  const sessionSummaryUser = document.getElementById('sessionSummaryUser');
  const sessionRoleLabel = document.getElementById('sessionRoleLabel');
  const overviewActiveSection = document.getElementById('overviewActiveSection');
  const overviewFlagRules = document.getElementById('overviewFlagRules');
  const overviewStoredRows = document.getElementById('overviewStoredRows');
  const overviewUploadTesting = document.getElementById('overviewUploadTesting');
  const headerUploadTestingStatus = document.getElementById('headerUploadTestingStatus');
  const sidebarUploadTestingStatus = document.getElementById('sidebarUploadTestingStatus');
  const transactionsResultCount = document.getElementById('transactionsResultCount');
  const flagCodeRuleCountTag = document.getElementById('flagCodeRuleCountTag');
  const createForm = document.getElementById('createEvaluatorForm');
  const createFeedback = document.getElementById('createEvaluatorFeedback');
  const clearForm = document.getElementById('clearStoreForm');
  const featureToggleForm = document.getElementById('featureToggleForm');
  const uploadTestingToggle = document.getElementById('uploadTestingToggle');
  const featureToggleFeedback = document.getElementById('featureToggleFeedback');
  const bankCodeFlagsForm = document.getElementById('bankCodeFlagsForm');
  const flagCodesFilterForm = document.getElementById('flagCodesFilterForm');
  const flagFilterBankId = document.getElementById('flagFilterBankId');
  const flagFilterBankName = document.getElementById('flagFilterBankName');
  const flagFilterQuery = document.getElementById('flagFilterQuery');
  const resetFlagCodesFiltersBtn = document.getElementById('resetFlagCodesFiltersBtn');
  const bankCodeRowsBody = document.getElementById('bankCodeRowsBody');
  const bankCodesTableWrap = document.querySelector('.admin-panel[data-tab-panel="flag-codes"] .bank-codes-table-wrap');
  const addBankCodeRowBtn = document.getElementById('addBankCodeRowBtn');
  const bankCodeFlagsFeedback = document.getElementById('bankCodeFlagsFeedback');
  const transactionsFilterForm = document.getElementById('transactionsFilterForm');
  const txFilterJobId = document.getElementById('txFilterJobId');
  const txFilterPageKey = document.getElementById('txFilterPageKey');
  const txFilterQuery = document.getElementById('txFilterQuery');
  const resetTransactionsFiltersBtn = document.getElementById('resetTransactionsFiltersBtn');
  const transactionsRowsBody = document.getElementById('transactionsRowsBody');
  const transactionsPageInfo = document.getElementById('transactionsPageInfo');
  const transactionsPrevBtn = document.getElementById('transactionsPrevBtn');
  const transactionsNextBtn = document.getElementById('transactionsNextBtn');
  const transactionsFeedback = document.getElementById('transactionsFeedback');
  const logoutBtn = document.getElementById('adminLogoutBtn');
  const confirmModal = document.getElementById('confirmModal');
  const confirmCancelBtn = document.getElementById('confirmCancelBtn');
  const confirmClearBtn = document.getElementById('confirmClearBtn');
  let transactionsPage = 1;
  let transactionsTotalPages = 1;
  let bankCodeRowsState = [];
  const TAB_META = {
    accounts: {
      label: 'Accounts',
      description: 'Provision evaluator access and review the authentication model.'
    },
    transactions: {
      label: 'Transactions',
      description: 'Audit stored parser output with filters and page-by-page inspection.'
    },
    'flag-codes': {
      label: 'Flag Codes',
      description: 'Maintain code-based parser flags for supported bank transaction rules.'
    },
    settings: {
      label: 'Settings',
      description: 'Adjust admin controls and run high-impact maintenance actions.'
    }
  };

  function setText(el, value) {
    if (!el) return;
    el.textContent = value;
  }

  function pluralize(value, singular, plural = `${singular}s`) {
    return `${value.toLocaleString()} ${value === 1 ? singular : plural}`;
  }

  function updateTabHeader(tabName) {
    const meta = TAB_META[tabName] || TAB_META.accounts;
    setText(activeTabLabel, meta.label);
    setText(activeTabTitle, meta.label);
    setText(activeTabDescription, meta.description);
    setText(overviewActiveSection, meta.label);
  }

  function setActiveTab(tabName, { updateHash = true } = {}) {
    const nextTab = TAB_META[tabName] ? tabName : 'accounts';
    tabButtons.forEach((btn) => {
      const isActive = btn.dataset.tabTarget === nextTab;
      btn.classList.toggle('is-active', isActive);
      btn.setAttribute('aria-pressed', isActive ? 'true' : 'false');
    });
    tabPanels.forEach((panel) => {
      const isActive = panel.dataset.tabPanel === nextTab;
      panel.classList.toggle('is-active', isActive);
      panel.setAttribute('aria-hidden', isActive ? 'false' : 'true');
    });
    updateTabHeader(nextTab);
    if (updateHash) {
      window.history.replaceState(null, '', nextTab === 'accounts' ? '/admin' : `/admin#${nextTab}`);
    }
  }

  function updateUploadTestingState(enabled) {
    const statusText = enabled ? 'Enabled' : 'Disabled';
    setText(overviewUploadTesting, statusText);
    setText(headerUploadTestingStatus, statusText);
    setText(sidebarUploadTestingStatus, statusText);
  }

  function updateBankCodeRuleCount(rows = null) {
    const normalizedRows = Array.isArray(rows) ? rows : collectBankCodeRowsPayload();
    const count = normalizedRows.length;
    setText(overviewFlagRules, String(count));
    setText(flagCodeRuleCountTag, pluralize(count, 'row'));
  }

  function updateStoredRowsCount(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric) || numeric < 0) return;
    setText(overviewStoredRows, numeric.toLocaleString());
    setText(transactionsResultCount, pluralize(numeric, 'row'));
  }

  function showInlineFeedback(el, message, isError = false) {
    if (!el) return;
    el.textContent = message;
    el.classList.remove('hidden');
    el.style.color = isError ? '#a22e45' : '#1f6d44';
    el.style.background = isError ? '#fdeef2' : '#edf9f2';
    el.style.borderColor = isError ? '#f5ccd8' : '#cae8d5';
  }

  function show(message, isError = false) {
    if (!msg) return;
    msg.textContent = message;
    msg.classList.remove('hidden');
    msg.style.color = isError ? '#a22e45' : '#1e6d43';
    msg.style.background = isError ? '#fdeef2' : '#e7f8ed';
    msg.style.borderColor = isError ? '#f5ccd8' : '#bde7cb';
  }

  function showCreateFeedback(message, isError = false) {
    showInlineFeedback(createFeedback, message, isError);
  }

  function showFeatureFeedback(message, isError = false) {
    showInlineFeedback(featureToggleFeedback, message, isError);
  }

  function showBankCodeFeedback(message, isError = false) {
    showInlineFeedback(bankCodeFlagsFeedback, message, isError);
  }

  function showTransactionsFeedback(message, isError = false) {
    showInlineFeedback(transactionsFeedback, message, isError);
  }

  function clearTransactionsFeedback() {
    transactionsFeedback?.classList.add('hidden');
  }

  function escapeHtml(value) {
    return String(value ?? '')
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }

  function formatAmount(value) {
    if (value == null || value === '') {
      return '<span class="transactions-cell-muted">-</span>';
    }
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return escapeHtml(value);
    }
    return escapeHtml(
      numeric.toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      })
    );
  }

  function formatBounds(bounds) {
    const x1 = Number(bounds?.x1);
    const y1 = Number(bounds?.y1);
    const x2 = Number(bounds?.x2);
    const y2 = Number(bounds?.y2);
    if (![x1, y1, x2, y2].every((value) => Number.isFinite(value))) {
      return '<span class="transactions-cell-muted">-</span>';
    }
    return `${x1.toFixed(3)}, ${y1.toFixed(3)}, ${x2.toFixed(3)}, ${y2.toFixed(3)}`;
  }

  function formatTimestamp(value) {
    const raw = String(value || '').trim();
    if (!raw) {
      return '<span class="transactions-cell-muted">-</span>';
    }
    const parsed = new Date(raw);
    if (Number.isNaN(parsed.getTime())) {
      return escapeHtml(raw);
    }
    return escapeHtml(parsed.toLocaleString());
  }

  function renderTransactionsTable(payload = {}) {
    if (!transactionsRowsBody) return;
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    const pagination = payload.pagination && typeof payload.pagination === 'object' ? payload.pagination : {};
    transactionsPage = Number(pagination.page || 1) || 1;
    transactionsTotalPages = Number(pagination.total_pages || 1) || 1;
    const totalRows = Number(pagination.total_rows || 0) || 0;
    updateStoredRowsCount(totalRows);

    if (!rows.length) {
      transactionsRowsBody.innerHTML = '<tr><td colspan="10">No transactions found.</td></tr>';
    } else {
      transactionsRowsBody.innerHTML = rows.map((row) => `
        <tr>
          <td>${escapeHtml(row.job_id || '')}</td>
          <td>${escapeHtml(row.page_key || '')}</td>
          <td>${escapeHtml(`${row.row_index || '-'} / ${row.row_id || '-'}`)}</td>
          <td>${escapeHtml(row.date || '-')}</td>
          <td class="transactions-cell-description">${escapeHtml(row.description || '-')}</td>
          <td>${formatAmount(row.debit)}</td>
          <td>${formatAmount(row.credit)}</td>
          <td>${formatAmount(row.balance)}</td>
          <td class="transactions-cell-bounds">${formatBounds(row.bounds)}</td>
          <td>${formatTimestamp(row.updated_at)}</td>
        </tr>
      `).join('');
    }

    if (transactionsPageInfo) {
      transactionsPageInfo.textContent = `Page ${transactionsPage} of ${transactionsTotalPages} • ${totalRows} row(s)`;
    }
    if (transactionsPrevBtn) transactionsPrevBtn.disabled = transactionsPage <= 1;
    if (transactionsNextBtn) transactionsNextBtn.disabled = transactionsPage >= transactionsTotalPages;
  }

  function setTransactionsLoadingState(message = 'Loading transactions…') {
    if (!transactionsRowsBody) return;
    const countLabel = message === 'Loading transactions…' ? 'Loading…' : 'Unavailable';
    setText(transactionsResultCount, countLabel);
    transactionsRowsBody.innerHTML = `<tr><td colspan="10">${escapeHtml(message)}</td></tr>`;
  }

  function buildTransactionsQuery(page = 1) {
    const params = new URLSearchParams();
    params.set('page', String(Math.max(1, Number(page) || 1)));
    params.set('limit', '50');

    const jobId = String(txFilterJobId?.value || '').trim();
    const pageKey = String(txFilterPageKey?.value || '').trim();
    const query = String(txFilterQuery?.value || '').trim();
    if (jobId) params.set('job_id', jobId);
    if (pageKey) params.set('page_key', pageKey);
    if (query) params.set('q', query);
    return params;
  }

  async function loadTransactions(page = 1) {
    if (!transactionsRowsBody) return;
    setTransactionsLoadingState();
    clearTransactionsFeedback();
    try {
      const params = buildTransactionsQuery(page);
      const res = await fetch(`/admin/job-transactions?${params.toString()}`);
      if (!res.ok) throw new Error(await res.text());
      const payload = await res.json();
      renderTransactionsTable(payload);
    } catch (err) {
      setTransactionsLoadingState('Failed to load transactions.');
      showTransactionsFeedback(`Failed to load transactions: ${err.message}`, true);
    }
  }

  function normalizeBankId(value) {
    return String(value || '').trim().toUpperCase().replaceAll(' ', '_').replace(/[^A-Z0-9_-]/g, '');
  }

  function normalizeBankName(value) {
    return String(value || '').trim().toUpperCase();
  }

  function normalizeTxCode(value) {
    return String(value || '').trim().toUpperCase().replace(/[^A-Z0-9_-]/g, '');
  }

  function normalizeBankCodeRow(row = {}) {
    return {
      bank_id: normalizeBankId(row.bank_id),
      bank_name: normalizeBankName(row.bank_name),
      tx_code: normalizeTxCode(row.tx_code),
      particulars: String(row.particulars || '').trim(),
      _isDraft: Boolean(row._isDraft)
    };
  }

  function rowMatchesFlagCodeFilters(row) {
    if (row && row._isDraft) return true;
    const bankIdFilter = String(flagFilterBankId?.value || '').trim().toUpperCase();
    const bankNameFilter = String(flagFilterBankName?.value || '').trim().toUpperCase();
    const query = String(flagFilterQuery?.value || '').trim().toUpperCase();
    const bankId = String(row.bank_id || '').toUpperCase();
    const bankName = String(row.bank_name || '').toUpperCase();
    const txCode = String(row.tx_code || '').toUpperCase();
    const particulars = String(row.particulars || '').toUpperCase();

    if (bankIdFilter && !bankId.includes(bankIdFilter)) return false;
    if (bankNameFilter && !bankName.includes(bankNameFilter)) return false;
    if (query && !`${bankId} ${bankName} ${txCode} ${particulars}`.includes(query)) return false;
    return true;
  }

  function renderBankCodeRows() {
    if (!bankCodeRowsBody) return;
    const filteredRows = bankCodeRowsState.filter((row) => rowMatchesFlagCodeFilters(row));
    if (!filteredRows.length) {
      bankCodeRowsBody.innerHTML = '<tr><td colspan="5">No flag code rows found.</td></tr>';
      updateBankCodeRuleCount();
      return;
    }

    bankCodeRowsBody.innerHTML = filteredRows.map((row) => {
      const rowIndex = bankCodeRowsState.indexOf(row);
      if (row._isDraft) {
        return `
          <tr data-row-index="${rowIndex}" class="flag-codes-draft-row">
            <td><input class="bank-code-id-input" type="text" value="${escapeHtml(row.bank_id || '')}" placeholder="e.g. 1" /></td>
            <td><input class="bank-code-name-input" type="text" value="${escapeHtml(row.bank_name || '')}" placeholder="e.g. BANCO DE ORO" /></td>
            <td><input class="bank-code-tx-input" type="text" value="${escapeHtml(row.tx_code || '')}" placeholder="e.g. ASC" /></td>
            <td><input class="bank-code-particulars-input" type="text" value="${escapeHtml(row.particulars || '')}" placeholder="transaction description" /></td>
            <td><button class="bank-code-remove-btn" type="button" aria-label="Remove row" title="Remove row">&times;</button></td>
          </tr>
        `;
      }
      return `
        <tr data-row-index="${rowIndex}">
          <td>${escapeHtml(row.bank_id || '-')}</td>
          <td>${escapeHtml(row.bank_name || '-')}</td>
          <td>${escapeHtml(row.tx_code || '-')}</td>
          <td class="flag-codes-cell-particulars" title="${escapeHtml(row.particulars || '')}">${escapeHtml(row.particulars || '-')}</td>
          <td><button class="bank-code-remove-btn" type="button" aria-label="Remove row" title="Remove row">&times;</button></td>
        </tr>
      `;
    }).join('');
    updateBankCodeRuleCount();
  }

  function appendBankCodeRow(row = {}) {
    bankCodeRowsState.push(normalizeBankCodeRow({ ...row, _isDraft: true }));
    renderBankCodeRows();
    window.requestAnimationFrame(() => {
      if (bankCodesTableWrap) {
        bankCodesTableWrap.scrollTop = bankCodesTableWrap.scrollHeight;
      }
      const lastDraftRow = bankCodeRowsBody?.querySelector('tr.flag-codes-draft-row:last-child');
      if (lastDraftRow instanceof HTMLElement) {
        lastDraftRow.scrollIntoView({ block: 'end' });
      }
    });
  }

  function setBankCodeRows(rows) {
    bankCodeRowsState = Array.isArray(rows)
      ? rows.map((row) => normalizeBankCodeRow({ ...row, _isDraft: false }))
      : [];
    renderBankCodeRows();
  }

  function collectBankCodeRowsPayload() {
    const payload = [];
    const seen = new Set();
    for (const item of bankCodeRowsState) {
      const row = normalizeBankCodeRow(item);
      if (!row.bank_id || !row.bank_name || !row.tx_code) continue;
      const key = `${row.bank_id}::${row.tx_code}`;
      const particularsKey = String(row.particulars || '').trim().toUpperCase();
      const dedupeKey = `${key}::${particularsKey}`;
      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);
      payload.push(row);
    }
    return payload;
  }

  function openConfirmModal() {
    confirmModal?.classList.remove('hidden');
  }

  function closeConfirmModal() {
    confirmModal?.classList.add('hidden');
  }

  async function requireAdmin() {
    const me = await fetch('/auth/me');
    if (me.status === 401) {
      window.location.href = '/login';
      return false;
    }
    const payload = await me.json();
    const username = String(payload.username || 'admin');
    const role = String(payload.role || '').toLowerCase();
    const roleLabel = role ? `${role.charAt(0).toUpperCase()}${role.slice(1)}` : 'Admin';
    setText(adminUserBadge, username);
    setText(sessionUserLabel, username);
    setText(sessionSummaryUser, username);
    setText(sessionRoleLabel, roleLabel);
    if (role !== 'admin') {
      show('Admin access required.', true);
      if (createForm) createForm.style.display = 'none';
      if (clearForm) clearForm.style.display = 'none';
      if (featureToggleForm) featureToggleForm.style.display = 'none';
      if (flagCodesFilterForm) flagCodesFilterForm.style.display = 'none';
      if (bankCodeFlagsForm) bankCodeFlagsForm.style.display = 'none';
      if (transactionsFilterForm) transactionsFilterForm.style.display = 'none';
      return false;
    }
    return true;
  }

  async function loadSettings() {
    const res = await fetch(`/admin/settings?_=${Date.now()}`, { cache: 'no-store' });
    if (!res.ok) throw new Error(await res.text());
    const payload = await res.json();
    const uploadTestingEnabled = Boolean(payload.upload_testing_enabled);
    if (uploadTestingToggle) uploadTestingToggle.checked = uploadTestingEnabled;
    updateUploadTestingState(uploadTestingEnabled);
    const rows = Array.isArray(payload.bank_code_flag_rows)
      ? payload.bank_code_flag_rows.map((item) => ({
        bank_id: String(item?.bank_id || ''),
        bank_name: String(item?.bank_name || ''),
        tx_code: String(item?.tx_code || ''),
        particulars: String(item?.particulars || '')
      }))
      : [];
    setBankCodeRows(rows);
    updateBankCodeRuleCount(rows);
  }

  createForm?.addEventListener('submit', async (e) => {
    e.preventDefault();
    try {
      const payload = {
        username: String(document.getElementById('evUsername')?.value || '').trim(),
        password: String(document.getElementById('evPassword')?.value || '')
      };
      const res = await fetch('/admin/evaluators', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (!res.ok) throw new Error(await res.text());
      showCreateFeedback(`Evaluator "${payload.username}" created.`);
      createForm.reset();
    } catch (err) {
      showCreateFeedback(`Failed to create evaluator: ${err.message}`, true);
    }
  });

  clearForm?.addEventListener('submit', async (e) => {
    e.preventDefault();
    openConfirmModal();
  });

  featureToggleForm?.addEventListener('submit', async (e) => {
    e.preventDefault();
    try {
      const enabled = Boolean(uploadTestingToggle?.checked);
      const res = await fetch('/admin/settings/upload-testing', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled })
      });
      if (!res.ok) throw new Error(await res.text());
      updateUploadTestingState(enabled);
      showFeatureFeedback(`Upload testing section ${enabled ? 'enabled' : 'disabled'}.`);
    } catch (err) {
      showFeatureFeedback(`Failed to save toggle: ${err.message}`, true);
    }
  });

  addBankCodeRowBtn?.addEventListener('click', () => {
    appendBankCodeRow({ bank_id: '', bank_name: '', tx_code: '', particulars: '' });
  });

  flagCodesFilterForm?.addEventListener('submit', (e) => {
    e.preventDefault();
    renderBankCodeRows();
  });

  resetFlagCodesFiltersBtn?.addEventListener('click', () => {
    if (flagFilterBankId) flagFilterBankId.value = '';
    if (flagFilterBankName) flagFilterBankName.value = '';
    if (flagFilterQuery) flagFilterQuery.value = '';
    renderBankCodeRows();
  });

  bankCodeRowsBody?.addEventListener('input', (e) => {
    const target = e.target;
    if (!(target instanceof HTMLInputElement)) return;
    const tr = target.closest('tr');
    const rowIndex = Number(tr?.dataset.rowIndex || '-1');
    if (!Number.isInteger(rowIndex) || rowIndex < 0 || rowIndex >= bankCodeRowsState.length) return;
    const row = { ...bankCodeRowsState[rowIndex] };
    if (!row._isDraft) return;
    if (target.classList.contains('bank-code-id-input')) row.bank_id = target.value;
    if (target.classList.contains('bank-code-name-input')) row.bank_name = target.value;
    if (target.classList.contains('bank-code-tx-input')) row.tx_code = target.value;
    if (target.classList.contains('bank-code-particulars-input')) row.particulars = target.value;
    bankCodeRowsState[rowIndex] = normalizeBankCodeRow(row);
    updateBankCodeRuleCount();
  });

  bankCodeRowsBody?.addEventListener('click', (e) => {
    const target = e.target;
    if (!(target instanceof HTMLElement)) return;
    const btn = target.closest('.bank-code-remove-btn');
    if (!btn) return;
    const tr = btn.closest('tr');
    const rowIndex = Number(tr?.dataset.rowIndex || '-1');
    if (!Number.isInteger(rowIndex) || rowIndex < 0 || rowIndex >= bankCodeRowsState.length) return;
    bankCodeRowsState.splice(rowIndex, 1);
    renderBankCodeRows();
  });

  bankCodeFlagsForm?.addEventListener('submit', async (e) => {
    e.preventDefault();
    try {
      const rows = collectBankCodeRowsPayload();
      if (!rows.length) {
        throw new Error('Please add at least one bank with at least one code.');
      }
      const res = await fetch('/admin/settings/bank-code-flags', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rows })
      });
      if (!res.ok) throw new Error(await res.text());
      const payload = await res.json();
      const updatedRows = Array.isArray(payload.bank_code_flag_rows)
        ? payload.bank_code_flag_rows.map((item) => ({
          bank_id: String(item?.bank_id || ''),
          bank_name: String(item?.bank_name || ''),
          tx_code: String(item?.tx_code || ''),
          particulars: String(item?.particulars || '')
        }))
        : [];
      setBankCodeRows(updatedRows);
      updateBankCodeRuleCount(updatedRows);
      showBankCodeFeedback(`Saved ${updatedRows.length} flag code row(s).`);
    } catch (err) {
      showBankCodeFeedback(`Failed to save bank code flags: ${err.message}`, true);
    }
  });

  transactionsFilterForm?.addEventListener('submit', async (e) => {
    e.preventDefault();
    await loadTransactions(1);
  });

  resetTransactionsFiltersBtn?.addEventListener('click', async () => {
    if (txFilterJobId) txFilterJobId.value = '';
    if (txFilterPageKey) txFilterPageKey.value = '';
    if (txFilterQuery) txFilterQuery.value = '';
    await loadTransactions(1);
  });

  transactionsPrevBtn?.addEventListener('click', async () => {
    if (transactionsPage <= 1) return;
    await loadTransactions(transactionsPage - 1);
  });

  transactionsNextBtn?.addEventListener('click', async () => {
    if (transactionsPage >= transactionsTotalPages) return;
    await loadTransactions(transactionsPage + 1);
  });

  confirmCancelBtn?.addEventListener('click', () => closeConfirmModal());
  confirmModal?.addEventListener('click', (e) => {
    if (e.target === confirmModal) closeConfirmModal();
  });

  confirmClearBtn?.addEventListener('click', async () => {
    closeConfirmModal();
    try {
      const res = await fetch('/admin/clear-store', { method: 'POST' });
      if (!res.ok) throw new Error(await res.text());
      const payload = await res.json();
      try {
        window.localStorage.removeItem(UPLOADS_CACHE_KEY);
      } catch (_err) {
        // no-op
      }
      show(
        `Cleared ${payload.cleared_jobs} jobs, ${payload.cleared_exports} exports, and ${payload.cleared_db_rows} DB rows.`
      );
      await loadTransactions(1);
    } catch (err) {
      show(`Failed to clear store: ${err.message}`, true);
    }
  });

  logoutBtn?.addEventListener('click', async () => {
    try {
      await fetch('/auth/logout', { method: 'POST' });
    } finally {
      window.location.href = '/login';
    }
  });

  tabButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      setActiveTab(btn.dataset.tabTarget || 'accounts');
    });
  });

  window.addEventListener('hashchange', () => {
    const nextTab = String(window.location.hash || '').replace(/^#/, '');
    setActiveTab(nextTab || 'accounts', { updateHash: false });
  });

  const initialTab = String(window.location.hash || '').replace(/^#/, '') || 'accounts';
  setActiveTab(initialTab, { updateHash: false });

  requireAdmin()
    .then((ok) => {
      if (!ok) return;
      return loadSettings().then(() => loadTransactions(1));
    })
    .catch(() => show('Failed to verify admin session.', true));
})();
