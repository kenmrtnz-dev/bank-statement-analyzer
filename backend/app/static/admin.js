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
  const jobsResultCount = document.getElementById('jobsResultCount');
  const jobsFilterForm = document.getElementById('jobsFilterForm');
  const jobsFilterJobId = document.getElementById('jobsFilterJobId');
  const jobsFilterOwner = document.getElementById('jobsFilterOwner');
  const jobsFilterStatus = document.getElementById('jobsFilterStatus');
  const jobsFilterQuery = document.getElementById('jobsFilterQuery');
  const resetJobsFiltersBtn = document.getElementById('resetJobsFiltersBtn');
  const jobsRowsBody = document.getElementById('jobsRowsBody');
  const jobsPageInfo = document.getElementById('jobsPageInfo');
  const jobsPrevBtn = document.getElementById('jobsPrevBtn');
  const jobsNextBtn = document.getElementById('jobsNextBtn');
  const jobsFeedback = document.getElementById('jobsFeedback');
  const jobsResultMeta = document.getElementById('jobsResultMeta');
  const jobsResultReadyTag = document.getElementById('jobsResultReadyTag');
  const jobsResultSummaryTotalTx = document.getElementById('jobsResultSummaryTotalTx');
  const jobsResultSummaryTotalDebit = document.getElementById('jobsResultSummaryTotalDebit');
  const jobsResultSummaryTotalCredit = document.getElementById('jobsResultSummaryTotalCredit');
  const jobsResultSummaryEndingBalance = document.getElementById('jobsResultSummaryEndingBalance');
  const jobsResultRowsBody = document.getElementById('jobsResultRowsBody');
  const jobsResultRowsMeta = document.getElementById('jobsResultRowsMeta');
  const jobsResultPdfLink = document.getElementById('jobsResultPdfLink');
  const jobsResultExcelLink = document.getElementById('jobsResultExcelLink');
  const volumeSetsCount = document.getElementById('volumeSetsCount');
  const refreshVolumeSetsBtn = document.getElementById('refreshVolumeSetsBtn');
  const volumeSetsRowsBody = document.getElementById('volumeSetsRowsBody');
  const volumeSetMeta = document.getElementById('volumeSetMeta');
  const volumeSetReadyTag = document.getElementById('volumeSetReadyTag');
  const refreshVolumeSetDetailBtn = document.getElementById('refreshVolumeSetDetailBtn');
  const startNextVolumeFileBtn = document.getElementById('startNextVolumeFileBtn');
  const volumeSetUploader = document.getElementById('volumeSetUploader');
  const volumeSetNextFile = document.getElementById('volumeSetNextFile');
  const volumeSetPendingCount = document.getElementById('volumeSetPendingCount');
  const volumeSetActiveCount = document.getElementById('volumeSetActiveCount');
  const volumeFilesRowsBody = document.getElementById('volumeFilesRowsBody');
  const volumeSetsFeedback = document.getElementById('volumeSetsFeedback');
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
  let jobsPage = 1;
  let jobsTotalPages = 1;
  let selectedVolumeSetName = '';
  let transactionsPage = 1;
  let transactionsTotalPages = 1;
  let bankCodeRowsState = [];
  const TAB_META = {
    accounts: {
      label: 'Accounts',
      description: 'Provision evaluator access and review the authentication model.'
    },
    jobs: {
      label: 'Jobs',
      description: 'Track all user jobs, progress, and parsed outputs from a single admin workspace.'
    },
    'volume-tests': {
      label: 'Volume Tests',
      description: 'Start saved VT files one by one and route completed jobs back to the original uploader.'
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

  async function apiJson(url, options = {}) {
    const res = await fetch(url, options);
    if (res.status === 401) {
      window.location.href = '/login';
      throw new Error('not_authenticated');
    }
    const raw = await res.text();
    if (!res.ok) {
      let detail = raw;
      try {
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed === 'object' && parsed.detail) {
          detail = String(parsed.detail);
        }
      } catch (_err) {
        // Keep raw text fallback.
      }
      throw new Error(detail || `HTTP ${res.status}`);
    }
    if (!raw.trim()) {
      return {};
    }
    try {
      return JSON.parse(raw);
    } catch (_err) {
      return {};
    }
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

  function formatBytes(value) {
    const bytes = Number(value || 0);
    if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let size = bytes;
    let unitIndex = 0;
    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex += 1;
    }
    return `${size.toFixed(size >= 10 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
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
    const rawValue = value;
    const raw = String(rawValue || '').trim();
    if (!raw) {
      return '<span class="transactions-cell-muted">-</span>';
    }
    if (typeof rawValue === 'number' && Number.isFinite(rawValue)) {
      const numericDate = new Date(rawValue < 1e12 ? rawValue * 1000 : rawValue);
      if (!Number.isNaN(numericDate.getTime())) {
        return escapeHtml(numericDate.toLocaleString());
      }
    }
    if (/^\d{10,13}(\.\d+)?$/.test(raw)) {
      const numeric = Number(raw);
      const numericDate = new Date(raw.length <= 10 ? numeric * 1000 : numeric);
      if (!Number.isNaN(numericDate.getTime())) {
        return escapeHtml(numericDate.toLocaleString());
      }
    }
    const parsed = new Date(raw);
    if (Number.isNaN(parsed.getTime())) {
      return escapeHtml(raw);
    }
    return escapeHtml(parsed.toLocaleString());
  }

  function formatJobStatusLabel(value) {
    const normalized = String(value || '').trim().toLowerCase();
    if (!normalized) return 'Unknown';
    if (normalized === 'done') return 'Done';
    if (normalized === 'done_with_warnings') return 'Done (Warnings)';
    if (normalized === 'queued') return 'Queued';
    if (normalized === 'processing') return 'Processing';
    if (normalized === 'failed') return 'Failed';
    if (normalized === 'cancelled') return 'Cancelled';
    return normalized.replaceAll('_', ' ').replace(/(^|\s)\S/g, (match) => match.toUpperCase());
  }

  function formatJobOwner(row = {}) {
    const owner = String(row.owner_username || '').trim();
    if (!owner) return '<span class="transactions-cell-muted">Unknown</span>';
    const role = String(row.owner_role || '').trim();
    if (!role) return escapeHtml(owner);
    return `${escapeHtml(owner)} <span class="transactions-cell-muted">(${escapeHtml(role)})</span>`;
  }

  function showJobsFeedback(message, isError = false) {
    showInlineFeedback(jobsFeedback, message, isError);
  }

  function clearJobsFeedback() {
    jobsFeedback?.classList.add('hidden');
  }

  function showVolumeSetsFeedback(message, isError = false) {
    showInlineFeedback(volumeSetsFeedback, message, isError);
  }

  function clearVolumeSetsFeedback() {
    volumeSetsFeedback?.classList.add('hidden');
  }

  function setJobsResultLoadingState(message = 'Select a job above to view parsed results.') {
    if (jobsResultRowsBody) {
      jobsResultRowsBody.innerHTML = `<tr><td colspan="7">${escapeHtml(message)}</td></tr>`;
    }
    setText(jobsResultMeta, message);
    setText(jobsResultRowsMeta, message);
    setText(jobsResultReadyTag, 'Not selected');
    setText(jobsResultSummaryTotalTx, '-');
    setText(jobsResultSummaryTotalDebit, '-');
    setText(jobsResultSummaryTotalCredit, '-');
    setText(jobsResultSummaryEndingBalance, '-');
    jobsResultPdfLink?.classList.add('hidden');
    jobsResultExcelLink?.classList.add('hidden');
    if (jobsResultPdfLink) jobsResultPdfLink.removeAttribute('href');
    if (jobsResultExcelLink) jobsResultExcelLink.removeAttribute('href');
  }

  function renderJobsResultSummary(summary = {}) {
    const totalTx = Number(summary.total_transactions || 0);
    setText(jobsResultSummaryTotalTx, Number.isFinite(totalTx) ? totalTx.toLocaleString() : '-');

    const totalDebit = Number(summary.total_debit);
    setText(
      jobsResultSummaryTotalDebit,
      Number.isFinite(totalDebit)
        ? totalDebit.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
        : '-'
    );

    const totalCredit = Number(summary.total_credit);
    setText(
      jobsResultSummaryTotalCredit,
      Number.isFinite(totalCredit)
        ? totalCredit.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
        : '-'
    );

    const endingBalance = Number(summary.ending_balance);
    setText(
      jobsResultSummaryEndingBalance,
      Number.isFinite(endingBalance)
        ? endingBalance.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
        : '-'
    );
  }

  function renderJobsResultRows(rows = []) {
    if (!jobsResultRowsBody) return;
    if (!rows.length) {
      jobsResultRowsBody.innerHTML = '<tr><td colspan="7">No parsed rows available for this job.</td></tr>';
      return;
    }
    jobsResultRowsBody.innerHTML = rows.map((row) => `
      <tr>
        <td>${escapeHtml(row.page_key || '-')}</td>
        <td>${escapeHtml(`${row.row_index || '-'} / ${row.row_id || '-'}`)}</td>
        <td>${escapeHtml(row.date || '-')}</td>
        <td class="transactions-cell-description">${escapeHtml(row.description || '-')}</td>
        <td>${formatAmount(row.debit)}</td>
        <td>${formatAmount(row.credit)}</td>
        <td>${formatAmount(row.balance)}</td>
      </tr>
    `).join('');
  }

  function renderJobsTable(payload = {}) {
    if (!jobsRowsBody) return;
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    const pagination = payload.pagination && typeof payload.pagination === 'object' ? payload.pagination : {};
    jobsPage = Number(pagination.page || 1) || 1;
    jobsTotalPages = Number(pagination.total_pages || 1) || 1;
    const totalRows = Number(pagination.total_rows || 0) || 0;

    setText(jobsResultCount, pluralize(totalRows, 'job'));
    if (jobsPageInfo) {
      jobsPageInfo.textContent = `Page ${jobsPage} of ${jobsTotalPages} • ${totalRows} job(s)`;
    }
    if (jobsPrevBtn) jobsPrevBtn.disabled = jobsPage <= 1;
    if (jobsNextBtn) jobsNextBtn.disabled = jobsPage >= jobsTotalPages;

    if (!rows.length) {
      jobsRowsBody.innerHTML = '<tr><td colspan="8">No jobs found.</td></tr>';
      return;
    }

    jobsRowsBody.innerHTML = rows.map((row) => {
      const progress = Math.max(0, Math.min(100, Number(row.progress || 0)));
      const status = String(row.status || '').trim().toLowerCase() || 'unknown';
      const statusClass = status.replace(/[^a-z0-9_-]/g, '');
      return `
        <tr>
          <td>
            <strong>${escapeHtml(row.job_id || '-')}</strong>
            <div class="subtle-id">${escapeHtml(row.created_at || '-')}</div>
          </td>
          <td>${formatJobOwner(row)}</td>
          <td>${escapeHtml(row.original_filename || '-')}</td>
          <td>
            <span class="jobs-status-pill jobs-status-${escapeHtml(statusClass)}">${escapeHtml(formatJobStatusLabel(status))}</span>
            <div class="subtle-id">${escapeHtml(row.step || '-')}</div>
          </td>
          <td>${escapeHtml(`${progress}%`)}</td>
          <td>${escapeHtml(row.parse_mode || row.requested_mode || '-')}</td>
          <td>${formatTimestamp(row.updated_at)}</td>
          <td>
            <button class="ghost-button jobs-view-result-btn" type="button" data-job-id="${escapeHtml(row.job_id || '')}">
              View Results
            </button>
          </td>
        </tr>
      `;
    }).join('');
  }

  function setJobsLoadingState(message = 'Loading jobs…') {
    if (jobsRowsBody) {
      jobsRowsBody.innerHTML = `<tr><td colspan="8">${escapeHtml(message)}</td></tr>`;
    }
    setText(jobsResultCount, message === 'Loading jobs…' ? 'Loading…' : 'Unavailable');
  }

  function buildJobsQuery(page = 1) {
    const params = new URLSearchParams();
    params.set('page', String(Math.max(1, Number(page) || 1)));
    params.set('limit', '20');
    const jobId = String(jobsFilterJobId?.value || '').trim();
    const owner = String(jobsFilterOwner?.value || '').trim();
    const status = String(jobsFilterStatus?.value || '').trim();
    const query = String(jobsFilterQuery?.value || '').trim();
    if (jobId) params.set('job_id', jobId);
    if (query) params.set('q', query);
    if (owner) params.set('owner', owner);
    if (status) params.set('status', status);
    return params;
  }

  async function loadJobs(page = 1) {
    if (!jobsRowsBody) return;
    setJobsLoadingState();
    clearJobsFeedback();
    try {
      const params = buildJobsQuery(page);
      const payload = await apiJson(`/admin/jobs?${params.toString()}`);
      renderJobsTable(payload);
    } catch (err) {
      setJobsLoadingState('Failed to load jobs.');
      showJobsFeedback(`Failed to load jobs: ${err.message}`, true);
    }
  }

  async function loadJobResult(jobId) {
    const cleanedJobId = String(jobId || '').trim();
    if (!cleanedJobId) return;
    setJobsResultLoadingState(`Loading results for ${cleanedJobId}…`);
    try {
      const payload = await apiJson(`/admin/jobs/${encodeURIComponent(cleanedJobId)}/result?limit=50`);
      const resultPayload = payload && typeof payload === 'object' ? payload : {};
      const summary = resultPayload.summary && typeof resultPayload.summary === 'object' ? resultPayload.summary : {};
      const results = resultPayload.results && typeof resultPayload.results === 'object' ? resultPayload.results : {};
      const rows = Array.isArray(results.rows) ? results.rows : [];
      const totalRows = Number(results.total_rows || 0) || 0;
      const ready = Boolean(results.ready);
      const statusLabel = formatJobStatusLabel(resultPayload.status || '');
      const ownerLabel = String(resultPayload.owner_username || '').trim() || 'unknown owner';
      setText(
        jobsResultMeta,
        `Job ${cleanedJobId} • ${statusLabel} • ${ownerLabel}`
      );
      setText(jobsResultReadyTag, ready ? 'Results Ready' : 'Not Ready');
      setText(
        jobsResultRowsMeta,
        ready
          ? `Showing ${rows.length} of ${totalRows} row(s).`
          : 'Results are not available for this job yet.'
      );
      renderJobsResultSummary(summary);
      renderJobsResultRows(rows);

      const downloads = resultPayload.downloads && typeof resultPayload.downloads === 'object' ? resultPayload.downloads : {};
      const pdfUrl = String(downloads.pdf || '').trim();
      const excelUrl = String(downloads.excel || '').trim();
      if (pdfUrl) {
        jobsResultPdfLink?.classList.remove('hidden');
        if (jobsResultPdfLink) jobsResultPdfLink.href = pdfUrl;
      } else {
        jobsResultPdfLink?.classList.add('hidden');
      }
      if (excelUrl) {
        jobsResultExcelLink?.classList.remove('hidden');
        if (jobsResultExcelLink) jobsResultExcelLink.href = excelUrl;
      } else {
        jobsResultExcelLink?.classList.add('hidden');
      }
    } catch (err) {
      setJobsResultLoadingState(`Failed to load job results: ${err.message}`);
      showJobsFeedback(`Failed to load job results: ${err.message}`, true);
    }
  }

  function formatVolumeOwner(row = {}) {
    const owner = String(row.uploader_username || '').trim();
    if (!owner) return '<span class="transactions-cell-muted">Unknown</span>';
    const role = String(row.uploader_role || '').trim();
    if (!role) return escapeHtml(owner);
    return `${escapeHtml(owner)} <span class="transactions-cell-muted">(${escapeHtml(role)})</span>`;
  }

  function formatVolumeStatusLabel(status, isPdf = true) {
    const normalized = String(status || '').trim().toLowerCase();
    if (!isPdf || normalized === 'unsupported') return 'Unsupported';
    if (!normalized || normalized === 'pending') return 'Pending';
    if (normalized === 'done') return 'Done';
    if (normalized === 'done_with_warnings') return 'Done (Warnings)';
    if (normalized === 'queued') return 'Queued';
    if (normalized === 'processing') return 'Processing';
    if (normalized === 'failed') return 'Failed';
    if (normalized === 'cancelled') return 'Cancelled';
    return normalized.replaceAll('_', ' ').replace(/(^|\s)\S/g, (match) => match.toUpperCase());
  }

  function formatVolumeStatusClass(status, isPdf = true) {
    const normalized = String(status || '').trim().toLowerCase();
    if (!isPdf || normalized === 'unsupported') return 'unsupported';
    if (!normalized || normalized === 'pending') return 'pending';
    return normalized.replace(/[^a-z0-9_-]/g, '') || 'pending';
  }

  function syncSelectedVolumeSetHighlight() {
    if (!volumeSetsRowsBody) return;
    Array.from(volumeSetsRowsBody.querySelectorAll('tr[data-set-name]')).forEach((row) => {
      const isSelected = row.getAttribute('data-set-name') === selectedVolumeSetName;
      row.classList.toggle('volume-row-selected', isSelected);
    });
  }

  function setVolumeSetLoadingState(message = 'Select a saved set to review its files and start the next VT job.') {
    setText(volumeSetMeta, message);
    setText(volumeSetReadyTag, 'Not selected');
    setText(volumeSetUploader, '-');
    setText(volumeSetNextFile, '-');
    setText(volumeSetPendingCount, '0');
    setText(volumeSetActiveCount, '0');
    if (refreshVolumeSetDetailBtn) refreshVolumeSetDetailBtn.disabled = !selectedVolumeSetName;
    if (startNextVolumeFileBtn) startNextVolumeFileBtn.disabled = true;
    if (volumeFilesRowsBody) {
      volumeFilesRowsBody.innerHTML = `<tr><td colspan="6">${escapeHtml(message)}</td></tr>`;
    }
  }

  function renderVolumeSetsTable(payload = {}) {
    if (!volumeSetsRowsBody) return;
    const rows = Array.isArray(payload.items) ? payload.items : [];
    setText(volumeSetsCount, pluralize(rows.length, 'set'));
    if (!rows.length) {
      volumeSetsRowsBody.innerHTML = '<tr><td colspan="8">No saved VT sets found.</td></tr>';
      return;
    }

    volumeSetsRowsBody.innerHTML = rows.map((row) => `
      <tr data-set-name="${escapeHtml(row.set_name || '')}" class="${selectedVolumeSetName === row.set_name ? 'volume-row-selected' : ''}">
        <td>
          <strong>${escapeHtml(row.set_name || '-')}</strong>
          <div class="subtle-id">${escapeHtml(row.next_file_name || 'No pending file')}</div>
        </td>
        <td>${formatVolumeOwner(row)}</td>
        <td>${escapeHtml(String(row.file_count ?? 0))}</td>
        <td>${escapeHtml(String(row.pending_count ?? 0))}</td>
        <td>${escapeHtml(String(row.active_count ?? 0))}</td>
        <td>${escapeHtml(String(row.completed_count ?? 0))}</td>
        <td>${formatTimestamp(row.updated_at)}</td>
        <td>
          <div class="volume-inline-actions">
            <button class="ghost-button volume-view-set-btn" type="button" data-set-name="${escapeHtml(row.set_name || '')}">View Files</button>
            <button class="volume-start-next-btn" type="button" data-set-name="${escapeHtml(row.set_name || '')}" ${row.next_file_name ? '' : 'disabled'}>
              Start Next
            </button>
          </div>
        </td>
      </tr>
    `).join('');
    syncSelectedVolumeSetHighlight();
  }

  function renderVolumeSetDetail(item = null) {
    if (!item || typeof item !== 'object') {
      setVolumeSetLoadingState();
      return;
    }
    selectedVolumeSetName = String(item.set_name || '').trim();
    const uploaderName = String(item.uploader_username || '').trim();
    const uploaderRole = String(item.uploader_role || '').trim();
    const uploaderLabel = uploaderName ? `${uploaderName}${uploaderRole ? ` (${uploaderRole})` : ''}` : 'Unknown';
    const nextFile = String(item.next_file_name || '').trim() || '-';
    const hasActiveJob = Boolean(item.has_active_job);
    const files = Array.isArray(item.files) ? item.files : [];

    setText(volumeSetMeta, `Set ${selectedVolumeSetName} • ${files.length} file(s)`);
    setText(volumeSetReadyTag, hasActiveJob ? 'Active Job' : 'Ready');
    setText(volumeSetUploader, uploaderLabel);
    setText(volumeSetNextFile, nextFile);
    setText(volumeSetPendingCount, String(item.pending_count ?? 0));
    setText(volumeSetActiveCount, String(item.active_count ?? 0));
    if (refreshVolumeSetDetailBtn) refreshVolumeSetDetailBtn.disabled = !selectedVolumeSetName;
    if (startNextVolumeFileBtn) startNextVolumeFileBtn.disabled = !selectedVolumeSetName || !String(item.next_file_name || '').trim();

    if (!files.length) {
      if (volumeFilesRowsBody) volumeFilesRowsBody.innerHTML = '<tr><td colspan="6">This set has no files.</td></tr>';
      return;
    }

    if (volumeFilesRowsBody) {
      volumeFilesRowsBody.innerHTML = files.map((row) => {
        const volumeStatus = String(row.volume_status || '').trim().toLowerCase();
        const statusClass = formatVolumeStatusClass(volumeStatus, Boolean(row.is_pdf));
        const canStart = Boolean(row.can_start);
        const hasJob = Boolean(row.job_id);
        const actionLabel = canStart ? 'Start File' : hasJob ? 'Open Job' : 'Unavailable';
        const actionClass = canStart ? 'volume-start-file-btn' : hasJob ? 'jobs-view-result-btn ghost-button' : 'ghost-button';
        const actionAttrs = canStart
          ? `data-set-name="${escapeHtml(selectedVolumeSetName)}" data-file-name="${escapeHtml(row.file_name || '')}"`
          : hasJob
            ? `data-job-id="${escapeHtml(row.job_id || '')}"`
            : '';
        return `
          <tr>
            <td>
              <strong>${escapeHtml(row.file_name || '-')}</strong>
              <div class="subtle-id">${escapeHtml(row.last_started_at || row.job_id || '-')}</div>
            </td>
            <td>${escapeHtml(formatBytes(row.size_bytes))}</td>
            <td>
              <span class="jobs-status-pill jobs-status-${escapeHtml(statusClass)}">${escapeHtml(formatVolumeStatusLabel(volumeStatus, Boolean(row.is_pdf)))}</span>
              <div class="subtle-id">${escapeHtml(row.job_step || row.parse_mode || '-')}</div>
            </td>
            <td>
              ${row.job_id ? `<strong>${escapeHtml(row.job_id)}</strong>` : '<span class="transactions-cell-muted">-</span>'}
            </td>
            <td>${formatTimestamp(row.updated_at || row.last_started_at)}</td>
            <td>
              <button class="${actionClass}" type="button" ${actionAttrs} ${canStart || hasJob ? '' : 'disabled'}>
                ${escapeHtml(actionLabel)}
              </button>
            </td>
          </tr>
        `;
      }).join('');
    }
    syncSelectedVolumeSetHighlight();
  }

  async function loadVolumeSets(selectedSetName = selectedVolumeSetName, { skipDetail = false } = {}) {
    if (!volumeSetsRowsBody) return;
    if (refreshVolumeSetsBtn) {
      refreshVolumeSetsBtn.disabled = true;
      refreshVolumeSetsBtn.textContent = 'Refreshing…';
    }
    clearVolumeSetsFeedback();
    try {
      const payload = await apiJson(`/admin/volume-sets?_=${Date.now()}`, { cache: 'no-store' });
      renderVolumeSetsTable(payload);
      const rows = Array.isArray(payload.items) ? payload.items : [];
      if (selectedSetName && !skipDetail) {
        const stillExists = rows.some((row) => String(row.set_name || '').trim() === selectedSetName);
        if (stillExists) await loadVolumeSetDetail(selectedSetName);
        else {
          selectedVolumeSetName = '';
          setVolumeSetLoadingState();
        }
      }
    } catch (err) {
      setText(volumeSetsCount, 'Unavailable');
      volumeSetsRowsBody.innerHTML = '<tr><td colspan="8">Failed to load saved VT sets.</td></tr>';
      showVolumeSetsFeedback(`Failed to load saved VT sets: ${err.message}`, true);
    } finally {
      if (refreshVolumeSetsBtn) {
        refreshVolumeSetsBtn.disabled = false;
        refreshVolumeSetsBtn.textContent = 'Refresh Sets';
      }
    }
  }

  async function loadVolumeSetDetail(setName) {
    const cleanedSetName = String(setName || '').trim();
    if (!cleanedSetName) {
      selectedVolumeSetName = '';
      setVolumeSetLoadingState();
      return;
    }
    selectedVolumeSetName = cleanedSetName;
    setVolumeSetLoadingState(`Loading ${cleanedSetName}…`);
    clearVolumeSetsFeedback();
    try {
      const payload = await apiJson(`/admin/volume-sets/${encodeURIComponent(cleanedSetName)}?_=${Date.now()}`, { cache: 'no-store' });
      renderVolumeSetDetail(payload.item);
      syncSelectedVolumeSetHighlight();
    } catch (err) {
      setVolumeSetLoadingState(`Failed to load ${cleanedSetName}.`);
      showVolumeSetsFeedback(`Failed to load set details: ${err.message}`, true);
    }
  }

  async function startVolumeFile(setName, fileName = '') {
    const cleanedSetName = String(setName || '').trim();
    const cleanedFileName = String(fileName || '').trim();
    if (!cleanedSetName || !cleanedFileName) return;
    clearVolumeSetsFeedback();
    try {
      const payload = await apiJson(
        `/admin/volume-sets/${encodeURIComponent(cleanedSetName)}/files/${encodeURIComponent(cleanedFileName)}/start`,
        { method: 'POST' }
      );
      renderVolumeSetDetail(payload.item);
      await loadVolumeSets(cleanedSetName, { skipDetail: true });
      showVolumeSetsFeedback(`Started ${cleanedFileName} as VT job ${payload.job_id}.`);
    } catch (err) {
      showVolumeSetsFeedback(`Failed to start ${cleanedFileName}: ${err.message}`, true);
    }
  }

  async function startNextVolumeFile() {
    const cleanedSetName = String(selectedVolumeSetName || '').trim();
    if (!cleanedSetName) return;
    clearVolumeSetsFeedback();
    try {
      const payload = await apiJson(`/admin/volume-sets/${encodeURIComponent(cleanedSetName)}/start-next`, { method: 'POST' });
      renderVolumeSetDetail(payload.item);
      await loadVolumeSets(cleanedSetName, { skipDetail: true });
      showVolumeSetsFeedback(`Started ${payload.file_name} as VT job ${payload.job_id}.`);
    } catch (err) {
      showVolumeSetsFeedback(`Failed to start the next VT file: ${err.message}`, true);
    }
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
      const payload = await apiJson(`/admin/job-transactions?${params.toString()}`);
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
      if (jobsFilterForm) jobsFilterForm.style.display = 'none';
      if (transactionsFilterForm) transactionsFilterForm.style.display = 'none';
      return false;
    }
    return true;
  }

  async function loadSettings() {
    const payload = await apiJson(`/admin/settings?_=${Date.now()}`, { cache: 'no-store' });
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
      await apiJson('/admin/evaluators', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
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
      await apiJson('/admin/settings/upload-testing', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled })
      });
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
      const payload = await apiJson('/admin/settings/bank-code-flags', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rows })
      });
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

  jobsFilterForm?.addEventListener('submit', async (e) => {
    e.preventDefault();
    await loadJobs(1);
  });

  resetJobsFiltersBtn?.addEventListener('click', async () => {
    if (jobsFilterJobId) jobsFilterJobId.value = '';
    if (jobsFilterOwner) jobsFilterOwner.value = '';
    if (jobsFilterStatus) jobsFilterStatus.value = '';
    if (jobsFilterQuery) jobsFilterQuery.value = '';
    await loadJobs(1);
  });

  jobsPrevBtn?.addEventListener('click', async () => {
    if (jobsPage <= 1) return;
    await loadJobs(jobsPage - 1);
  });

  jobsNextBtn?.addEventListener('click', async () => {
    if (jobsPage >= jobsTotalPages) return;
    await loadJobs(jobsPage + 1);
  });

  jobsRowsBody?.addEventListener('click', async (e) => {
    const target = e.target;
    if (!(target instanceof HTMLElement)) return;
    const btn = target.closest('.jobs-view-result-btn');
    if (!(btn instanceof HTMLElement)) return;
    const jobId = String(btn.dataset.jobId || '').trim();
    if (!jobId) return;
    const url = `/processing?job-id=${encodeURIComponent(jobId)}`;
    const opened = window.open(url, '_blank', 'noopener,noreferrer');
    if (!opened) {
      window.location.href = url;
    }
  });

  refreshVolumeSetsBtn?.addEventListener('click', async () => {
    await loadVolumeSets(selectedVolumeSetName, { skipDetail: false });
  });

  refreshVolumeSetDetailBtn?.addEventListener('click', async () => {
    if (!selectedVolumeSetName) return;
    await loadVolumeSetDetail(selectedVolumeSetName);
  });

  startNextVolumeFileBtn?.addEventListener('click', async () => {
    await startNextVolumeFile();
  });

  volumeSetsRowsBody?.addEventListener('click', async (e) => {
    const target = e.target;
    if (!(target instanceof HTMLElement)) return;
    const viewBtn = target.closest('.volume-view-set-btn');
    if (viewBtn instanceof HTMLElement) {
      const setName = String(viewBtn.dataset.setName || '').trim();
      if (!setName) return;
      await loadVolumeSetDetail(setName);
      return;
    }

    const startBtn = target.closest('.volume-start-next-btn');
    if (startBtn instanceof HTMLElement) {
      const setName = String(startBtn.dataset.setName || '').trim();
      if (!setName) return;
      selectedVolumeSetName = setName;
      await startNextVolumeFile();
    }
  });

  volumeFilesRowsBody?.addEventListener('click', async (e) => {
    const target = e.target;
    if (!(target instanceof HTMLElement)) return;
    const startBtn = target.closest('.volume-start-file-btn');
    if (startBtn instanceof HTMLElement) {
      const setName = String(startBtn.dataset.setName || '').trim();
      const fileName = String(startBtn.dataset.fileName || '').trim();
      if (!setName || !fileName) return;
      await startVolumeFile(setName, fileName);
      return;
    }

    const openBtn = target.closest('.jobs-view-result-btn');
    if (!(openBtn instanceof HTMLElement)) return;
    const jobId = String(openBtn.dataset.jobId || '').trim();
    if (!jobId) return;
    const url = `/processing?job-id=${encodeURIComponent(jobId)}`;
    const opened = window.open(url, '_blank', 'noopener,noreferrer');
    if (!opened) {
      window.location.href = url;
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
      const payload = await apiJson('/admin/clear-store', { method: 'POST' });
      try {
        window.localStorage.removeItem(UPLOADS_CACHE_KEY);
      } catch (_err) {
        // no-op
      }
      show(
        `Cleared ${payload.cleared_jobs} jobs, ${payload.cleared_exports} exports, and ${payload.cleared_db_rows} DB rows.`
      );
      setJobsResultLoadingState('Select a job above to view parsed results.');
      await loadJobs(1);
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
  setJobsResultLoadingState('Select a job above to view parsed results.');

  requireAdmin()
    .then((ok) => {
      if (!ok) return;
      return loadSettings().then(() => Promise.all([loadJobs(1), loadVolumeSets(), loadTransactions(1)]));
    })
    .catch(() => show('Failed to verify admin session.', true));
})();
