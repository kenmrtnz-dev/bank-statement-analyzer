(() => {
  const STORAGE_KEY = 'bsa_uploaded_jobs_v1';
  const EDITABLE_ROW_FIELDS = ['date', 'description', 'debit', 'credit', 'balance'];
  const AMOUNT_ROW_FIELDS = new Set(['debit', 'credit', 'balance']);
  const ROW_SAVE_DEBOUNCE_MS = 220;

  const els = {
    form: document.getElementById('uploadForm'),
    file: document.getElementById('pdfFile'),
    mode: document.getElementById('mode'),
    startBtn: document.getElementById('startBtn'),
    jobId: document.getElementById('jobId'),
    jobStatus: document.getElementById('jobStatus'),
    jobStep: document.getElementById('jobStep'),
    jobProgress: document.getElementById('jobProgress'),
    jobProgressFill: document.getElementById('jobProgressFill'),
    summary: document.getElementById('summary'),
    summaryEmpty: document.getElementById('summaryEmpty'),
    pageSelect: document.getElementById('pageSelect'),
    pagePrevBtn: document.getElementById('pagePrevBtn'),
    pageNextBtn: document.getElementById('pageNextBtn'),
    pageIndicator: document.getElementById('pageIndicator'),
    previewImage: document.getElementById('previewImage'),
    previewEmpty: document.getElementById('previewEmpty'),
    overlay: document.getElementById('overlay'),
    rowsBody: document.getElementById('rowsBody'),
    rowCount: document.getElementById('rowCount'),
    reverseRowsBtn: document.getElementById('reverseRowsBtn'),
    exportPdf: document.getElementById('exportPdf'),
    exportExcel: document.getElementById('exportExcel'),
    jobIdInput: document.getElementById('jobIdInput'),
    loadJobBtn: document.getElementById('loadJobBtn'),
    uploadsView: document.getElementById('uploadsView'),
    processingView: document.getElementById('processingView'),
    processingGrid: document.querySelector('.processing-main-grid'),
    previewSectionCard: document.querySelector('.processing-main-grid > article.section-card'),
    parsedSectionCard: document.querySelector('.processing-main-grid > section.table-card.section-card'),
    parsedTableWrap: document.getElementById('parsedTableWrap'),
    parsedJsonWrap: document.getElementById('parsedJsonWrap'),
    parsedJsonBody: document.getElementById('parsedJsonBody'),
    parsedDebugToggleBtn: document.getElementById('parsedDebugToggleBtn'),
    menuUploads: document.getElementById('menuUploads'),
    menuProcessing: document.getElementById('menuProcessing'),
    uploadsBadge: document.getElementById('uploadsBadge'),
    uploadRowsBody: document.getElementById('uploadRowsBody'),
    uploadSearch: document.getElementById('uploadSearch'),
    uploadSurface: document.getElementById('uploadSurface'),
    uploadTestingSection: document.getElementById('uploadTestingSection'),
    uploadEmptyState: document.getElementById('uploadEmptyState'),
    uploadTableWrap: document.getElementById('uploadTableWrap'),
    crmAttachmentsSection: document.getElementById('crmAttachmentsSection'),
    crmRefreshBtn: document.getElementById('crmRefreshBtn'),
    crmSearch: document.getElementById('crmSearch'),
    crmLoadMoreBtn: document.getElementById('crmLoadMoreBtn'),
    crmPager: document.getElementById('crmPager'),
    crmPrevBtn: document.getElementById('crmPrevBtn'),
    crmNextBtn: document.getElementById('crmNextBtn'),
    crmPageInfo: document.getElementById('crmPageInfo'),
    crmAttachmentsRowsBody: document.getElementById('crmAttachmentsRowsBody'),
    crmAttachmentsEmptyState: document.getElementById('crmAttachmentsEmptyState'),
    crmAttachmentsTableWrap: document.getElementById('crmAttachmentsTableWrap'),
    uploadProgressWrap: document.getElementById('uploadProgressWrap'),
    uploadProgressBar: document.getElementById('uploadProgressBar'),
    uploadProgressText: document.getElementById('uploadProgressText'),
    logoutBtn: document.getElementById('logoutBtn'),
    adminLink: document.getElementById('adminLink'),
    userRoleLabel: document.getElementById('userRoleLabel')
  };

  const state = {
    jobId: null,
    pages: [],
    parsedByPage: {},
    boundsByPage: {},
    openaiRawByPage: {},
    currentPage: null,
    selectedRowId: null,
    pollTimer: null,
    view: 'uploads',
    uploadedJobs: loadStoredJobs(),
    uploadSearch: '',
    totalParsedRows: 0,
    isCompleted: false,
    authRole: '',
    uploadTestingEnabled: false,
    crmAttachments: [],
    crmAttachmentsError: '',
    crmLoading: false,
    crmLoadingMore: false,
    crmLimit: 25,
    crmOffset: 0,
    crmCurrentOffset: 0,
    crmNextOffset: 0,
    crmHasMore: false,
    crmProbeMode: 'lazy',
    crmProcessByAttachment: {},
    crmStatusTimer: null,
    crmSearch: '',
    pageSaveTimers: {},
    pageSaveTokenByPage: {},
    parsedPanelMode: 'table',
    currentParseMode: ''
  };
  const ROUTE_TO_VIEW = {
    '/uploads': 'uploads',
    '/processing': 'processing',
    '/evaluator': 'uploads'
  };
  const VIEW_TO_ROUTE = {
    uploads: '/uploads',
    processing: '/processing'
  };

  function loadStoredJobs() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      const payload = raw ? JSON.parse(raw) : [];
      return Array.isArray(payload) ? payload : [];
    } catch {
      return [];
    }
  }

  function saveStoredJobs() {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state.uploadedJobs.slice(0, 100)));
    } catch {
      // no-op
    }
  }

  async function reconcileStoredJobsStatuses() {
    if (!Array.isArray(state.uploadedJobs) || state.uploadedJobs.length === 0) return;

    const next = [];
    for (const row of state.uploadedJobs.slice(0, 100)) {
      const jobId = String(row?.jobId || '').trim();
      if (!jobId) continue;

      try {
        const res = await fetch(`/jobs/${encodeURIComponent(jobId)}`);
        if (res.status === 401) {
          window.location.href = '/login';
          return;
        }
        if (res.status === 404) {
          // Remove stale local row when backend no longer has the job.
          continue;
        }
        if (!res.ok) {
          next.push(row);
          continue;
        }
        const payload = await res.json();
        next.push({
          ...row,
          status: payload?.status || row.status || 'queued',
          step: payload?.step || row.step || '',
          progress: Number(payload?.progress ?? row.progress ?? 0)
        });
      } catch {
        next.push(row);
      }
    }

    state.uploadedJobs = next;
    saveStoredJobs();
    renderUploadedRows();
  }

  async function api(url, opts) {
    const res = await fetch(url, opts);
    if (res.status === 401) {
      window.location.href = '/login';
      throw new Error('not_authenticated');
    }
    if (!res.ok) {
      const text = await res.text();
      throw new Error(text || `HTTP ${res.status}`);
    }
    const ct = res.headers.get('content-type') || '';
    if (ct.includes('application/json')) return res.json();
    return res.text();
  }

  function upsertUploadedJob(partial) {
    const id = String(partial.jobId || '').trim();
    if (!id) return;
    const idx = state.uploadedJobs.findIndex((j) => String(j.jobId) === id);
    if (idx >= 0) state.uploadedJobs[idx] = { ...state.uploadedJobs[idx], ...partial };
    else state.uploadedJobs.unshift(partial);
    saveStoredJobs();
    renderUploadedRows();
  }

  function updateUploadedJobIfExists(partial) {
    const id = String(partial.jobId || '').trim();
    if (!id) return;
    const idx = state.uploadedJobs.findIndex((j) => String(j.jobId) === id);
    if (idx < 0) return;
    state.uploadedJobs[idx] = { ...state.uploadedJobs[idx], ...partial };
    saveStoredJobs();
    renderUploadedRows();
  }

  function applyFeatureVisibility() {
    if (els.uploadTestingSection) {
      els.uploadTestingSection.classList.toggle('hidden', !state.uploadTestingEnabled);
    }
  }

  function formatBytes(value) {
    const n = Number(value || 0);
    if (!Number.isFinite(n) || n <= 0) return '-';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let size = n;
    let i = 0;
    while (size >= 1024 && i < units.length - 1) {
      size /= 1024;
      i += 1;
    }
    return `${size.toFixed(size >= 100 || i === 0 ? 0 : 1)} ${units[i]}`;
  }

  function formatDate(ts) {
    const raw = String(ts || '').trim();
    if (!raw) return '-';
    const basic = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (basic) return `${basic[1]}-${basic[2]}-${basic[3]}`;
    const d = new Date(raw.replace(' ', 'T'));
    if (Number.isNaN(d.getTime())) return raw;
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }

  function formatParsedRowDate(value) {
    const raw = String(value ?? '').trim();
    if (!raw) return '';
    const iso = raw.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);
    if (iso) {
      const mm = String(iso[2]).padStart(2, '0');
      const dd = String(iso[3]).padStart(2, '0');
      return `${mm}/${dd}/${iso[1]}`;
    }
    const parsed = new Date(raw);
    if (Number.isNaN(parsed.getTime())) return raw;
    const mm = String(parsed.getMonth() + 1).padStart(2, '0');
    const dd = String(parsed.getDate()).padStart(2, '0');
    const yyyy = String(parsed.getFullYear());
    return `${mm}/${dd}/${yyyy}`;
  }

  function formatNumber(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '-';
    return n.toLocaleString('en-US');
  }

  function formatCurrency(value, currency = '₱') {
    const n = Number(value);
    if (!Number.isFinite(n)) return `${currency}0.00`;
    const abs = Math.abs(n).toLocaleString('en-US', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });
    return `${n < 0 ? '-' : ''}${currency}${abs}`;
  }

  function escapeHtml(value) {
    return String(value ?? '')
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }

  function renderUploadedRows() {
    if (!els.uploadRowsBody) return;
    const search = state.uploadSearch.toLowerCase();
    const rows = state.uploadedJobs.filter((row) => {
      if (!search) return true;
      return `${row.fileName || ''} ${row.jobId || ''}`.toLowerCase().includes(search);
    });
    if (els.uploadsBadge) els.uploadsBadge.textContent = String(state.uploadedJobs.length || 0);
    const hasAnyUploads = state.uploadedJobs.length > 0;
    if (els.uploadEmptyState) els.uploadEmptyState.classList.toggle('hidden', hasAnyUploads);
    if (els.uploadTableWrap) els.uploadTableWrap.classList.toggle('hidden', !hasAnyUploads);

    els.uploadRowsBody.innerHTML = '';
    for (const row of rows) {
      const tr = document.createElement('tr');
      const status = String(row.status || 'queued').toLowerCase();
      const normalizedStatus = status === 'done' ? 'completed' : status;
      const actionMeta =
        normalizedStatus === 'processing'
          ? { label: 'Processing…', action: 'none', disabled: true, className: 'action-processing' }
          : normalizedStatus === 'completed'
            ? { label: 'View Results', action: 'open', disabled: false, className: 'action-completed' }
            : normalizedStatus === 'failed'
              ? { label: 'Retry', action: 'start', disabled: false, className: 'action-failed' }
              : normalizedStatus === 'needs_review'
                ? { label: 'Open Review', action: 'open', disabled: false, className: 'action-review' }
                : { label: 'Begin Processing', action: 'start', disabled: false, className: 'action-queued' };
      tr.innerHTML = `
        <td class="file-name-cell">
          <strong>${escapeHtml(row.fileName || 'document.pdf')}</strong>
          <div class="subtle-id">${escapeHtml(row.jobId || '')}</div>
        </td>
        <td>${escapeHtml(formatBytes(row.sizeBytes))}</td>
        <td>${escapeHtml(formatDate(row.lastModified || row.createdAt))}</td>
        <td>You</td>
        <td><span class="status-pill status-${escapeHtml(normalizedStatus)}">${escapeHtml(normalizedStatus)}</span></td>
        <td>
          <button class="row-action-btn ${actionMeta.className}" type="button" data-action="${actionMeta.action}" data-job-id="${escapeHtml(row.jobId)}" ${actionMeta.disabled ? 'disabled' : ''}>${actionMeta.label}</button>
        </td>
      `;
      els.uploadRowsBody.appendChild(tr);
    }
    if (hasAnyUploads && rows.length === 0) {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td colspan="6" class="table-empty-cell">No matching files.</td>`;
      els.uploadRowsBody.appendChild(tr);
    }
  }

  function setCrmRefreshState() {
    if (!els.crmRefreshBtn) return;
    els.crmRefreshBtn.disabled = state.crmLoading || state.crmLoadingMore;
    els.crmRefreshBtn.textContent = state.crmLoading ? 'Refreshing…' : 'Refresh CRM Files';
  }

  function setCrmLoadMoreState() {
    if (!els.crmLoadMoreBtn) return;
    // Replaced by bottom pagination controls.
    els.crmLoadMoreBtn.classList.add('hidden');
    els.crmLoadMoreBtn.disabled = true;
    els.crmLoadMoreBtn.textContent = 'Load More';
  }

  function setCrmPaginationState() {
    if (!els.crmPager) return;
    const hasRows = Array.isArray(state.crmAttachments) && state.crmAttachments.length > 0;
    const show = !state.crmAttachmentsError && (hasRows || state.crmCurrentOffset > 0 || state.crmHasMore || state.crmLoading);
    els.crmPager.classList.toggle('hidden', !show);

    const from = hasRows ? state.crmCurrentOffset + 1 : 0;
    const to = hasRows ? state.crmCurrentOffset + state.crmAttachments.length : 0;
    if (els.crmPageInfo) els.crmPageInfo.textContent = `Showing ${from}-${to}`;
    if (els.crmPrevBtn) els.crmPrevBtn.disabled = state.crmLoading || state.crmLoadingMore || state.crmCurrentOffset <= 0;
    if (els.crmNextBtn) els.crmNextBtn.disabled = state.crmLoading || state.crmLoadingMore || !state.crmHasMore;
  }

  function normalizeApiErrorMessage(rawMessage) {
    const text = String(rawMessage || '').trim();
    if (!text) return 'unknown_error';
    try {
      const parsed = JSON.parse(text);
      if (parsed && typeof parsed === 'object' && parsed.detail) {
        return String(parsed.detail);
      }
    } catch {
      // no-op
    }
    return text;
  }

  function normalizeProcessStatus(rawStatus) {
    const status = String(rawStatus || '').trim().toLowerCase();
    if (status === 'done') return 'completed';
    if (['queued', 'processing', 'completed', 'failed', 'needs_review'].includes(status)) return status;
    return 'not_started';
  }

  function formatProcessStatusLabel(status) {
    const key = normalizeProcessStatus(status);
    if (key === 'not_started') return 'Not Started';
    if (key === 'needs_review') return 'Needs Review';
    return key.charAt(0).toUpperCase() + key.slice(1);
  }

  function processStatusClass(status) {
    const key = normalizeProcessStatus(status);
    if (key === 'not_started') return 'status-not_started';
    return `status-${key}`;
  }

  function normalizeProcessStepForDisplay(rawStep) {
    const value = String(rawStep || '').trim().toLowerCase();
    if (!value) return '';
    if (value === 'done') return 'completed';
    return value;
  }

  function shouldShowProcessStep(status, step) {
    const normalizedStatus = normalizeProcessStatus(status);
    const normalizedStep = normalizeProcessStepForDisplay(step);
    if (!normalizedStep) return false;
    return normalizedStep !== normalizedStatus;
  }

  function syncCrmProcessMapFromItems(items) {
    const next = {};
    for (const item of items) {
      const attachmentId = String(item?.attachment_id || '').trim();
      if (!attachmentId) continue;
      const previous = state.crmProcessByAttachment[attachmentId] || {};
      const jobId = String(item?.process_job_id || previous.jobId || '').trim();
      const step = String(item?.process_step || previous.step || '').trim();
      const status = normalizeProcessStatus(item?.process_status || previous.status || 'not_started');
      const rawProgress = item?.process_progress ?? previous.progress ?? 0;
      const progress = Number.isFinite(Number(rawProgress)) ? Number(rawProgress) : 0;
      next[attachmentId] = { jobId, status, step, progress };
    }
    state.crmProcessByAttachment = next;
  }

  function startCrmStatusPolling() {
    if (state.crmStatusTimer) return;
    state.crmStatusTimer = window.setInterval(() => {
      refreshCrmProcessStatuses().catch(() => {
        // ignore poll noise
      });
    }, 2500);
  }

  function stopCrmStatusPolling() {
    if (!state.crmStatusTimer) return;
    clearInterval(state.crmStatusTimer);
    state.crmStatusTimer = null;
  }

  async function refreshCrmProcessStatuses() {
    const entries = Object.entries(state.crmProcessByAttachment || {});
    if (!entries.length) return;
    let changed = false;

    await Promise.all(entries.map(async ([attachmentId, processInfo]) => {
      const jobId = String(processInfo?.jobId || '').trim();
      if (!jobId) return;

      const currentStatus = normalizeProcessStatus(processInfo?.status);
      if (!['queued', 'processing'].includes(currentStatus)) return;

      try {
        const payload = await api(`/jobs/${encodeURIComponent(jobId)}`);
        const nextStatus = normalizeProcessStatus(payload?.status);
        const nextStep = String(payload?.step || '').trim();
        const rawProgress = payload?.progress ?? processInfo?.progress ?? 0;
        const nextProgress = Number.isFinite(Number(rawProgress)) ? Number(rawProgress) : 0;

        const prev = state.crmProcessByAttachment[attachmentId] || {};
        if (prev.status !== nextStatus || prev.step !== nextStep || Number(prev.progress || 0) !== nextProgress) {
          state.crmProcessByAttachment[attachmentId] = {
            ...prev,
            jobId,
            status: nextStatus,
            step: nextStep,
            progress: nextProgress
          };
          changed = true;
        }
      } catch {
        // keep current status when polling fails
      }
    }));

    if (changed) renderCrmAttachmentRows();
  }

  function renderCrmAttachmentRows() {
    if (!els.crmAttachmentsRowsBody) return;

    const search = String(state.crmSearch || '').trim().toLowerCase();
    const sourceItems = Array.isArray(state.crmAttachments) ? state.crmAttachments : [];
    const items = sourceItems.filter((item) => {
      if (!search) return true;
      return [
        item?.id,
        item?.type,
        item?.created_at,
        item?.account_name,
        item?.assigned_user,
        item?.attachment_id,
        item?.filename,
        item?.process_job_id,
        item?.process_status,
      ]
        .map((v) => String(v || '').toLowerCase())
        .join(' ')
        .includes(search);
    });
    const hasRows = items.length > 0;
    const hasError = Boolean(state.crmAttachmentsError);
    const showTable = state.crmLoading || hasRows || hasError;

    if (els.crmAttachmentsTableWrap) els.crmAttachmentsTableWrap.classList.toggle('hidden', !showTable);
    if (els.crmAttachmentsEmptyState) {
      const showEmpty = !state.crmLoading && !hasRows && !hasError;
      els.crmAttachmentsEmptyState.classList.toggle('hidden', !showEmpty);
    }

    els.crmAttachmentsRowsBody.innerHTML = '';

    if (state.crmLoading) {
      const tr = document.createElement('tr');
      tr.innerHTML = '<td colspan="8" class="table-empty-cell">Loading CRM files…</td>';
      els.crmAttachmentsRowsBody.appendChild(tr);
      setCrmLoadMoreState();
      setCrmPaginationState();
      return;
    }

    if (hasError) {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td colspan="8" class="table-empty-cell">${escapeHtml(state.crmAttachmentsError)}</td>`;
      els.crmAttachmentsRowsBody.appendChild(tr);
      setCrmLoadMoreState();
      setCrmPaginationState();
      return;
    }

    if (!hasRows) {
      if (search && sourceItems.length > 0) {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td colspan="8" class="table-empty-cell">No matching CRM files.</td>';
        els.crmAttachmentsRowsBody.appendChild(tr);
      }
      setCrmLoadMoreState();
      setCrmPaginationState();
      return;
    }

    for (const item of items) {
      const status = String(item.status || 'unavailable').toLowerCase();
      const isAvailable = status === 'available';
      const attachmentId = String(item.attachment_id || '').trim();
      const recordId = String(item.id || item.lead_id || '').trim();
      const sourceType = String(item.type || 'Lead').trim();
      const process = state.crmProcessByAttachment[attachmentId] || {
        jobId: String(item.process_job_id || '').trim(),
        status: normalizeProcessStatus(item.process_status),
        step: String(item.process_step || '').trim(),
        progress: Number(item.process_progress || 0)
      };
      const processStatus = normalizeProcessStatus(process.status);

      let actionCell = '<span class="subtle-id">Unavailable</span>';
      if (isAvailable) {
        if (process.jobId && (processStatus === 'queued' || processStatus === 'processing')) {
          actionCell = `<button class="row-action-btn action-queued" type="button" data-open-job-id="${escapeHtml(process.jobId)}">Open Processing</button>`;
        } else if (process.jobId && (processStatus === 'completed' || processStatus === 'needs_review')) {
          actionCell = `<button class="row-action-btn action-completed" type="button" data-open-job-id="${escapeHtml(process.jobId)}">Open Result</button>`;
        } else if (processStatus === 'failed') {
          actionCell = `<button class="row-action-btn action-failed" type="button" data-process-attachment-id="${escapeHtml(attachmentId)}">Retry Process</button>`;
        } else {
          actionCell = `<button class="row-action-btn action-completed" type="button" data-process-attachment-id="${escapeHtml(attachmentId)}">Begin Process</button>`;
        }
      }

      const fileNameCell = isAvailable
        ? escapeHtml(item.filename || '-')
        : `${escapeHtml(item.filename || '-')}${item.error ? `<div class="subtle-id">${escapeHtml(item.error)}</div>` : ''}`;
      const statusCell = `<span class="status-pill ${processStatusClass(processStatus)}">${escapeHtml(formatProcessStatusLabel(processStatus))}</span>${shouldShowProcessStep(processStatus, process.step) ? `<div class="subtle-id">${escapeHtml(process.step)}</div>` : ''}`;
      const recordEntity = sourceType === 'Business Profile' ? 'Account' : 'Lead';
      const idCell = recordId
        ? `<a href="https://staging-crm.discoverycsc.com/#${recordEntity}/view/${encodeURIComponent(recordId)}" target="_blank" rel="noopener noreferrer">${escapeHtml(recordId)}</a>`
        : '-';
      const createdAtCell = escapeHtml(formatDate(item.created_at || item.createdAt || ''));

      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${idCell}</td>
        <td>${escapeHtml(sourceType || '-')}</td>
        <td>${createdAtCell}</td>
        <td>${escapeHtml(item.account_name || '-')}</td>
        <td>${escapeHtml(item.assigned_user || '-')}</td>
        <td>${fileNameCell}</td>
        <td>${statusCell}</td>
        <td>${actionCell}</td>
      `;
      els.crmAttachmentsRowsBody.appendChild(tr);
    }
    setCrmLoadMoreState();
    setCrmPaginationState();
  }

  async function loadCrmAttachments(reset = true) {
    if (!els.crmAttachmentsRowsBody) return;
    if (state.crmLoading || state.crmLoadingMore) return;

    if (reset) {
      state.crmLoading = true;
      state.crmAttachmentsError = '';
      state.crmHasMore = false;
      setCrmRefreshState();
      setCrmLoadMoreState();
      setCrmPaginationState();
      renderCrmAttachmentRows();
    } else {
      state.crmLoadingMore = true;
      setCrmRefreshState();
      setCrmLoadMoreState();
      setCrmPaginationState();
    }

    try {
      const requestOffset = Math.max(0, Number(state.crmOffset || 0));
      const params = new URLSearchParams({
        limit: String(state.crmLimit),
        offset: String(requestOffset),
        probe: state.crmProbeMode,
      });
      const payload = await api(`/crm/attachments?${params.toString()}`);
      const items = Array.isArray(payload?.items) ? payload.items : [];
      state.crmAttachments = items;
      state.crmHasMore = Boolean(payload?.has_more);
      state.crmCurrentOffset = requestOffset;
      state.crmNextOffset = Number(payload?.next_offset ?? (requestOffset + items.length)) || 0;
      syncCrmProcessMapFromItems(state.crmAttachments);
      await refreshCrmProcessStatuses();
    } catch (err) {
      if (reset) {
        state.crmAttachments = [];
        state.crmProcessByAttachment = {};
      }
      state.crmAttachmentsError = `CRM load failed: ${normalizeApiErrorMessage(err?.message)}`;
    } finally {
      state.crmLoading = false;
      state.crmLoadingMore = false;
      setCrmRefreshState();
      setCrmLoadMoreState();
      setCrmPaginationState();
      renderCrmAttachmentRows();
    }
  }

  function setView(view) {
    state.view = view === 'processing' ? 'processing' : 'uploads';
    if (els.uploadsView) els.uploadsView.classList.toggle('hidden', state.view !== 'uploads');
    if (els.processingView) els.processingView.classList.toggle('hidden', state.view !== 'processing');
    if (els.menuUploads) els.menuUploads.classList.toggle('active', state.view === 'uploads');
    if (els.menuProcessing) els.menuProcessing.classList.toggle('active', state.view === 'processing');
    if (els.menuUploads) els.menuUploads.setAttribute('aria-current', state.view === 'uploads' ? 'page' : 'false');
    if (els.menuProcessing) els.menuProcessing.setAttribute('aria-current', state.view === 'processing' ? 'page' : 'false');
    const canAccessCrmAttachments = state.authRole === 'evaluator' || state.authRole === 'admin';
    if (state.view === 'uploads' && canAccessCrmAttachments) startCrmStatusPolling();
    else stopCrmStatusPolling();
  }

  function resolveViewFromPath(pathname) {
    const clean = String(pathname || '/').replace(/\/+$/, '') || '/';
    return ROUTE_TO_VIEW[clean] || 'uploads';
  }

  function buildRoute(view, jobId = null) {
    const path = VIEW_TO_ROUTE[view] || '/uploads';
    if (view === 'processing' && jobId) return `${path}?job-id=${encodeURIComponent(jobId)}`;
    return path;
  }

  function syncRoute(route, replace = false, jobId = null) {
    const parsed = new URL(String(route || '/uploads'), window.location.origin);
    const view = resolveViewFromPath(parsed.pathname);
    const routeJobId = jobId ?? (view === 'processing' ? new URLSearchParams(parsed.search).get('job-id') : null);
    setView(view);
    if (view === 'processing' && routeJobId && routeJobId !== state.jobId) {
      setActiveJob(routeJobId, false).catch((err) => {
        alert(`Load job failed: ${err.message}`);
      });
    }
    const targetPath = buildRoute(view, routeJobId);
    const current = `${window.location.pathname}${window.location.search}`;
    if (current !== targetPath) {
      const fn = replace ? window.history.replaceState : window.history.pushState;
      fn.call(window.history, {}, '', targetPath);
    }
  }

  function setStatus(payload) {
    const raw = String(payload.status || 'idle').toLowerCase();
    const mapped = raw === 'done' ? 'completed' : raw;
    if (els.jobStatus) {
      els.jobStatus.textContent = mapped;
      els.jobStatus.classList.remove('status-idle', 'status-processing', 'status-completed', 'status-failed');
      if (mapped === 'processing') els.jobStatus.classList.add('status-processing');
      else if (mapped === 'completed') els.jobStatus.classList.add('status-completed');
      else if (mapped === 'failed') els.jobStatus.classList.add('status-failed');
      else els.jobStatus.classList.add('status-idle');
    }
    if (els.jobStep) els.jobStep.textContent = payload.step || '-';
    const progress = Math.max(0, Math.min(100, Number(payload.progress ?? 0)));
    if (els.jobProgress) els.jobProgress.textContent = `${progress}%`;
    if (els.jobProgressFill) els.jobProgressFill.style.width = `${progress}%`;
    state.isCompleted = mapped === 'completed';
    state.currentParseMode = String(payload.parse_mode || state.currentParseMode || '').trim().toLowerCase();

    if (els.startBtn) {
      const allowStart = Boolean(state.jobId) && !['processing', 'completed', 'failed'].includes(mapped);
      els.startBtn.disabled = !allowStart;
      els.startBtn.classList.toggle('start-emphasis', mapped === 'idle' && Boolean(state.jobId));
    }
    updateExportAvailability();
    if (mapped === 'idle' && state.totalParsedRows === 0) renderSummary(null);
    updatePreviewEmptyState();

    if (state.jobId) {
      updateUploadedJobIfExists({
        jobId: state.jobId,
        status: payload.status || 'queued',
        step: payload.step || 'queued',
        progress: Number(payload.progress ?? 0),
        parseMode: payload.parse_mode
      });
    }
  }

  function setExportLinks(enabled) {
    const links = [
      [els.exportPdf, 'pdf'],
      [els.exportExcel, 'excel']
    ];
    for (const [el, type] of links) {
      if (!el) continue;
      if (!state.jobId || !enabled) {
        el.classList.add('disabled');
        el.setAttribute('aria-disabled', 'true');
        el.href = '#';
        el.onclick = (evt) => evt.preventDefault();
      } else {
        el.classList.remove('disabled');
        el.setAttribute('aria-disabled', 'false');
        el.href = `/jobs/${state.jobId}/export/${type}`;
        el.onclick = null;
      }
    }
  }

  function updateExportAvailability() {
    const allowed = Boolean(state.jobId) && state.isCompleted && state.totalParsedRows > 0;
    setExportLinks(allowed);
  }

  function clearRows() {
    if (els.rowsBody) els.rowsBody.innerHTML = '';
    if (els.rowCount) els.rowCount.textContent = '0 rows';
    if (els.parsedJsonBody) els.parsedJsonBody.textContent = '';
    updateReverseRowsActionState();
    syncParsedSectionHeightToPreview();
  }

  function setParsedPanelMode(mode) {
    const nextMode = mode === 'json' ? 'json' : 'table';
    state.parsedPanelMode = nextMode;
    if (els.parsedTableWrap) els.parsedTableWrap.classList.toggle('hidden', nextMode !== 'table');
    if (els.parsedJsonWrap) els.parsedJsonWrap.classList.toggle('hidden', nextMode !== 'json');
    if (els.parsedDebugToggleBtn) {
      els.parsedDebugToggleBtn.textContent = nextMode === 'json' ? 'Table View' : 'Debug JSON';
      els.parsedDebugToggleBtn.disabled = !state.jobId || !state.currentPage;
    }
    updateReverseRowsActionState();
    syncParsedSectionHeightToPreview();
  }

  function updateReverseRowsActionState() {
    if (!els.reverseRowsBtn) return;
    if (state.parsedPanelMode !== 'table') {
      els.reverseRowsBtn.disabled = true;
      return;
    }
    const page = String(state.currentPage || '').trim();
    const rows = page ? state.parsedByPage[page] : null;
    const canReverse = Array.isArray(rows) && rows.length > 1;
    els.reverseRowsBtn.disabled = !canReverse;
  }

  async function ensureCurrentPageOpenaiRawLoaded() {
    const page = String(state.currentPage || '').trim();
    if (!state.jobId || !page) return;
    if (state.openaiRawByPage[page] !== undefined) return;
    try {
      state.openaiRawByPage[page] = await api(`/jobs/${state.jobId}/ocr/${page}/openai-raw`);
    } catch {
      state.openaiRawByPage[page] = { detail: "openai_ocr_raw_not_ready" };
    }
  }

  function renderParsedDebugJson() {
    if (!els.parsedJsonBody) return;
    const page = String(state.currentPage || '').trim();
    const rawOpenai = page ? state.openaiRawByPage[page] : null;
    const parsedRows = page ? (state.parsedByPage[page] || []) : [];
    if (els.rowCount) els.rowCount.textContent = `${Array.isArray(parsedRows) ? parsedRows.length : 0} rows`;
    const payload = {
      job_id: state.jobId || null,
      page: page || null,
      openai_raw_response: rawOpenai ?? { detail: "openai_ocr_raw_not_ready" },
    };
    els.parsedJsonBody.textContent = JSON.stringify(payload, null, 2);
  }

  function syncParsedSectionHeightToPreview() {
    if (!els.processingGrid || !els.previewSectionCard || !els.parsedSectionCard || !els.parsedTableWrap) return;

    const computed = window.getComputedStyle(els.processingGrid);
    if (computed.gridTemplateColumns === 'none') {
      els.parsedSectionCard.style.removeProperty('height');
      els.parsedTableWrap.style.removeProperty('height');
      els.parsedTableWrap.style.removeProperty('max-height');
      return;
    }

    const previewHeight = Math.round(els.previewSectionCard.getBoundingClientRect().height || 0);
    if (!Number.isFinite(previewHeight) || previewHeight <= 0) return;

    els.parsedSectionCard.style.height = `${previewHeight}px`;
    const parsedHeader = els.parsedSectionCard.querySelector('.row-between');
    const headerHeight = Math.round(parsedHeader ? parsedHeader.getBoundingClientRect().height : 0);
    const tableHeight = Math.max(120, previewHeight - headerHeight - 22);
    els.parsedTableWrap.style.height = `${tableHeight}px`;
    els.parsedTableWrap.style.maxHeight = `${tableHeight}px`;
  }

  function recomputeTotalParsedRows() {
    state.totalParsedRows = Object.values(state.parsedByPage).reduce(
      (acc, pageRows) => acc + (Array.isArray(pageRows) ? pageRows.length : 0),
      0,
    );
  }

  function normalizeEditableCellValue(value) {
    if (value === null || value === undefined) return '';
    return String(value);
  }

  function parseAmountForFormatting(value) {
    const raw = normalizeEditableCellValue(value).trim();
    if (!raw) return null;

    const hasParenNegative = raw.startsWith('(') && raw.endsWith(')');
    const cleaned = raw
      .replaceAll(',', '')
      .replace(/[^\d.\-]/g, '');

    if (!cleaned || cleaned === '-' || cleaned === '.' || cleaned === '-.') return null;
    const parsed = Number.parseFloat(cleaned);
    if (!Number.isFinite(parsed)) return null;
    if (hasParenNegative && parsed > 0) return -parsed;
    return parsed;
  }

  function formatAmountCellValue(value) {
    const raw = normalizeEditableCellValue(value).trim();
    if (!raw) return '';
    const parsed = parseAmountForFormatting(raw);
    if (!Number.isFinite(parsed)) return raw;
    return parsed.toLocaleString('en-US', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }

  function computeBalanceMismatchRowIds(rows) {
    const mismatches = new Set();
    if (!Array.isArray(rows) || rows.length < 2) return mismatches;

    for (let idx = 1; idx < rows.length; idx += 1) {
      const prev = rows[idx - 1] || {};
      const current = rows[idx] || {};
      const rowId = String(current?.row_id || '').trim();
      if (!rowId) continue;

      const prevBal = parseAmountForFormatting(prev.balance);
      const currBal = parseAmountForFormatting(current.balance);
      const debit = parseAmountForFormatting(current.debit);
      const credit = parseAmountForFormatting(current.credit);
      const hasFlow = Number.isFinite(debit) || Number.isFinite(credit);

      if (!Number.isFinite(prevBal) || !Number.isFinite(currBal) || !hasFlow) continue;

      const expected = prevBal - (Number.isFinite(debit) ? debit : 0) + (Number.isFinite(credit) ? credit : 0);
      if (Math.abs(currBal - expected) > 0.01) {
        mismatches.add(rowId);
      }
    }
    return mismatches;
  }

  function applyBalanceMismatchStyles(page) {
    if (!els.rowsBody) return;
    const rows = Array.isArray(state.parsedByPage[page]) ? state.parsedByPage[page] : [];
    const mismatches = computeBalanceMismatchRowIds(rows);

    for (const tr of els.rowsBody.querySelectorAll('tr')) {
      const rowId = String(tr.dataset.rowId || '').trim();
      const isMismatch = mismatches.has(rowId);
      tr.classList.toggle('balance-mismatch-row', isMismatch);
      const input = tr.querySelector('.table-row-input-balance');
      if (input) {
        input.classList.toggle('balance-mismatch', isMismatch);
      }
    }
  }

  function clearPendingPageSaves() {
    for (const key of Object.keys(state.pageSaveTimers)) {
      window.clearTimeout(state.pageSaveTimers[key]);
    }
    state.pageSaveTimers = {};
    state.pageSaveTokenByPage = {};
  }

  async function flushPendingPageSaves() {
    const pendingPages = Object.keys(state.pageSaveTimers);
    if (!pendingPages.length) return;

    for (const page of pendingPages) {
      window.clearTimeout(state.pageSaveTimers[page]);
      delete state.pageSaveTimers[page];
    }

    await Promise.allSettled(pendingPages.map((page) => persistPageRows(page)));
  }

  function selectRow(rowId) {
    state.selectedRowId = String(rowId || '').trim();
    if (!els.rowsBody) return;
    for (const tr of els.rowsBody.querySelectorAll('tr')) {
      tr.classList.toggle('selected', String(tr.dataset.rowId || '') === state.selectedRowId);
    }
    drawSelectedBound();
    syncParsedSectionHeightToPreview();
  }

  async function persistPageRows(page) {
    const pageName = String(page || '').trim();
    if (!state.jobId || !pageName) return;

    const sourceRows = Array.isArray(state.parsedByPage[pageName]) ? state.parsedByPage[pageName] : [];
    const payloadRows = sourceRows.map((row, idx) => {
      const rowId = String(row?.row_id || '').trim() || String(idx + 1).padStart(3, '0');
      return {
        row_id: rowId,
        date: normalizeEditableCellValue(row?.date).trim(),
        description: normalizeEditableCellValue(row?.description).trim(),
        debit: formatAmountCellValue(row?.debit),
        credit: formatAmountCellValue(row?.credit),
        balance: formatAmountCellValue(row?.balance),
      };
    });

    const token = (state.pageSaveTokenByPage[pageName] || 0) + 1;
    state.pageSaveTokenByPage[pageName] = token;

    const payload = await api(`/jobs/${state.jobId}/parsed/${pageName}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payloadRows),
    });

    if (state.pageSaveTokenByPage[pageName] !== token) return;

    if (payload && Array.isArray(payload.rows)) {
      const existingRows = Array.isArray(state.parsedByPage[pageName]) ? state.parsedByPage[pageName] : [];
      const sameShape = existingRows.length === payload.rows.length
        && existingRows.every((row, idx) => {
          const existingId = String(row?.row_id || idx + 1);
          const payloadId = String(payload.rows[idx]?.row_id || idx + 1);
          return existingId === payloadId;
        });

      if (sameShape) {
        for (let idx = 0; idx < existingRows.length; idx += 1) {
          const src = payload.rows[idx] || {};
          existingRows[idx].row_id = String(src.row_id || existingRows[idx].row_id || '').trim();
          existingRows[idx].date = normalizeEditableCellValue(src.date).trim();
          existingRows[idx].description = normalizeEditableCellValue(src.description).trim();
          existingRows[idx].debit = formatAmountCellValue(src.debit);
          existingRows[idx].credit = formatAmountCellValue(src.credit);
          existingRows[idx].balance = formatAmountCellValue(src.balance);
        }
      } else {
        state.parsedByPage[pageName] = payload.rows;
        if (pageName === state.currentPage) {
          renderRows();
        }
      }

      if (
        state.selectedRowId
        && !payload.rows.some((row) => String(row?.row_id || '') === String(state.selectedRowId))
      ) {
        state.selectedRowId = null;
      }
    }

    recomputeTotalParsedRows();
    updateExportAvailability();

    if (payload && payload.summary && typeof payload.summary === 'object') {
      renderSummary(payload.summary);
    }
  }

  function queuePageRowsSave(page) {
    const pageName = String(page || '').trim();
    if (!pageName || !state.jobId) return;
    if (state.pageSaveTimers[pageName]) {
      window.clearTimeout(state.pageSaveTimers[pageName]);
    }
    state.pageSaveTimers[pageName] = window.setTimeout(() => {
      delete state.pageSaveTimers[pageName];
      persistPageRows(pageName).catch((err) => {
        alert(`Save failed: ${normalizeApiErrorMessage(err?.message)}`);
      });
    }, ROW_SAVE_DEBOUNCE_MS);
  }

  function buildNextRowId(rows) {
    const maxRow = (Array.isArray(rows) ? rows : []).reduce((max, row, idx) => {
      const raw = String(row?.row_id || '').trim() || String(idx + 1);
      const num = Number.parseInt(raw, 10);
      if (Number.isFinite(num)) return Math.max(max, num);
      return max;
    }, 0);
    return String(maxRow + 1).padStart(3, '0');
  }

  function insertRowAfter(page, rowId) {
    const rows = Array.isArray(state.parsedByPage[page]) ? state.parsedByPage[page] : [];
    const idx = rows.findIndex((row) => String(row?.row_id || '') === String(rowId || ''));
    if (idx < 0) return;
    const nextRow = {
      row_id: buildNextRowId(rows),
      date: '',
      description: '',
      debit: '',
      credit: '',
      balance: '',
    };
    rows.splice(idx + 1, 0, nextRow);
    state.parsedByPage[page] = rows;
    renderRows();
    selectRow(nextRow.row_id);
    queuePageRowsSave(page);
  }

  function deleteRow(page, rowId) {
    const rows = Array.isArray(state.parsedByPage[page]) ? state.parsedByPage[page] : [];
    const idx = rows.findIndex((row) => String(row?.row_id || '') === String(rowId || ''));
    if (idx < 0) return;
    rows.splice(idx, 1);
    state.parsedByPage[page] = rows;

    if (!rows.length) {
      state.selectedRowId = null;
    } else if (String(state.selectedRowId || '') === String(rowId || '')) {
      const fallback = rows[Math.min(idx, rows.length - 1)];
      state.selectedRowId = String(fallback?.row_id || '').trim() || null;
    }

    renderRows();
    queuePageRowsSave(page);
  }

  function reverseCurrentPageRows() {
    const page = String(state.currentPage || '').trim();
    if (!page) return;
    const rows = Array.isArray(state.parsedByPage[page]) ? state.parsedByPage[page] : [];
    if (rows.length < 2) return;

    state.parsedByPage[page] = rows.slice().reverse();
    renderRows();
    queuePageRowsSave(page);
  }

  function drawSelectedBound() {
    if (!els.overlay || !els.previewImage) return;
    const canvas = els.overlay;
    const img = els.previewImage;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    const rect = img.getBoundingClientRect();

    canvas.width = Math.max(1, Math.round(rect.width));
    canvas.height = Math.max(1, Math.round(rect.height));
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    if (!state.currentPage || !state.selectedRowId) return;
    const bounds = state.boundsByPage[state.currentPage] || [];
    const bound = bounds.find((b) => String(b.row_id) === String(state.selectedRowId));
    if (!bound) return;

    const x = Number(bound.x1 || 0) * canvas.width;
    const y = Number(bound.y1 || 0) * canvas.height;
    const w = (Number(bound.x2 || 0) - Number(bound.x1 || 0)) * canvas.width;
    const h = (Number(bound.y2 || 0) - Number(bound.y1 || 0)) * canvas.height;

    ctx.strokeStyle = '#111111';
    ctx.lineWidth = 1;
    ctx.strokeRect(x, y, w, h);
  }

  function getRowDisplayValue(row, rowId, index) {
    if (state.currentParseMode === 'ocr') {
      if (row && Object.prototype.hasOwnProperty.call(row, 'rownumber')) {
        const value = row.rownumber;
        return value === null || value === undefined ? '' : String(value).trim();
      }
      if (row && Object.prototype.hasOwnProperty.call(row, 'row_number')) {
        return String(row.row_number || '').trim();
      }
      return '';
    }
    return String(Number(index) + 1);
  }

  function renderRows() {
    clearRows();
    if (!state.currentPage || !els.rowsBody) return;

    const page = state.currentPage;
    const rows = state.parsedByPage[page] || [];
    recomputeTotalParsedRows();
    updateExportAvailability();
    if (els.rowCount) els.rowCount.textContent = `${rows.length} rows`;
    for (const [idx, row] of rows.entries()) {
      const tr = document.createElement('tr');
      tr.className = 'clickable';
      const rowId = String(row?.row_id || '').trim() || String(idx + 1).padStart(3, '0');
      row.row_id = rowId;
      tr.dataset.rowId = rowId;

      const rowIdCell = document.createElement('td');
      rowIdCell.textContent = getRowDisplayValue(row, rowId, idx);
      tr.appendChild(rowIdCell);

      for (const field of EDITABLE_ROW_FIELDS) {
        const td = document.createElement('td');
        const input = document.createElement('input');
        input.type = 'text';
        input.className = `table-row-input table-row-input-${field}`;
        input.dataset.field = field;
        input.value = field === 'date'
          ? formatParsedRowDate(normalizeEditableCellValue(row[field]))
          : (AMOUNT_ROW_FIELDS.has(field)
            ? formatAmountCellValue(row[field])
            : normalizeEditableCellValue(row[field]));
        input.placeholder = field.toUpperCase();

        input.addEventListener('click', (evt) => {
          evt.stopPropagation();
        });
        input.addEventListener('focus', () => {
          selectRow(rowId);
        });
        input.addEventListener('input', () => {
          row[field] = input.value;
          queuePageRowsSave(page);
          applyBalanceMismatchStyles(page);
        });
        input.addEventListener('change', () => {
          const normalized = AMOUNT_ROW_FIELDS.has(field)
            ? formatAmountCellValue(input.value)
            : input.value.trim();
          row[field] = normalized;
          input.value = normalized;
          queuePageRowsSave(page);
          applyBalanceMismatchStyles(page);
        });
        input.addEventListener('blur', () => {
          const normalized = AMOUNT_ROW_FIELDS.has(field)
            ? formatAmountCellValue(input.value)
            : input.value.trim();
          row[field] = normalized;
          input.value = normalized;
          queuePageRowsSave(page);
          applyBalanceMismatchStyles(page);
        });

        td.appendChild(input);
        tr.appendChild(td);
      }

      const actionsCell = document.createElement('td');
      actionsCell.className = 'parsed-actions-cell';

      const insertBtn = document.createElement('button');
      insertBtn.type = 'button';
      insertBtn.className = 'parsed-row-action parsed-row-action-insert';
      insertBtn.textContent = '+';
      insertBtn.title = 'Insert row';
      insertBtn.setAttribute('aria-label', 'Insert row');
      insertBtn.addEventListener('click', (evt) => {
        evt.stopPropagation();
        insertRowAfter(page, rowId);
      });

      const deleteBtn = document.createElement('button');
      deleteBtn.type = 'button';
      deleteBtn.className = 'parsed-row-action parsed-row-action-delete';
      deleteBtn.textContent = '×';
      deleteBtn.title = 'Delete row';
      deleteBtn.setAttribute('aria-label', 'Delete row');
      deleteBtn.disabled = rows.length <= 1;
      deleteBtn.addEventListener('click', (evt) => {
        evt.stopPropagation();
        deleteRow(page, rowId);
      });

      actionsCell.appendChild(insertBtn);
      actionsCell.appendChild(deleteBtn);
      tr.appendChild(actionsCell);

      tr.addEventListener('click', () => {
        selectRow(rowId);
      });
      if (state.selectedRowId && String(state.selectedRowId) === rowId) {
        tr.classList.add('selected');
      }
      els.rowsBody.appendChild(tr);
    }
    updateReverseRowsActionState();
    applyBalanceMismatchStyles(page);
    drawSelectedBound();
  }

  function renderPages() {
    if (!els.pageSelect) return;
    els.pageSelect.innerHTML = '';
    for (const page of state.pages) {
      const option = document.createElement('option');
      option.value = page;
      option.textContent = page;
      els.pageSelect.appendChild(option);
    }
    if (state.pages.length && (!state.currentPage || !state.pages.includes(state.currentPage))) {
      state.currentPage = state.pages[0];
    }
    if (state.currentPage) els.pageSelect.value = state.currentPage;
    updatePageNav();
  }

  function loadPreview() {
    if (!state.jobId || !state.currentPage || !els.previewImage) {
      if (els.previewImage) els.previewImage.removeAttribute('src');
      updatePreviewEmptyState();
      return;
    }
    const url = `/jobs/${state.jobId}/preview/${state.currentPage}?v=${Date.now()}`;
    els.previewImage.src = url;
    state.selectedRowId = null;
    drawSelectedBound();
    updatePreviewEmptyState();
  }

  function updatePageNav() {
    const total = state.pages.length;
    const idx = total ? state.pages.indexOf(state.currentPage) : -1;
    if (els.pageIndicator) {
      els.pageIndicator.textContent = total ? `${idx >= 0 ? idx + 1 : 0}/${total}` : '0/0';
    }
    if (els.pagePrevBtn) els.pagePrevBtn.disabled = !(idx > 0);
    if (els.pageNextBtn) els.pageNextBtn.disabled = !(idx >= 0 && idx < total - 1);
    if (els.parsedDebugToggleBtn) els.parsedDebugToggleBtn.disabled = !state.jobId || !state.currentPage;
  }

  async function loadCurrentPageData() {
    if (!state.jobId || !state.currentPage) return;
    const page = state.currentPage;
    if (!state.parsedByPage[page]) {
      state.parsedByPage[page] = await api(`/jobs/${state.jobId}/parsed/${page}`);
    }
    if (!state.boundsByPage[page]) {
      state.boundsByPage[page] = await api(`/jobs/${state.jobId}/rows/${page}/bounds`);
    }
    if (state.parsedPanelMode === 'json') {
      await ensureCurrentPageOpenaiRawLoaded();
      renderParsedDebugJson();
    } else {
      renderRows();
    }
    loadPreview();
  }

  async function loadResultData() {
    await flushPendingPageSaves();
    if (!state.jobId) return;
    const [cleaned, summary] = await Promise.all([
      api(`/jobs/${state.jobId}/cleaned`),
      api(`/jobs/${state.jobId}/summary`)
    ]);

    const pages = (cleaned.pages || [])
      .map((name) => String(name || '').replace(/\.png$/i, ''))
      .filter(Boolean)
      .sort();

    state.pages = pages;
    state.currentPage = pages[0] || null;
    state.parsedByPage = {};
    state.boundsByPage = {};
    state.openaiRawByPage = {};
    state.totalParsedRows = Number(summary?.total_transactions || 0);

    renderSummary(state.totalParsedRows > 0 ? (summary || null) : null);
    renderPages();
    await loadCurrentPageData();
    updateExportAvailability();
    updatePreviewEmptyState();
  }

  function stopPolling() {
    if (state.pollTimer) clearInterval(state.pollTimer);
    state.pollTimer = null;
  }

  async function pollStatus() {
    if (!state.jobId) return;
    try {
      const payload = await api(`/jobs/${state.jobId}`);
      setStatus(payload);
      const status = String(payload.status || '').toLowerCase();
      if (status === 'done') {
        stopPolling();
        await loadResultData();
      }
      if (status === 'failed') {
        stopPolling();
        alert(`Job failed: ${payload.message || 'unknown error'}`);
      }
    } catch (err) {
      stopPolling();
      alert(`Status check failed: ${err.message}`);
    }
  }

  function startPolling() {
    stopPolling();
    state.pollTimer = setInterval(pollStatus, 2000);
    pollStatus();
  }

  async function setActiveJob(jobId, switchToProcessing = false) {
    await flushPendingPageSaves();
    const id = String(jobId || '').trim();
    if (!id) return;

    state.jobId = id;
    if (els.jobId) els.jobId.textContent = id;
    if (els.startBtn) els.startBtn.disabled = false;
    state.isCompleted = false;
    state.totalParsedRows = 0;
    state.openaiRawByPage = {};
    setExportLinks(false);
    setParsedPanelMode(state.parsedPanelMode);
    clearRows();
    renderSummary(null);
    if (switchToProcessing) syncRoute('/processing', false, id);

    const status = await api(`/jobs/${id}`);
    setStatus(status);
    const terminal = new Set(['done', 'failed']);
    if (terminal.has(String(status.status || '').toLowerCase())) {
      if (String(status.status || '').toLowerCase() === 'done') await loadResultData();
      return;
    }
    startPolling();
  }

  async function createJob(e) {
    e.preventDefault();
    if (!els.file || !els.file.files.length) return;
    const file = els.file.files[0];
    if (!file) return;
    await uploadSelectedFile(file);
  }

  async function uploadSelectedFile(file) {
    try {
      setUploadProgress(0, true);
      const payload = await uploadWithProgress(file, els.mode ? els.mode.value : 'auto', true);
      upsertUploadedJob({
        jobId: payload.job_id,
        fileName: file.name,
        sizeBytes: file.size,
        lastModified: file.lastModified,
        createdAt: Date.now(),
        status: payload.started ? 'processing' : 'queued',
        step: payload.started ? 'initializing' : 'queued',
        progress: payload.started ? 1 : 0,
        parseMode: payload.parse_mode
      });
      if (payload?.job_id && payload.started) {
        await setActiveJob(payload.job_id, true);
      }
      if (els.file) els.file.value = '';
      setUploadProgress(100, true);
      window.setTimeout(() => setUploadProgress(0, false), 500);
    } catch (err) {
      setUploadProgress(0, false);
      alert(`Upload failed: ${err.message}`);
    }
  }

  async function startJob(jobId = state.jobId) {
    const id = String(jobId || '').trim();
    if (!id) return;
    try {
      await setActiveJob(id, true);
      const payload = await api(`/jobs/${id}/start`, { method: 'POST' });
      if (payload.started) {
        updateUploadedJobIfExists({ jobId: id, status: 'processing', step: 'initializing', progress: 1 });
        startPolling();
      } else {
        pollStatus();
      }
    } catch (err) {
      alert(`Start failed: ${err.message}`);
    }
  }

  async function loadJobById() {
    const value = String(els.jobIdInput?.value || '').trim();
    if (!value) return;
    try {
      await setActiveJob(value, true);
    } catch (err) {
      alert(`Load job failed: ${err.message}`);
    }
  }

  if (els.form) els.form.addEventListener('submit', createJob);
  if (els.startBtn) els.startBtn.addEventListener('click', () => startJob());
  if (els.reverseRowsBtn) {
    els.reverseRowsBtn.addEventListener('click', () => {
      reverseCurrentPageRows();
    });
  }
  if (els.parsedDebugToggleBtn) {
    els.parsedDebugToggleBtn.addEventListener('click', async () => {
      if (!state.jobId || !state.currentPage) return;
      if (state.parsedPanelMode === 'table') {
        await ensureCurrentPageOpenaiRawLoaded();
        setParsedPanelMode('json');
        renderParsedDebugJson();
      } else {
        setParsedPanelMode('table');
        renderRows();
      }
    });
  }

  if (els.pageSelect) {
    els.pageSelect.addEventListener('change', () => {
      state.currentPage = els.pageSelect.value;
      state.selectedRowId = null;
      updatePageNav();
      loadCurrentPageData().catch((err) => alert(`Page load failed: ${err.message}`));
    });
  }
  if (els.pagePrevBtn) {
    els.pagePrevBtn.addEventListener('click', () => {
      const idx = state.pages.indexOf(state.currentPage);
      if (idx <= 0) return;
      state.currentPage = state.pages[idx - 1];
      if (els.pageSelect) els.pageSelect.value = state.currentPage;
      state.selectedRowId = null;
      loadCurrentPageData().catch((err) => alert(`Page load failed: ${err.message}`));
      updatePageNav();
    });
  }
  if (els.pageNextBtn) {
    els.pageNextBtn.addEventListener('click', () => {
      const idx = state.pages.indexOf(state.currentPage);
      if (idx < 0 || idx >= state.pages.length - 1) return;
      state.currentPage = state.pages[idx + 1];
      if (els.pageSelect) els.pageSelect.value = state.currentPage;
      state.selectedRowId = null;
      loadCurrentPageData().catch((err) => alert(`Page load failed: ${err.message}`));
      updatePageNav();
    });
  }

  if (els.loadJobBtn && els.jobIdInput) els.loadJobBtn.addEventListener('click', loadJobById);

  if (els.file && els.form) {
    els.file.addEventListener('change', () => {
      if (!els.file.files || !els.file.files.length) return;
      if (typeof els.form.requestSubmit === 'function') els.form.requestSubmit();
      else els.form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
    });
  }
  if (els.uploadSurface) {
    const onDragOver = (evt) => {
      evt.preventDefault();
      els.uploadSurface.classList.add('drag-over');
    };
    const onDragLeave = () => {
      els.uploadSurface.classList.remove('drag-over');
    };
    els.uploadSurface.addEventListener('dragenter', onDragOver);
    els.uploadSurface.addEventListener('dragover', onDragOver);
    els.uploadSurface.addEventListener('dragleave', onDragLeave);
    els.uploadSurface.addEventListener('drop', async (evt) => {
      evt.preventDefault();
      onDragLeave();
      const files = evt.dataTransfer?.files;
      if (!files || !files.length) return;
      await uploadSelectedFile(files[0]);
    });
  }

  if (els.uploadRowsBody) {
    els.uploadRowsBody.addEventListener('click', (e) => {
      const target = e.target;
      if (!(target instanceof HTMLElement)) return;
      const btn = target.closest('button[data-job-id]');
      if (!btn) return;
      const jobId = String(btn.getAttribute('data-job-id') || '');
      const action = String(btn.getAttribute('data-action') || '');
      if (!jobId) return;
      if (action === 'none') return;
      if (action === 'start') startJob(jobId);
      else setActiveJob(jobId, true).catch((err) => alert(`Load job failed: ${err.message}`));
    });
  }

  if (els.crmRefreshBtn) {
    els.crmRefreshBtn.addEventListener('click', () => {
      state.crmOffset = 0;
      loadCrmAttachments(true).catch((err) => {
        state.crmAttachmentsError = `CRM load failed: ${normalizeApiErrorMessage(err?.message)}`;
        renderCrmAttachmentRows();
      });
    });
  }

  if (els.crmLoadMoreBtn) {
    els.crmLoadMoreBtn.addEventListener('click', () => {
      if (!state.crmHasMore) return;
      state.crmOffset = state.crmNextOffset;
      loadCrmAttachments(true).catch((err) => {
        state.crmAttachmentsError = `CRM load failed: ${normalizeApiErrorMessage(err?.message)}`;
        renderCrmAttachmentRows();
      });
    });
  }

  if (els.crmPrevBtn) {
    els.crmPrevBtn.addEventListener('click', () => {
      if (state.crmLoading || state.crmLoadingMore || state.crmCurrentOffset <= 0) return;
      state.crmOffset = Math.max(0, state.crmCurrentOffset - state.crmLimit);
      loadCrmAttachments(true).catch((err) => {
        state.crmAttachmentsError = `CRM load failed: ${normalizeApiErrorMessage(err?.message)}`;
        renderCrmAttachmentRows();
      });
    });
  }

  if (els.crmNextBtn) {
    els.crmNextBtn.addEventListener('click', () => {
      if (state.crmLoading || state.crmLoadingMore || !state.crmHasMore) return;
      state.crmOffset = state.crmNextOffset;
      loadCrmAttachments(true).catch((err) => {
        state.crmAttachmentsError = `CRM load failed: ${normalizeApiErrorMessage(err?.message)}`;
        renderCrmAttachmentRows();
      });
    });
  }

  if (els.crmAttachmentsRowsBody) {
    els.crmAttachmentsRowsBody.addEventListener('click', (e) => {
      const target = e.target;
      if (!(target instanceof HTMLElement)) return;
      const openBtn = target.closest('button[data-open-job-id]');
      if (openBtn) {
        const openJobId = String(openBtn.getAttribute('data-open-job-id') || '').trim();
        if (!openJobId) return;
        setActiveJob(openJobId, true).catch((err) => alert(`Load job failed: ${err.message}`));
        return;
      }

      const btn = target.closest('button[data-process-attachment-id]');
      if (!btn) return;
      const attachmentId = String(btn.getAttribute('data-process-attachment-id') || '').trim();
      if (!attachmentId) return;
      const priorLabel = btn.textContent || 'Begin Process';
      btn.disabled = true;
      btn.textContent = 'Starting…';
      api(`/crm/attachments/${encodeURIComponent(attachmentId)}/begin-process`, { method: 'POST' })
        .then((payload) => {
          const jobId = String(payload?.job_id || '').trim();
          if (!jobId) throw new Error('missing_job_id');
          state.crmProcessByAttachment[attachmentId] = { jobId, status: 'queued', step: 'queued', progress: 0 };
          renderCrmAttachmentRows();
          startCrmStatusPolling();
          return setActiveJob(jobId, true);
        })
        .catch((err) => {
          alert(`Begin process failed: ${normalizeApiErrorMessage(err?.message)}`);
          btn.disabled = false;
          btn.textContent = priorLabel;
        });
    });
  }

  if (els.uploadSearch) {
    els.uploadSearch.addEventListener('input', () => {
      state.uploadSearch = String(els.uploadSearch.value || '').trim();
      renderUploadedRows();
    });
  }

  if (els.crmSearch) {
    els.crmSearch.addEventListener('input', () => {
      state.crmSearch = String(els.crmSearch.value || '').trim();
      renderCrmAttachmentRows();
    });
  }

  if (els.menuUploads) {
    els.menuUploads.addEventListener('click', (e) => {
      e.preventDefault();
      syncRoute('/uploads');
    });
  }
  if (els.menuProcessing) {
    els.menuProcessing.addEventListener('click', (e) => {
      e.preventDefault();
      syncRoute('/processing', false, state.jobId);
    });
  }
  window.addEventListener('popstate', () => syncRoute(`${window.location.pathname}${window.location.search}`, true));

  if (els.previewImage) els.previewImage.addEventListener('load', drawSelectedBound);
  if (els.previewImage) els.previewImage.addEventListener('load', syncParsedSectionHeightToPreview);
  window.addEventListener('resize', () => {
    drawSelectedBound();
    syncParsedSectionHeightToPreview();
  });
  if (els.logoutBtn) {
    els.logoutBtn.addEventListener('click', async () => {
      try {
        stopCrmStatusPolling();
        await fetch('/auth/logout', { method: 'POST' });
      } finally {
        window.location.href = '/login';
      }
    });
  }

  renderUploadedRows();
  setCrmRefreshState();
  setCrmLoadMoreState();
  setCrmPaginationState();
  renderCrmAttachmentRows();
  setParsedPanelMode('table');
  syncRoute(`${window.location.pathname}${window.location.search}` || '/uploads', true);
  setExportLinks(false);
  renderSummary(null);
  updatePreviewEmptyState();
  updatePageNav();
  syncParsedSectionHeightToPreview();
  initAuth()
    .then(() => {
      reconcileStoredJobsStatuses().catch(() => {
        // best-effort sync only
      });
    })
    .catch(() => {
      window.location.href = '/login';
    });

  function setUploadProgress(percent, visible) {
    const safe = Math.max(0, Math.min(100, Math.round(Number(percent) || 0)));
    if (els.uploadProgressBar) els.uploadProgressBar.style.width = `${safe}%`;
    if (els.uploadProgressText) els.uploadProgressText.textContent = `${safe}%`;
    if (els.uploadProgressWrap) els.uploadProgressWrap.classList.toggle('hidden', !visible);
  }

  function uploadWithProgress(file, mode, autoStart) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', '/jobs');
      xhr.responseType = 'json';

      xhr.upload.onprogress = (evt) => {
        if (!evt.lengthComputable) return;
        const pct = (evt.loaded / evt.total) * 100;
        setUploadProgress(pct, true);
      };

      xhr.onload = () => {
        if (xhr.status < 200 || xhr.status >= 300) {
          const msg = typeof xhr.response === 'string' ? xhr.response : `HTTP ${xhr.status}`;
          reject(new Error(msg));
          return;
        }
        const body = xhr.response;
        if (!body || typeof body !== 'object') {
          reject(new Error('Invalid upload response'));
          return;
        }
        resolve(body);
      };

      xhr.onerror = () => reject(new Error('Network error during upload'));

      const form = new FormData();
      form.append('file', file);
      form.append('mode', mode || 'auto');
      form.append('auto_start', String(Boolean(autoStart)));
      xhr.send(form);
    });
  }

  async function initAuth() {
    const me = await api('/auth/me');
    const role = String(me.role || '').toLowerCase();
    state.authRole = role;
    if (els.userRoleLabel) els.userRoleLabel.textContent = role || '-';
    if (els.adminLink) els.adminLink.classList.toggle('hidden', role !== 'admin');
    const canAccessCrmAttachments = role === 'evaluator' || role === 'admin';
    if (els.crmAttachmentsSection) {
      els.crmAttachmentsSection.classList.toggle('hidden', !canAccessCrmAttachments);
    }
    try {
      const settings = await api('/ui/settings');
      state.uploadTestingEnabled = Boolean(settings?.upload_testing_enabled);
    } catch {
      state.uploadTestingEnabled = false;
    }
    applyFeatureVisibility();
    if (canAccessCrmAttachments) {
      state.crmOffset = 0;
      await loadCrmAttachments();
      if (state.view === 'uploads') startCrmStatusPolling();
    } else {
      stopCrmStatusPolling();
    }
  }

  function renderSummary(summary) {
    const hasData = summary && typeof summary === 'object' && Object.keys(summary).length > 0;
    if (els.summary) {
      els.summary.classList.toggle('hidden', !hasData);
      if (!hasData) {
        els.summary.innerHTML = '';
      } else {
        const metrics = [
          { label: 'Total Transactions', value: formatNumber(summary.total_transactions), negative: Number(summary.total_transactions) < 0 },
          { label: 'Debit Transactions', value: formatNumber(summary.debit_transactions), negative: Number(summary.debit_transactions) < 0 },
          { label: 'Credit Transactions', value: formatNumber(summary.credit_transactions), negative: Number(summary.credit_transactions) < 0 },
          { label: 'Total Debit', value: formatCurrency(summary.total_debit), negative: Number(summary.total_debit) < 0 },
          { label: 'Total Credit', value: formatCurrency(summary.total_credit), negative: Number(summary.total_credit) < 0 },
          { label: 'Ending Balance', value: formatCurrency(summary.ending_balance), negative: Number(summary.ending_balance) < 0 },
          { label: 'Average Daily Balance (ADB)', value: formatCurrency(summary.adb), negative: Number(summary.adb) < 0 }
        ];
        const monthlyRows = Array.isArray(summary.monthly) ? summary.monthly : [];

        els.summary.innerHTML = `
          <section class="summary-section">
            <h3>Account Summary</h3>
            <div class="summary-metrics-grid">
              ${metrics.map((item) => `
                <article class="summary-metric-card">
                  <div class="metric-label">${escapeHtml(item.label)}</div>
                  <div class="metric-value${item.negative ? ' is-negative' : ''}">${escapeHtml(item.value)}</div>
                </article>
              `).join('')}
            </div>
          </section>
          <section class="summary-section">
            <h3>Monthly Breakdown</h3>
            <div class="summary-monthly-wrap">
              <table class="summary-monthly-table">
                <thead>
                  <tr>
                    <th>Month</th>
                    <th class="num">Total Debit</th>
                    <th class="num">Total Credit</th>
                    <th class="num">Avg Debit</th>
                    <th class="num">Avg Credit</th>
                    <th class="num">ADB</th>
                  </tr>
                </thead>
                <tbody>
                  ${
                    monthlyRows.length
                      ? monthlyRows.map((row) => `
                          <tr>
                            <td>${escapeHtml(row.month || '-')}</td>
                            <td class="num${Number(row.debit) < 0 ? ' is-negative' : ''}">${escapeHtml(formatCurrency(row.debit))}</td>
                            <td class="num${Number(row.credit) < 0 ? ' is-negative' : ''}">${escapeHtml(formatCurrency(row.credit))}</td>
                            <td class="num${Number(row.avg_debit) < 0 ? ' is-negative' : ''}">${escapeHtml(formatCurrency(row.avg_debit))}</td>
                            <td class="num${Number(row.avg_credit) < 0 ? ' is-negative' : ''}">${escapeHtml(formatCurrency(row.avg_credit))}</td>
                            <td class="num${Number(row.adb) < 0 ? ' is-negative' : ''}">${escapeHtml(formatCurrency(row.adb))}</td>
                          </tr>
                        `).join('')
                      : '<tr><td colspan="6" class="summary-table-empty">No monthly data available.</td></tr>'
                  }
                </tbody>
              </table>
            </div>
          </section>
        `;
      }
    }
    if (els.summaryEmpty) els.summaryEmpty.classList.toggle('hidden', hasData);
  }

  function updatePreviewEmptyState() {
    if (!els.previewEmpty) return;
    const hasPage = Boolean(state.currentPage && state.jobId);
    const hasImage = Boolean(els.previewImage && els.previewImage.getAttribute('src'));
    els.previewEmpty.classList.toggle('hidden', hasPage && hasImage);
  }
})();
