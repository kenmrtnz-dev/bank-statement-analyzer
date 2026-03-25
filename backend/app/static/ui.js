// Main browser controller for the evaluator UI. It owns route sync, API calls,
// polling, and DOM rendering for the uploads and processing workspaces.
(() => {
  const STORAGE_KEY = 'bsa_uploaded_jobs_v1';
  const MODE_STORAGE_KEY = 'bsa_process_mode_v1';
  const SUPPORTED_PROCESS_MODES = new Set(['auto']);
  const EDITABLE_ROW_FIELDS = ['date', 'description', 'debit', 'credit', 'balance'];
  const AMOUNT_ROW_FIELDS = new Set(['debit', 'credit', 'balance']);
  const ROW_SAVE_DEBOUNCE_MS = 220;

  // Cache DOM nodes once so render paths and pollers do not keep querying the document.
  const els = {
    form: document.getElementById('uploadForm'),
    file: document.getElementById('pdfFile'),
    mode: document.getElementById('mode'),
    processingGoogleVisionParserWrap: document.getElementById('processingGoogleVisionParserWrap'),
    processingGoogleVisionParser: document.getElementById('processingGoogleVisionParser'),
    pageNotesBtn: document.getElementById('pageNotesBtn'),
    pageAiFixBtn: document.getElementById('pageAiFixBtn'),
    pageNotesModal: document.getElementById('pageNotesModal'),
    pageNotesSubtitle: document.getElementById('pageNotesSubtitle'),
    pageNotesInput: document.getElementById('pageNotesInput'),
    pageNotesCloseBtn: document.getElementById('pageNotesCloseBtn'),
    pageNotesCancelBtn: document.getElementById('pageNotesCancelBtn'),
    pageNotesSaveBtn: document.getElementById('pageNotesSaveBtn'),
    pageAiFixModal: document.getElementById('pageAiFixModal'),
    pageAiFixSubtitle: document.getElementById('pageAiFixSubtitle'),
    pageAiFixStatus: document.getElementById('pageAiFixStatus'),
    pageAiFixJson: document.getElementById('pageAiFixJson'),
    pageAiFixCloseBtn: document.getElementById('pageAiFixCloseBtn'),
    pageAiFixCancelBtn: document.getElementById('pageAiFixCancelBtn'),
    pageAiFixApplyBtn: document.getElementById('pageAiFixApplyBtn'),
    startBtn: document.getElementById('startBtn'),
    jobId: document.getElementById('jobId'),
    jobStatus: document.getElementById('jobStatus'),
    jobStep: document.getElementById('jobStep'),
    jobProgress: document.getElementById('jobProgress'),
    jobProgressFill: document.getElementById('jobProgressFill'),
    summary: document.getElementById('summary'),
    summaryEmpty: document.getElementById('summaryEmpty'),
    pageFirstBtn: document.getElementById('pageFirstBtn'),
    pagePrevBtn: document.getElementById('pagePrevBtn'),
    pageNumberInput: document.getElementById('pageNumberInput'),
    pageCount: document.getElementById('pageCount'),
    pageNextBtn: document.getElementById('pageNextBtn'),
    pageLastBtn: document.getElementById('pageLastBtn'),
    previewTabButtons: Array.from(document.querySelectorAll('[data-preview-tab]')),
    previewTabPreviewBtn: document.getElementById('previewTabPreviewBtn'),
    previewTabDisbalanceBtn: document.getElementById('previewTabDisbalanceBtn'),
    previewTabFlaggedBtn: document.getElementById('previewTabFlaggedBtn'),
    previewImagePanel: document.getElementById('previewImagePanel'),
    previewDisbalancePanel: document.getElementById('previewDisbalancePanel'),
    previewFlaggedPanel: document.getElementById('previewFlaggedPanel'),
    disbalanceRowsBody: document.getElementById('disbalanceRowsBody'),
    flaggedRowsBody: document.getElementById('flaggedRowsBody'),
    previewWrap: document.getElementById('previewWrap'),
    previewStage: document.getElementById('previewStage'),
    previewImage: document.getElementById('previewImage'),
    previewEmpty: document.getElementById('previewEmpty'),
    overlay: document.getElementById('overlay'),
    rowsBody: document.getElementById('rowsBody'),
    rowCount: document.getElementById('rowCount'),
    reverseRowsBtn: document.getElementById('reverseRowsBtn'),
    exportPdf: document.getElementById('exportPdf'),
    exportExcel: document.getElementById('exportExcel'),
    exportCrm: document.getElementById('exportCrm'),
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
    crmStatusTabs: Array.from(document.querySelectorAll('[data-crm-status-tab]')),
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

  // Shared in-memory state. Render helpers read from here, and async events write back into it.
  const state = {
    jobId: null,
    pages: [],
    parsedByPage: {},
    baselineParsedByPage: {},
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
    crmLimit: 12,
    crmOffset: 0,
    crmCurrentOffset: 0,
    crmNextOffset: 0,
    crmHasMore: false,
    crmProbeMode: 'lazy',
    crmProcessByAttachment: {},
    crmStatusTimer: null,
    crmSearch: '',
    crmStatusTab: 'not_started',
    crmSearchDebounceTimer: null,
    crmLeadByJobId: {},
    crmUploadedByJobId: {},
    currentCrmLeadId: '',
    pageSaveTimers: {},
    pageSaveTokenByPage: {},
    pendingRowFocus: null,
    pendingParsedScrollTop: null,
    pendingWindowScrollY: null,
    parsedPanelMode: 'table',
    currentParseMode: '',
    reverseRowsBusy: false,
    reversePageOrder: false,
    previewPanelTab: 'preview',
    previewPanelHeightRef: 0,
    previewZoom: 1,
    previewPanActive: false,
    previewPanStartX: 0,
    previewPanStartY: 0,
    previewPanScrollLeft: 0,
    previewPanScrollTop: 0,
    disbalanceLoading: false,
    disbalanceLoadPromise: null,
    summaryRaw: null,
    summaryIncludedMonths: new Set(),
    summaryKnownMonthKeys: new Set(),
    summarySelectionInitialized: false,
    bankCodeFlags: [],
    bankCodeFlagsLoaded: false,
    bankCodeFlagsPromise: null,
    pageProfileByPage: {},
    parseDiagnostics: null,
    googleVisionParserInFlight: false,
    pageNotesByPage: {},
    pageNotesModalOpen: false,
    pageNotesSaving: false,
    pageNotesLoading: false,
    pageAiFixEnabled: false,
    pageAiFixLoading: false,
    pageAiFixApplying: false,
    pageAiFixModalOpen: false,
    pageAiFixDraftByPage: {}
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

  function uploadRowSortValue(row) {
    const candidates = [row?.lastModified, row?.processStarted, row?.createdAt];
    for (const candidate of candidates) {
      const text = String(candidate || '').trim();
      if (!text) continue;
      const parsed = new Date(text);
      if (!Number.isNaN(parsed.getTime())) return parsed.getTime();
    }
    return 0;
  }

  function normalizeBackendOwnedJob(row = {}) {
    const jobId = String(row.job_id || row.jobId || '').trim();
    if (!jobId) return null;
    const sizeValue = Number(row.size_bytes ?? row.file_size ?? 0);
    const progressValue = Number(row.progress ?? 0);
    return {
      jobId,
      fileName: String(row.file_name || row.original_filename || 'document.pdf').trim() || 'document.pdf',
      sizeBytes: Number.isFinite(sizeValue) && sizeValue >= 0 ? sizeValue : 0,
      createdAt: String(row.created_at || '').trim() || null,
      lastModified: String(row.updated_at || row.created_at || '').trim() || null,
      processStarted: String(row.process_started || row.created_at || '').trim() || null,
      processEnd: String(row.process_end || '').trim() || null,
      status: String(row.status || 'queued').trim() || 'queued',
      step: String(row.step || '').trim(),
      progress: Number.isFinite(progressValue) ? progressValue : 0,
      parseMode: String(row.parse_mode || row.requested_mode || '').trim(),
      isReversed: Boolean(row.is_reversed),
      sourceTag: String(row.source_tag || '').trim().toUpperCase(),
      sourceCategory: String(row.source_category || '').trim().toLowerCase(),
      volumeSetName: String(row.volume_set_name || '').trim(),
      volumeFileName: String(row.volume_file_name || '').trim()
    };
  }

  function mergeUploadedJobs(rows = []) {
    const mergedByJobId = new Map();
    for (const existing of Array.isArray(state.uploadedJobs) ? state.uploadedJobs : []) {
      const jobId = String(existing?.jobId || '').trim();
      if (!jobId) continue;
      mergedByJobId.set(jobId, { ...existing });
    }
    for (const item of Array.isArray(rows) ? rows : []) {
      const normalized = normalizeBackendOwnedJob(item);
      if (!normalized) continue;
      const existing = mergedByJobId.get(normalized.jobId) || {};
      mergedByJobId.set(normalized.jobId, { ...existing, ...normalized });
    }
    state.uploadedJobs = Array.from(mergedByJobId.values())
      .sort((left, right) => uploadRowSortValue(right) - uploadRowSortValue(left))
      .slice(0, 100);
    saveStoredJobs();
    renderUploadedRows();
  }

  async function syncOwnedJobs() {
    const payload = await api('/jobs/mine?limit=100');
    const rows = Array.isArray(payload?.rows) ? payload.rows : [];
    mergeUploadedJobs(rows);
    return rows;
  }

  function normalizeRequestedProcessMode(value) {
    const mode = String(value || '').trim().toLowerCase();
    return SUPPORTED_PROCESS_MODES.has(mode) ? mode : 'auto';
  }

  function getRequestedProcessMode() {
    if (!els.mode) return 'auto';
    return normalizeRequestedProcessMode(els.mode.value);
  }

  function setGoogleVisionReparseVisibility(diagnostics) {
    if (els.processingGoogleVisionParserWrap) {
      els.processingGoogleVisionParserWrap.classList.add('hidden');
    }
    if (els.processingGoogleVisionParser) {
      els.processingGoogleVisionParser.disabled = true;
    }
  }

  function initProcessingGoogleVisionParser() {
    if (els.processingGoogleVisionParser) els.processingGoogleVisionParser.disabled = true;
  }

  function initRequestedProcessMode() {
    if (!els.mode) {
      try {
        localStorage.setItem(MODE_STORAGE_KEY, 'auto');
      } catch {
        // no-op
      }
      return;
    }
    let initialMode = 'auto';
    try {
      initialMode = normalizeRequestedProcessMode(localStorage.getItem(MODE_STORAGE_KEY));
    } catch {
      initialMode = 'auto';
    }
    els.mode.value = initialMode;
    els.mode.addEventListener('change', () => {
      const nextMode = getRequestedProcessMode();
      els.mode.value = nextMode;
      try {
        localStorage.setItem(MODE_STORAGE_KEY, nextMode);
      } catch {
        // no-op
      }
    });
  }

  async function reconcileStoredJobsStatuses() {
    try {
      await syncOwnedJobs();
    } catch {
      // best-effort sync only
    }
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
          progress: Number(payload?.progress ?? row.progress ?? 0),
          processStarted: payload?.process_started || row.processStarted || null,
          processEnd: payload?.process_end || row.processEnd || null
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
    updatePageAiFixActionState();
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
    const rawValue = ts;
    const raw = String(rawValue || '').trim();
    if (!raw) return '-';
    if (typeof rawValue === 'number' && Number.isFinite(rawValue)) {
      const d = new Date(rawValue);
      if (!Number.isNaN(d.getTime())) {
        const y = d.getFullYear();
        const m = String(d.getMonth() + 1).padStart(2, '0');
        const day = String(d.getDate()).padStart(2, '0');
        return `${y}-${m}-${day}`;
      }
    }
    if (/^\d{10,13}$/.test(raw)) {
      const numeric = Number(raw);
      const d = new Date(raw.length === 10 ? numeric * 1000 : numeric);
      if (!Number.isNaN(d.getTime())) {
        const y = d.getFullYear();
        const m = String(d.getMonth() + 1).padStart(2, '0');
        const day = String(d.getDate()).padStart(2, '0');
        return `${y}-${m}-${day}`;
      }
    }
    const basic = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (basic) return `${basic[1]}-${basic[2]}-${basic[3]}`;
    const d = new Date(raw.replace(' ', 'T'));
    if (Number.isNaN(d.getTime())) return raw;
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }

  function formatDateTime(ts) {
    const rawValue = ts;
    const raw = String(rawValue || '').trim();
    if (!raw) return '-';
    let date = null;
    if (typeof rawValue === 'number' && Number.isFinite(rawValue)) {
      date = new Date(rawValue);
    } else if (/^\d{10,13}$/.test(raw)) {
      const numeric = Number(raw);
      date = new Date(raw.length === 10 ? numeric * 1000 : numeric);
    } else {
      date = new Date(raw);
    }
    if (Number.isNaN(date.getTime())) return raw;
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    let hours = date.getHours();
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const meridiem = hours >= 12 ? 'PM' : 'AM';
    hours = hours % 12 || 12;
    return `${yyyy}-${mm}-${dd} ${hours}:${minutes} ${meridiem}`;
  }

  function formatElapsed(startTs, endTs = null) {
    const startRaw = String(startTs || '').trim();
    let start = null;
    if (typeof startTs === 'number' && Number.isFinite(startTs)) {
      start = new Date(startTs);
    } else if (/^\d{10,13}$/.test(startRaw)) {
      const numeric = Number(startRaw);
      start = new Date(startRaw.length === 10 ? numeric * 1000 : numeric);
    } else {
      start = new Date(startRaw);
    }
    if (Number.isNaN(start.getTime())) return '-';
    const endRaw = String(endTs || '').trim();
    let end = null;
    if (!endRaw) {
      end = new Date();
    } else if (typeof endTs === 'number' && Number.isFinite(endTs)) {
      end = new Date(endTs);
    } else if (/^\d{10,13}$/.test(endRaw)) {
      const numeric = Number(endRaw);
      end = new Date(endRaw.length === 10 ? numeric * 1000 : numeric);
    } else {
      end = new Date(endRaw);
    }
    if (Number.isNaN(end.getTime())) return '-';
    const diffMs = Math.max(0, end.getTime() - start.getTime());
    const totalSeconds = Math.floor(diffMs / 1000);
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    if (hours > 0) return `${hours}h ${minutes}m`;
    if (minutes > 0) return `${minutes}m ${seconds}s`;
    return `${seconds}s`;
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

  function formatCurrencyOrDash(value, currency = '₱') {
    if (value === null || value === undefined || String(value).trim() === '') return '-';
    const n = Number(value);
    if (!Number.isFinite(n)) return '-';
    return formatCurrency(n, currency);
  }

  function escapeHtml(value) {
    return String(value ?? '')
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }

  function toFiniteNumber(value, fallback = 0) {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
  }

  function monthlyRowKey(row, index) {
    const month = String(row?.month || '').trim();
    if (month) return month;
    return `idx-${index}`;
  }

  function syncSummaryMonthSelection(monthlyRows) {
    const rows = Array.isArray(monthlyRows) ? monthlyRows : [];
    if (!rows.length) {
      state.summaryIncludedMonths = new Set();
      state.summaryKnownMonthKeys = new Set();
      state.summarySelectionInitialized = false;
      return [];
    }

    const keyedRows = rows.map((row, index) => ({ ...row, __monthKey: monthlyRowKey(row, index) }));
    const availableKeys = new Set(keyedRows.map((row) => row.__monthKey));

    if (!state.summarySelectionInitialized) {
      state.summaryIncludedMonths = new Set(availableKeys);
      state.summaryKnownMonthKeys = new Set(availableKeys);
      state.summarySelectionInitialized = true;
      return keyedRows;
    }

    const previousKnownKeys = new Set(state.summaryKnownMonthKeys);
    const next = new Set();
    for (const key of state.summaryIncludedMonths) {
      if (availableKeys.has(key)) next.add(key);
    }
    for (const key of availableKeys) {
      if (!previousKnownKeys.has(key)) next.add(key);
    }
    state.summaryIncludedMonths = next;
    state.summaryKnownMonthKeys = new Set(availableKeys);
    return keyedRows;
  }

  function renderUploadedRows() {
    if (!els.uploadRowsBody) return;
    const search = state.uploadSearch.toLowerCase();
    const rows = state.uploadedJobs.filter((row) => {
      if (!search) return true;
      return `${row.fileName || ''} ${row.jobId || ''} ${row.sourceTag || ''} ${row.volumeSetName || ''}`.toLowerCase().includes(search);
    });
    if (els.uploadsBadge) els.uploadsBadge.textContent = String(state.uploadedJobs.length || 0);
    const hasAnyUploads = state.uploadedJobs.length > 0;
    if (els.uploadEmptyState) els.uploadEmptyState.classList.toggle('hidden', hasAnyUploads);
    if (els.uploadTableWrap) els.uploadTableWrap.classList.toggle('hidden', !hasAnyUploads);

    els.uploadRowsBody.innerHTML = '';
    for (const row of rows) {
      const tr = document.createElement('tr');
      const normalizedStatus = normalizeProcessStatus(row.status || 'queued');
      const progress = Math.max(0, Math.min(100, Number(row.progress ?? 0)));
      const showProgress = normalizedStatus === 'queued' || normalizedStatus === 'processing';
      const sourceTag = String(row.sourceTag || '').trim().toUpperCase();
      const sourceMeta = sourceTag
        ? `
            <div class="file-meta-row">
              <span class="job-source-chip job-source-${escapeHtml(sourceTag.toLowerCase())}">${escapeHtml(sourceTag)}</span>
              ${sourceTag === 'VT' && row.volumeSetName ? `<span class="job-source-meta">Set: ${escapeHtml(row.volumeSetName)}</span>` : ''}
            </div>
          `
        : '';
      const actionButtons =
        normalizedStatus === 'processing'
          ? `
              <div class="row-action-group">
                <button class="row-action-btn action-open" type="button" data-action="open" data-job-id="${escapeHtml(row.jobId)}">Open Processing</button>
                <button class="row-action-btn action-cancel" type="button" data-action="cancel" data-job-id="${escapeHtml(row.jobId)}">Cancel</button>
              </div>
            `
          : normalizedStatus === 'queued'
            ? `
                <div class="row-action-group">
                  <button class="row-action-btn action-queued" type="button" data-action="start" data-job-id="${escapeHtml(row.jobId)}">Begin Processing</button>
                  <button class="row-action-btn action-cancel" type="button" data-action="cancel" data-job-id="${escapeHtml(row.jobId)}">Cancel</button>
                </div>
              `
            : normalizedStatus === 'completed'
              ? `<button class="row-action-btn action-completed" type="button" data-action="open" data-job-id="${escapeHtml(row.jobId)}">View Results</button>`
              : normalizedStatus === 'failed'
                ? `<button class="row-action-btn action-failed" type="button" data-action="start" data-job-id="${escapeHtml(row.jobId)}">Retry</button>`
                : normalizedStatus === 'needs_review'
                  ? `<button class="row-action-btn action-review" type="button" data-action="open" data-job-id="${escapeHtml(row.jobId)}">Open Review</button>`
                  : `<button class="row-action-btn action-queued" type="button" data-action="start" data-job-id="${escapeHtml(row.jobId)}">Begin Processing</button>`;
      tr.innerHTML = `
        <td class="file-name-cell">
          <strong>${escapeHtml(row.fileName || 'document.pdf')}</strong>
          <div class="subtle-id">${escapeHtml(row.jobId || '')}</div>
          ${sourceMeta}
        </td>
        <td>${escapeHtml(formatBytes(row.sizeBytes))}</td>
        <td>${escapeHtml(formatDate(row.lastModified || row.createdAt))}</td>
        <td>${escapeHtml(formatDateTime(row.processStarted || row.createdAt))}</td>
        <td>${escapeHtml(formatElapsed(row.processStarted || row.createdAt, row.processEnd))}</td>
        <td>
          <div class="upload-status-cell">
            <span class="status-pill status-${escapeHtml(normalizedStatus)}">${escapeHtml(formatProcessStatusLabel(normalizedStatus))}</span>
            ${shouldShowProcessStep(normalizedStatus, row.step) ? `<div class="subtle-id">${escapeHtml(normalizeProcessStepForDisplay(row.step))}</div>` : ''}
            ${showProgress ? `
              <div class="upload-row-progress" aria-label="Processing progress">
                <div class="upload-row-progress-track">
                  <div class="upload-row-progress-bar" style="width: ${progress}%"></div>
                </div>
                <div class="upload-row-progress-value">${progress}%</div>
              </div>
            ` : ''}
          </div>
        </td>
        <td>
          ${actionButtons}
        </td>
      `;
      els.uploadRowsBody.appendChild(tr);
    }
    if (hasAnyUploads && rows.length === 0) {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td colspan="7" class="table-empty-cell">No matching files.</td>`;
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
    const sourceItems = Array.isArray(state.crmAttachments) ? state.crmAttachments : [];
    const filteredItems = getCrmTabFilteredItems(sourceItems);
    const hasRows = sourceItems.length > 0;
    const show = !state.crmAttachmentsError && (hasRows || state.crmCurrentOffset > 0 || state.crmHasMore || state.crmLoading);
    els.crmPager.classList.toggle('hidden', !show);

    if (els.crmPageInfo) {
      const visible = filteredItems.length;
      const start = hasRows ? state.crmCurrentOffset + 1 : 0;
      const end = hasRows ? state.crmCurrentOffset + visible : 0;
      els.crmPageInfo.textContent = hasRows ? `Showing ${start}-${end}` : 'Showing 0-0';
    }
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

  function getToastRoot() {
    let root = document.getElementById('toastRoot');
    if (root) return root;
    root = document.createElement('div');
    root.id = 'toastRoot';
    root.className = 'toast-root';
    document.body.appendChild(root);
    return root;
  }

  function showToast(message, type = 'info', durationMs = 3200) {
    const root = getToastRoot();
    const toast = document.createElement('div');
    const tone = String(type || 'info').toLowerCase();
    toast.className = `toast toast-${tone}`;
    toast.textContent = String(message || '').trim() || 'Done';
    root.appendChild(toast);
    window.requestAnimationFrame(() => toast.classList.add('is-visible'));
    window.setTimeout(() => {
      toast.classList.remove('is-visible');
      window.setTimeout(() => toast.remove(), 180);
    }, Math.max(1200, Number(durationMs) || 3200));
  }

  function normalizeProcessStatus(rawStatus) {
    const status = String(rawStatus || '').trim().toLowerCase();
    if (status === 'done') return 'completed';
    if (status === 'done_with_warnings') return 'needs_review';
    if (['queued', 'processing', 'completed', 'failed', 'needs_review', 'uploaded', 'cancelled'].includes(status)) return status;
    return 'not_started';
  }

  function formatProcessStatusLabel(status) {
    const key = normalizeProcessStatus(status);
    if (key === 'not_started') return 'Not Started';
    if (key === 'needs_review') return 'Needs Review';
    if (key === 'uploaded') return 'Uploaded';
    if (key === 'cancelled') return 'Cancelled';
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
    if (value === 'done_with_warnings' || value === 'completed_with_warnings') return 'needs_review';
    return value;
  }

  function shouldShowProcessStep(status, step) {
    const normalizedStatus = normalizeProcessStatus(status);
    const normalizedStep = normalizeProcessStepForDisplay(step);
    if (!normalizedStep) return false;
    return normalizedStep !== normalizedStatus;
  }

  function normalizeCrmStatusTab(rawTab) {
    const tab = String(rawTab || '').trim().toLowerCase();
    if (tab === 'queued' || tab === 'completed') return tab;
    return 'not_started';
  }

  function resolveCrmProcessState(item) {
    const attachmentId = String(item?.attachment_id || '').trim();
    const fallback = {
      jobId: String(item?.process_job_id || '').trim(),
      status: normalizeProcessStatus(item?.process_status),
      step: String(item?.process_step || '').trim(),
      progress: Number(item?.process_progress || 0),
    };
    if (fallback.jobId && state.crmUploadedByJobId[fallback.jobId]) {
      fallback.status = 'uploaded';
      fallback.step = 'uploaded';
      fallback.progress = 100;
    }
    if (!attachmentId) return fallback;
    const cached = state.crmProcessByAttachment[attachmentId];
    if (!cached) return fallback;
    const resolved = {
      jobId: String(cached.jobId || fallback.jobId).trim(),
      status: normalizeProcessStatus(cached.status || fallback.status),
      step: String(cached.step || fallback.step).trim(),
      progress: Number(cached.progress ?? fallback.progress ?? 0),
    };
    if (resolved.jobId && state.crmUploadedByJobId[resolved.jobId]) {
      resolved.status = 'uploaded';
      resolved.step = 'uploaded';
      resolved.progress = 100;
    }
    return resolved;
  }

  function crmProcessMatchesTab(processStatus, tab) {
    const status = normalizeProcessStatus(processStatus);
    const tabKey = normalizeCrmStatusTab(tab);
    if (tabKey === 'queued') return status === 'queued' || status === 'processing';
    if (tabKey === 'completed') return status === 'completed' || status === 'needs_review' || status === 'uploaded';
    return status === 'not_started' || status === 'failed' || status === 'cancelled';
  }

  function getCrmTabFilteredItems(sourceItems) {
    const items = Array.isArray(sourceItems) ? sourceItems : [];
    const tab = normalizeCrmStatusTab(state.crmStatusTab);
    return items.filter((item) => crmProcessMatchesTab(resolveCrmProcessState(item).status, tab));
  }

  function renderCrmStatusTabs() {
    const tabs = Array.isArray(els.crmStatusTabs) ? els.crmStatusTabs : [];
    if (!tabs.length) return;

    const counts = { not_started: 0, queued: 0, completed: 0 };
    const sourceItems = Array.isArray(state.crmAttachments) ? state.crmAttachments : [];
    for (const item of sourceItems) {
      const status = resolveCrmProcessState(item).status;
      if (crmProcessMatchesTab(status, 'queued')) counts.queued += 1;
      else if (crmProcessMatchesTab(status, 'completed')) counts.completed += 1;
      else counts.not_started += 1;
    }

    const active = normalizeCrmStatusTab(state.crmStatusTab);
    for (const button of tabs) {
      const tab = normalizeCrmStatusTab(button.dataset.crmStatusTab);
      const selected = tab === active;
      const baseLabel = String(button.dataset.label || button.textContent || '').trim() || 'Status';
      button.textContent = `${baseLabel} (${counts[tab] || 0})`;
      button.classList.toggle('active', selected);
      button.setAttribute('aria-selected', selected ? 'true' : 'false');
    }
  }

  function setCrmStatusTab(tab) {
    const next = normalizeCrmStatusTab(tab);
    if (next === state.crmStatusTab) return;
    state.crmStatusTab = next;
    renderCrmStatusTabs();
    renderCrmAttachmentRows();
  }

  function syncCrmProcessMapFromItems(items) {
    const next = {};
    for (const item of items) {
      const attachmentId = String(item?.attachment_id || '').trim();
      if (!attachmentId) continue;
      const previous = state.crmProcessByAttachment[attachmentId] || {};
      const jobId = String(item?.process_job_id || previous.jobId || '').trim();
      const step = String(item?.process_step || previous.step || '').trim();
      let status = normalizeProcessStatus(item?.process_status || previous.status || 'not_started');
      const rawProgress = item?.process_progress ?? previous.progress ?? 0;
      let progress = Number.isFinite(Number(rawProgress)) ? Number(rawProgress) : 0;
      let nextStep = step;
      if (jobId && state.crmUploadedByJobId[jobId]) {
        status = 'uploaded';
        nextStep = 'uploaded';
        progress = 100;
      }
      next[attachmentId] = { jobId, status, step: nextStep, progress };
    }
    state.crmProcessByAttachment = next;
  }

  function markCrmExportUploadedByJob(jobId) {
    const targetJobId = String(jobId || '').trim();
    if (!targetJobId) return;
    state.crmUploadedByJobId[targetJobId] = { uploadedAt: Date.now() };

    for (const [attachmentId, process] of Object.entries(state.crmProcessByAttachment || {})) {
      if (String(process?.jobId || '').trim() !== targetJobId) continue;
      state.crmProcessByAttachment[attachmentId] = {
        ...process,
        status: 'uploaded',
        step: 'uploaded',
        progress: 100,
      };
    }

    if (Array.isArray(state.crmAttachments) && state.crmAttachments.length) {
      for (const item of state.crmAttachments) {
        const processJobId = String(item?.process_job_id || '').trim();
        if (processJobId !== targetJobId) continue;
        item.process_status = 'uploaded';
        item.process_step = 'uploaded';
        item.process_progress = 100;
      }
    }

    renderCrmAttachmentRows();
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

    const sourceItems = Array.isArray(state.crmAttachments) ? state.crmAttachments : [];
    renderCrmStatusTabs();
    const searchActive = Boolean(String(state.crmSearch || '').trim());
    const items = getCrmTabFilteredItems(sourceItems);
    const hasSourceRows = sourceItems.length > 0;
    const hasRows = items.length > 0;
    const hasError = Boolean(state.crmAttachmentsError);
    const showTable = state.crmLoading || hasRows || hasError || hasSourceRows;

    if (els.crmAttachmentsTableWrap) els.crmAttachmentsTableWrap.classList.toggle('hidden', !showTable);
    if (els.crmAttachmentsEmptyState) {
      const showEmpty = !state.crmLoading && !hasSourceRows && !hasError;
      els.crmAttachmentsEmptyState.classList.toggle('hidden', !showEmpty);
    }

    els.crmAttachmentsRowsBody.innerHTML = '';

    if (state.crmLoading) {
      const tr = document.createElement('tr');
      tr.innerHTML = '<td colspan="7" class="table-empty-cell">Loading CRM files…</td>';
      els.crmAttachmentsRowsBody.appendChild(tr);
      setCrmLoadMoreState();
      setCrmPaginationState();
      return;
    }

    if (hasError) {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td colspan="7" class="table-empty-cell">${escapeHtml(state.crmAttachmentsError)}</td>`;
      els.crmAttachmentsRowsBody.appendChild(tr);
      setCrmLoadMoreState();
      setCrmPaginationState();
      return;
    }

    if (!hasRows) {
      const tr = document.createElement('tr');
      if (searchActive) {
        tr.innerHTML = '<td colspan="7" class="table-empty-cell">No matching CRM files.</td>';
      } else if (hasSourceRows) {
        tr.innerHTML = '<td colspan="7" class="table-empty-cell">No CRM files in this tab.</td>';
      } else {
        tr.innerHTML = '<td colspan="7" class="table-empty-cell">No CRM files found.</td>';
      }
      els.crmAttachmentsRowsBody.appendChild(tr);
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
      const process = resolveCrmProcessState(item);
      const processStatus = normalizeProcessStatus(process.status);
      const leadId = sourceType === 'Lead' ? recordId : '';

      let actionCell = '<span class="subtle-id">Unavailable</span>';
      if (isAvailable) {
        if (process.jobId && (processStatus === 'queued' || processStatus === 'processing')) {
          actionCell = `<button class="row-action-btn action-queued" type="button" data-open-job-id="${escapeHtml(process.jobId)}" data-lead-id="${escapeHtml(leadId)}">Open Processing</button>`;
        } else if (process.jobId && (processStatus === 'completed' || processStatus === 'needs_review' || processStatus === 'uploaded')) {
          actionCell = `<button class="row-action-btn action-completed" type="button" data-open-job-id="${escapeHtml(process.jobId)}" data-lead-id="${escapeHtml(leadId)}">Open Result</button>`;
        } else if (processStatus === 'failed') {
          actionCell = `<button class="row-action-btn action-failed" type="button" data-process-attachment-id="${escapeHtml(attachmentId)}" data-lead-id="${escapeHtml(leadId)}">Retry Process</button>`;
        } else {
          actionCell = `<button class="row-action-btn action-completed" type="button" data-process-attachment-id="${escapeHtml(attachmentId)}" data-lead-id="${escapeHtml(leadId)}">Begin Process</button>`;
        }
      }

      const fileNameCell = isAvailable
        ? escapeHtml(item.filename || '-')
        : `${escapeHtml(item.filename || '-')}${item.error ? `<div class="subtle-id">${escapeHtml(item.error)}</div>` : ''}`;
      const statusCell = `<span class="status-pill ${processStatusClass(processStatus)}">${escapeHtml(formatProcessStatusLabel(processStatus))}</span>${shouldShowProcessStep(processStatus, process.step) ? `<div class="subtle-id">${escapeHtml(process.step)}</div>` : ''}`;
      const recordEntity = sourceType === 'Business Profile' ? 'Account' : 'Lead';
      const accountName = escapeHtml(item.account_name || '-');
      const accountNameCell = recordId
        ? `<a href="https://staging-crm.discoverycsc.com/#${recordEntity}/view/${encodeURIComponent(recordId)}" target="_blank" rel="noopener noreferrer">${accountName}</a>`
        : accountName;
      const createdAtCell = escapeHtml(formatDate(item.created_at || item.createdAt || ''));

      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${accountNameCell}</td>
        <td>${escapeHtml(sourceType || '-')}</td>
        <td>${createdAtCell}</td>
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
      const searchQuery = String(state.crmSearch || '').trim();
      if (searchQuery) params.set('q', searchQuery);
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
    if (state.view === 'uploads') {
      if (canAccessCrmAttachments) startCrmStatusPolling();
      if (state.authRole) {
        syncOwnedJobs().catch(() => {
          // best-effort sync only
        });
      }
    } else {
      stopCrmStatusPolling();
    }
    setGoogleVisionReparseVisibility(state.parseDiagnostics || null);
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
        alert(`Load job failed: ${normalizeApiErrorMessage(err?.message)}`);
      });
    }
    const targetPath = buildRoute(view, routeJobId);
    const current = `${window.location.pathname}${window.location.search}`;
    if (current !== targetPath) {
      const fn = replace ? window.history.replaceState : window.history.pushState;
      fn.call(window.history, {}, '', targetPath);
    }
  }

  // Apply the latest backend status payload to both the processing header and cached upload row state.
  function setStatus(payload) {
    const raw = String(payload.status || 'idle').toLowerCase();
    const mapped = raw === 'done' ? 'completed' : raw === 'done_with_warnings' ? 'needs_review' : raw;
    if (els.jobStatus) {
      els.jobStatus.textContent = mapped === 'idle' ? 'idle' : formatProcessStatusLabel(mapped);
      els.jobStatus.classList.remove('status-idle', 'status-queued', 'status-processing', 'status-completed', 'status-failed', 'status-needs_review', 'status-cancelled');
      if (mapped === 'queued') els.jobStatus.classList.add('status-queued');
      else if (mapped === 'processing') els.jobStatus.classList.add('status-processing');
      else if (mapped === 'completed') els.jobStatus.classList.add('status-completed');
      else if (mapped === 'failed') els.jobStatus.classList.add('status-failed');
      else if (mapped === 'needs_review') els.jobStatus.classList.add('status-needs_review');
      else if (mapped === 'cancelled') els.jobStatus.classList.add('status-cancelled');
      else els.jobStatus.classList.add('status-idle');
    }
    if (els.jobStep) els.jobStep.textContent = payload.step || '-';
    const progress = Math.max(0, Math.min(100, Number(payload.progress ?? 0)));
    if (els.jobProgress) els.jobProgress.textContent = `${progress}%`;
    if (els.jobProgressFill) els.jobProgressFill.style.width = `${progress}%`;
    state.isCompleted = mapped === 'completed' || mapped === 'needs_review';
    state.currentParseMode = String(payload.parse_mode || state.currentParseMode || '').trim().toLowerCase();
    state.pageAiFixEnabled = Boolean(payload?.page_ai_fix_enabled);

    if (els.startBtn) {
      const allowStart = Boolean(state.jobId) && !['processing', 'completed', 'needs_review', 'failed'].includes(mapped);
      els.startBtn.disabled = !allowStart;
      els.startBtn.classList.toggle('start-emphasis', mapped === 'idle' && Boolean(state.jobId));
    }
    updateExportAvailability();
    updatePageAiFixActionState();
    if (mapped === 'idle' && state.totalParsedRows === 0) renderSummary(null);
    updatePreviewEmptyState();

    if (state.jobId) {
      updateUploadedJobIfExists({
        jobId: state.jobId,
        status: payload.status || 'queued',
        step: payload.step || 'queued',
        progress: Number(payload.progress ?? 0),
        parseMode: payload.parse_mode,
        processStarted: payload.process_started || null,
        processEnd: payload.process_end || null
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

    if (els.exportCrm) {
      if (!state.jobId || !enabled) {
        els.exportCrm.classList.add('disabled');
        els.exportCrm.setAttribute('aria-disabled', 'true');
      } else {
        els.exportCrm.classList.remove('disabled');
        els.exportCrm.setAttribute('aria-disabled', 'false');
      }
    }
  }

  async function exportToCrm() {
    if (!state.jobId) return;
    if (!els.exportCrm || els.exportCrm.classList.contains('disabled')) return;
    const button = els.exportCrm;
    const originalLabel = button.textContent || 'Export to CRM';

    button.classList.add('disabled');
    button.setAttribute('aria-disabled', 'true');
    button.textContent = 'Exporting…';
    try {
      const params = new URLSearchParams();
      const mappedLeadId = String(state.crmLeadByJobId[state.jobId] || state.currentCrmLeadId || '').trim();
      if (mappedLeadId) params.set('lead_id', mappedLeadId);
      const path = `/crm/jobs/${encodeURIComponent(state.jobId)}/export-excel${params.toString() ? `?${params.toString()}` : ''}`;
      const payload = await api(path, {
        method: 'POST',
      });
      const leadId = String(payload?.lead_id || '').trim();
      const attachmentId = String(payload?.attachment_id || '').trim();
      markCrmExportUploadedByJob(state.jobId);
      showToast(`Exported to CRM successfully.${leadId ? ` Lead: ${leadId}.` : ''}${attachmentId ? ` Attachment: ${attachmentId}.` : ''}`, 'success');
    } catch (err) {
      showToast(`Export to CRM failed: ${normalizeApiErrorMessage(err?.message)}`, 'error', 4200);
    } finally {
      button.textContent = originalLabel;
      updateExportAvailability();
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
    renderDisbalanceTable();
    renderFlaggedTransactionsTable();
    updateReverseRowsActionState();
    syncParsedSectionHeightToPreview();
  }

  function setParsedPanelMode(mode) {
    const nextMode = mode === 'json' ? 'json' : 'table';
    state.parsedPanelMode = nextMode;
    if (els.parsedTableWrap) els.parsedTableWrap.classList.toggle('hidden', nextMode !== 'table');
    if (els.parsedJsonWrap) els.parsedJsonWrap.classList.toggle('hidden', nextMode !== 'json');
    updateReverseRowsActionState();
    syncParsedSectionHeightToPreview();
  }

  function updateReverseRowsActionState() {
    if (!els.reverseRowsBtn) return;
    if (state.parsedPanelMode !== 'table') {
      els.reverseRowsBtn.disabled = true;
      return;
    }
    if (state.reverseRowsBusy) {
      els.reverseRowsBtn.disabled = true;
      return;
    }
    const canReverse = state.pages.length > 1 || Object.values(state.parsedByPage).some((rows) => Array.isArray(rows) && rows.length > 1);
    els.reverseRowsBtn.disabled = !canReverse;
  }

  function normalizePreviewPanelTab(rawTab) {
    const tab = String(rawTab || '').trim().toLowerCase();
    if (tab === 'disbalance' || tab === 'flagged') return tab;
    return 'preview';
  }

  function formatSignedAmount(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return '-';
    const formatted = Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return `${n < 0 ? '-' : ''}${formatted}`;
  }

  function pageSortValue(page) {
    const text = String(page || '').trim().toLowerCase();
    const m = text.match(/page[_-]?(\d+)/);
    return m ? Number(m[1]) : Number.MAX_SAFE_INTEGER;
  }

  function getCurrentPageNumber() {
    const value = pageSortValue(state.currentPage);
    return Number.isFinite(value) && value !== Number.MAX_SAFE_INTEGER ? value : 0;
  }

  function getTotalPageCount() {
    return state.pages.length;
  }

  function clampPageNumber(value) {
    const total = getTotalPageCount();
    if (total <= 0) return 0;
    const numeric = Number.parseInt(String(value || '').trim(), 10);
    if (!Number.isFinite(numeric)) return getCurrentPageNumber() || 1;
    return Math.min(total, Math.max(1, numeric));
  }

  function findPageKeyByNumber(pageNumber) {
    const target = clampPageNumber(pageNumber);
    if (target <= 0) return null;
    return state.pages.find((page) => pageSortValue(page) === target) || null;
  }

  function getOrderedRows(page) {
    const rows = Array.isArray(state.parsedByPage[page]) ? state.parsedByPage[page] : [];
    return state.reversePageOrder ? rows.slice().reverse() : rows;
  }

  function getOrderedPages() {
    const pages = Array.isArray(state.pages) && state.pages.length
      ? state.pages.slice()
      : Object.keys(state.parsedByPage || {}).sort((a, b) => pageSortValue(a) - pageSortValue(b));
    return state.reversePageOrder ? pages.slice().sort((a, b) => pageSortValue(b) - pageSortValue(a)) : pages;
  }

  function collectDisbalanceEntries() {
    const entries = [];
    for (const page of getOrderedPages()) {
      const rows = getOrderedRows(page);
      for (let idx = 0; idx < rows.length; idx += 1) {
        const current = rows[idx] || {};
        const rowId = String(current?.row_id || '').trim() || String(idx + 1).padStart(3, '0');
        if (!current?.is_disbalanced) continue;

        const expected = toFiniteNumber(current?.disbalance_expected_balance, null);
        const actual = parseAmountForFormatting(current.balance);
        const delta = toFiniteNumber(current?.disbalance_delta, null);
        if (!Number.isFinite(expected) || !Number.isFinite(actual) || !Number.isFinite(delta)) continue;

        entries.push({
          page,
          rowId,
          date: normalizeEditableCellValue(current.date).trim(),
          description: normalizeEditableCellValue(current.description).trim(),
          expected,
          actual,
          delta,
        });
      }
    }
    return entries;
  }

  function collectFlaggedTransactionEntries() {
    const entries = [];
    for (const page of getOrderedPages()) {
      const rows = getOrderedRows(page);
      for (let idx = 0; idx < rows.length; idx += 1) {
        const row = rows[idx] || {};
        const description = normalizeEditableCellValue(row?.description).trim();
        const match = getDescriptionFlagMatch(description, page);
        const isFlagged = Boolean(row?.is_flagged) || Boolean(match);
        if (!isFlagged) continue;

        entries.push({
          page,
          rowId: String(row?.row_id || '').trim() || String(idx + 1).padStart(3, '0'),
          date: normalizeEditableCellValue(row?.date).trim(),
          description,
          bank: match?.bank || '',
          code: match?.code || '',
        });
      }
    }
    return entries;
  }

  function formatSupplementalPageLabel(page) {
    const value = pageSortValue(page);
    return value > 0 ? String(value) : normalizeEditableCellValue(page).trim();
  }

  function formatSupplementalRowLabel(rowId) {
    const raw = normalizeEditableCellValue(rowId).trim();
    if (!raw) return '';
    const numeric = Number.parseInt(raw, 10);
    return Number.isFinite(numeric) ? String(numeric) : raw;
  }

  function updatePreviewSupplementalTabLabels() {
    if (els.previewTabDisbalanceBtn) {
      const loadedCount = collectDisbalanceEntries().length;
      els.previewTabDisbalanceBtn.textContent = state.disbalanceLoading
        ? `Disbalance (${loadedCount}+)`
        : `Disbalance (${loadedCount})`;
    }
    if (els.previewTabFlaggedBtn) {
      const count = collectFlaggedTransactionEntries().length;
      els.previewTabFlaggedBtn.textContent = `Flagged Transactions (${count})`;
    }
  }

  function renderDisbalanceTable() {
    if (!els.disbalanceRowsBody) return;
    const rowsBody = els.disbalanceRowsBody;
    rowsBody.innerHTML = '';
    updatePreviewSupplementalTabLabels();

    if (state.disbalanceLoading) {
      const tr = document.createElement('tr');
      tr.innerHTML = '<td colspan="7" class="table-empty-cell">Loading disbalance rows…</td>';
      rowsBody.appendChild(tr);
      return;
    }

    const entries = collectDisbalanceEntries();

    if (!entries.length) {
      const tr = document.createElement('tr');
      tr.innerHTML = '<td colspan="7" class="table-empty-cell">No disbalance found.</td>';
      rowsBody.appendChild(tr);
      return;
    }

    for (const entry of entries) {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${escapeHtml(formatSupplementalPageLabel(entry.page))}</td>
        <td>${escapeHtml(formatSupplementalRowLabel(entry.rowId))}</td>
        <td>${escapeHtml(formatParsedRowDate(entry.date))}</td>
        <td>${escapeHtml(formatSignedAmount(entry.expected))}</td>
        <td>${escapeHtml(formatSignedAmount(entry.actual))}</td>
        <td class="${entry.delta < 0 ? 'is-negative' : ''}">${escapeHtml(formatSignedAmount(entry.delta))}</td>
        <td><button class="row-action-btn action-review disbalance-jump-btn" type="button" data-page="${escapeHtml(entry.page)}" data-row-id="${escapeHtml(entry.rowId)}">Go to Row</button></td>
      `;
      rowsBody.appendChild(tr);
    }
  }

  function renderFlaggedTransactionsTable() {
    if (!els.flaggedRowsBody) return;
    const rowsBody = els.flaggedRowsBody;
    rowsBody.innerHTML = '';
    updatePreviewSupplementalTabLabels();

    if (state.disbalanceLoading) {
      const tr = document.createElement('tr');
      tr.innerHTML = '<td colspan="6" class="table-empty-cell">Loading flagged transactions…</td>';
      rowsBody.appendChild(tr);
      return;
    }

    const entries = collectFlaggedTransactionEntries();
    if (!entries.length) {
      const tr = document.createElement('tr');
      tr.innerHTML = '<td colspan="6" class="table-empty-cell">No flagged transactions found.</td>';
      rowsBody.appendChild(tr);
      return;
    }

    for (const entry of entries) {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${escapeHtml(formatSupplementalPageLabel(entry.page))}</td>
        <td>${escapeHtml(formatSupplementalRowLabel(entry.rowId))}</td>
        <td>${escapeHtml(formatParsedRowDate(entry.date))}</td>
        <td>${escapeHtml(entry.description || '-')}</td>
        <td>${escapeHtml(entry.code || '-')}</td>
        <td><button class="row-action-btn action-review flagged-jump-btn" type="button" data-page="${escapeHtml(entry.page)}" data-row-id="${escapeHtml(entry.rowId)}">Go to Row</button></td>
      `;
      rowsBody.appendChild(tr);
    }
  }

  async function ensureAllPagesParsedLoaded() {
    if (!state.jobId) return;
    const missingPages = (state.pages || []).filter((page) => !Array.isArray(state.parsedByPage[page]));
    if (!missingPages.length) return;
    if (state.disbalanceLoadPromise) return state.disbalanceLoadPromise;

    state.disbalanceLoading = true;
    renderDisbalanceTable();
    renderFlaggedTransactionsTable();

    state.disbalanceLoadPromise = (async () => {
      let payload = null;
      try {
        payload = await api(`/jobs/${state.jobId}/parsed`);
      } catch {
        payload = null;
      }

      const rowsByPage = payload && typeof payload === 'object' ? payload : {};
      for (const page of missingPages) {
        if (Array.isArray(state.parsedByPage[page])) continue;
        const rows = rowsByPage && Object.prototype.hasOwnProperty.call(rowsByPage, page)
          ? rowsByPage[page]
          : [];
        state.parsedByPage[page] = Array.isArray(rows) ? rows : [];
      }
    })();

    try {
      await state.disbalanceLoadPromise;
    } finally {
      state.disbalanceLoadPromise = null;
      state.disbalanceLoading = false;
      renderDisbalanceTable();
      renderFlaggedTransactionsTable();
    }
  }

  function setPreviewPanelTab(tab) {
    const next = normalizePreviewPanelTab(tab);
    const wasSupplemental = state.previewPanelTab !== 'preview';
    const isSupplemental = next !== 'preview';
    if (isSupplemental && state.previewPanelTab === 'preview' && els.previewSectionCard) {
      const currentHeight = Math.round(els.previewSectionCard.getBoundingClientRect().height || 0);
      if (Number.isFinite(currentHeight) && currentHeight > 0) {
        state.previewPanelHeightRef = currentHeight;
      }
    }
    state.previewPanelTab = next;
    const isDisbalance = next === 'disbalance';
    const isFlagged = next === 'flagged';

    if (els.previewImagePanel) els.previewImagePanel.classList.toggle('hidden', isSupplemental);
    if (els.previewDisbalancePanel) els.previewDisbalancePanel.classList.toggle('hidden', !isDisbalance);
    if (els.previewFlaggedPanel) els.previewFlaggedPanel.classList.toggle('hidden', !isFlagged);
    if (els.previewSectionCard) {
      if (isSupplemental && state.previewPanelHeightRef > 0) {
        els.previewSectionCard.style.minHeight = `${state.previewPanelHeightRef}px`;
      } else {
        els.previewSectionCard.style.removeProperty('min-height');
      }
    }

    const buttons = Array.isArray(els.previewTabButtons) ? els.previewTabButtons : [];
    for (const button of buttons) {
      const active = normalizePreviewPanelTab(button.dataset.previewTab) === next;
      button.classList.toggle('active', active);
      button.setAttribute('aria-selected', active ? 'true' : 'false');
    }

    if (isSupplemental) {
      renderDisbalanceTable();
      renderFlaggedTransactionsTable();
      const supplementalTasks = [ensureAllPagesParsedLoaded()];
      if (isFlagged) supplementalTasks.unshift(ensureUiSettingsLoaded());
      Promise.all(supplementalTasks).catch(() => {
        renderDisbalanceTable();
        renderFlaggedTransactionsTable();
      });
    } else {
      updatePreviewSupplementalTabLabels();
      if (wasSupplemental) {
        window.requestAnimationFrame(() => {
          loadPreview({ preserveSelectedRow: true, forceReload: true });
          drawSelectedBound();
          syncParsedSectionHeightToPreview();
          updatePreviewEmptyState();
        });
      }
    }
    syncParsedSectionHeightToPreview();
    updatePreviewEmptyState();
  }

  async function jumpToParsedRow(page, rowId) {
    const targetPage = String(page || '').trim();
    const targetRowId = String(rowId || '').trim();
    if (!targetPage || !targetRowId) return;

    if (state.parsedPanelMode === 'json') {
      setParsedPanelMode('table');
    }

    if (state.currentPage !== targetPage) {
      state.currentPage = targetPage;
      state.selectedRowId = null;
      updatePageNav();
      await loadCurrentPageData();
    } else {
      renderRows();
    }

    selectRow(targetRowId);
    setPreviewPanelTab('preview');
    focusParsedRow(targetRowId, { block: 'center', behavior: 'smooth', focusInput: false });
  }

  function isDisbalancedRow(page, rowId) {
    const rows = getOrderedRows(page);
    const targetRowId = String(rowId || '').trim();
    if (!targetRowId) return false;
    return rows.some((row, idx) => {
      const currentRowId = String(row?.row_id || '').trim() || String(idx + 1).padStart(3, '0');
      return currentRowId === targetRowId && Boolean(row?.is_disbalanced);
    });
  }

  function buildMissingTransactionRow(page, rowId, rows) {
    const context = findOrderedRowContext(page, rowId);
    if (!context || !context.row || !context.prevRow) return null;

    const { row, prevRow } = context;
    const prevBal = parseAmountForFormatting(prevRow.balance);
    const delta = toFiniteNumber(row?.disbalance_delta, null);
    if (!Number.isFinite(prevBal) || !Number.isFinite(delta) || Math.abs(delta) <= 0.01) return null;

    const nextDebit = delta < 0 ? Math.abs(delta) : null;
    const nextCredit = delta > 0 ? delta : null;

    return {
      row_id: buildNextRowId(rows),
      date: formatParsedRowDate(normalizeEditableCellValue(row.date)),
      description: 'Missing Transaction',
      debit: Number.isFinite(nextDebit) && Math.abs(nextDebit) > 0.005 ? formatAmountCellValue(nextDebit) : '',
      credit: Number.isFinite(nextCredit) && Math.abs(nextCredit) > 0.005 ? formatAmountCellValue(nextCredit) : '',
      balance: formatAmountCellValue(prevBal + delta),
    };
  }

  function resolveInsertIndex(page, rowId, { beforeCurrent = false } = {}) {
    const rows = Array.isArray(state.parsedByPage[page]) ? state.parsedByPage[page] : [];
    const idx = rows.findIndex((row) => String(row?.row_id || '') === String(rowId || ''));
    if (idx < 0) return -1;
    if (beforeCurrent) {
      return state.reversePageOrder ? idx + 1 : idx;
    }
    return state.reversePageOrder ? idx : idx + 1;
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
    const parsedRows = page ? getOrderedRows(page) : [];
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
    if (state.previewPanelTab === 'preview') {
      state.previewPanelHeightRef = previewHeight;
    }

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

  const BANK_TO_PROFILE_ALIASES = {
    AUB: ['AUB'],
    BDO: ['BDO'],
    BPI: ['BPI'],
    METROBANK: ['METROBANK'],
    RCBC: ['RCBC'],
    SECB: ['SECB', 'SECURITY_BANK'],
    'SECURITY BANK': ['SECB', 'SECURITY_BANK']
  };

  function normalizeProfileAlias(value) {
    return String(value || '').trim().toUpperCase().replaceAll(' ', '_');
  }

  function normalizeBankCode(value) {
    return String(value || '').trim().toUpperCase().replace(/[^A-Z0-9_-]/g, '');
  }

  function inferProfileAliasesFromBank(bankName) {
    const bank = String(bankName || '').trim().toUpperCase();
    const aliases = BANK_TO_PROFILE_ALIASES[bank] || BANK_TO_PROFILE_ALIASES[bank.replaceAll('_', ' ')] || [bank];
    return aliases
      .map((value) => normalizeProfileAlias(value))
      .filter(Boolean)
      .filter((value, idx, arr) => arr.indexOf(value) === idx);
  }

  function normalizeBankCodeFlagRows(rawRows) {
    if (!Array.isArray(rawRows)) return [];
    const output = [];
    for (const item of rawRows) {
      if (!item || typeof item !== 'object') continue;
      const bank = String(item.bank || '').trim().toUpperCase();
      if (!bank) continue;

      const rawCodes = Array.isArray(item.codes)
        ? item.codes
        : String(item.codes || '')
          .replaceAll('\n', ',')
          .split(',');
      const codes = rawCodes
        .map((code) => normalizeBankCode(code))
        .filter(Boolean)
        .filter((code, idx, arr) => arr.indexOf(code) === idx);
      if (!codes.length) continue;

      const profileAliases = Array.isArray(item.profile_aliases) && item.profile_aliases.length
        ? item.profile_aliases
          .map((alias) => normalizeProfileAlias(alias))
          .filter(Boolean)
          .filter((alias, idx, arr) => arr.indexOf(alias) === idx)
        : inferProfileAliasesFromBank(bank);

      output.push({ bank, codes, profileAliases });
    }
    return output;
  }

  function escapeRegex(value) {
    return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  function descriptionContainsCode(description, code) {
    const text = String(description || '').toUpperCase();
    const token = String(code || '').toUpperCase();
    if (!text || !token) return false;
    const pattern = new RegExp(`(^|[^A-Z0-9])${escapeRegex(token)}([^A-Z0-9]|$)`);
    return pattern.test(text);
  }

  function resolveActiveBankCodeEntries(page) {
    const entries = Array.isArray(state.bankCodeFlags) ? state.bankCodeFlags : [];
    if (!entries.length) return [];
    const profile = normalizeProfileAlias(state.pageProfileByPage[page]);
    if (!profile) return entries;
    const matched = entries.filter((entry) => Array.isArray(entry.profileAliases) && entry.profileAliases.includes(profile));
    return matched.length ? matched : entries;
  }

  function getDescriptionFlagMatch(description, page) {
    const entries = resolveActiveBankCodeEntries(page);
    for (const entry of entries) {
      for (const code of entry.codes || []) {
        if (descriptionContainsCode(description, code)) {
          return { bank: entry.bank, code };
        }
      }
    }
    return null;
  }

  async function ensureUiSettingsLoaded() {
    if (state.bankCodeFlagsLoaded) return;
    if (state.bankCodeFlagsPromise) {
      await state.bankCodeFlagsPromise;
      return;
    }

    state.bankCodeFlagsPromise = (async () => {
      try {
        const settings = await api('/ui/settings');
        state.uploadTestingEnabled = Boolean(settings?.upload_testing_enabled);
        state.bankCodeFlags = normalizeBankCodeFlagRows(settings?.bank_code_flags);
        state.bankCodeFlagsLoaded = true;
      } catch {
        state.uploadTestingEnabled = false;
        state.bankCodeFlags = [];
        state.bankCodeFlagsLoaded = false;
      }

      applyFeatureVisibility();
      await refreshBankCodeFlagUi();
    })();

    try {
      await state.bankCodeFlagsPromise;
    } finally {
      state.bankCodeFlagsPromise = null;
    }
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

  function normalizeCommittedFieldValue(field, value) {
    const raw = normalizeEditableCellValue(value).trim();
    if (!raw) return '';
    if (AMOUNT_ROW_FIELDS.has(field)) return formatAmountCellValue(raw);
    return raw;
  }

  function getRenderedFieldValue(field, value) {
    if (field === 'date') return formatParsedRowDate(normalizeEditableCellValue(value));
    if (AMOUNT_ROW_FIELDS.has(field)) return formatAmountCellValue(value);
    return normalizeEditableCellValue(value);
  }

  function findOrderedRowContext(page, rowId) {
    const rows = getOrderedRows(page);
    const index = rows.findIndex((row, idx) => {
      const currentRowId = String(row?.row_id || '').trim() || String(idx + 1).padStart(3, '0');
      return currentRowId === String(rowId || '').trim();
    });
    if (index < 0) return null;
    return {
      rows,
      index,
      row: rows[index],
      prevRow: index > 0 ? rows[index - 1] : null,
    };
  }

  function hasUsableAmountValue(raw, parsed) {
    return raw === '' || Number.isFinite(parsed);
  }

  function amountsRoughlyEqual(left, right, tolerance = 0.01) {
    return Number.isFinite(left) && Number.isFinite(right) && Math.abs(left - right) <= tolerance;
  }

  function inferBalanceFlowDirection(rows) {
    if (!Array.isArray(rows) || rows.length < 2) return null;

    let ascendingHits = 0;
    let descendingHits = 0;
    for (let idx = 1; idx < rows.length; idx += 1) {
      const prev = rows[idx - 1] || {};
      const current = rows[idx] || {};
      const prevBal = parseAmountForFormatting(prev.balance);
      const currBal = parseAmountForFormatting(current.balance);
      const debit = parseAmountForFormatting(current.debit);
      const credit = parseAmountForFormatting(current.credit);
      if (!Number.isFinite(prevBal) || !Number.isFinite(currBal)) continue;

      if (Number.isFinite(debit) && !Number.isFinite(credit)) {
        if (amountsRoughlyEqual(prevBal - debit, currBal)) ascendingHits += 1;
        if (amountsRoughlyEqual(prevBal + debit, currBal)) descendingHits += 1;
      } else if (Number.isFinite(credit) && !Number.isFinite(debit)) {
        if (amountsRoughlyEqual(prevBal + credit, currBal)) ascendingHits += 1;
        if (amountsRoughlyEqual(prevBal - credit, currBal)) descendingHits += 1;
      }
    }

    if (ascendingHits > descendingHits) return 'ascending';
    if (descendingHits > ascendingHits) return 'descending';
    return null;
  }

  function expectedBalanceForFlow(prevBalance, debit, credit, flowDirection) {
    if (!Number.isFinite(prevBalance)) return null;
    const debitValue = Number.isFinite(debit) ? debit : 0;
    const creditValue = Number.isFinite(credit) ? credit : 0;
    if (flowDirection === 'descending') {
      return prevBalance + debitValue - creditValue;
    }
    return prevBalance - debitValue + creditValue;
  }

  function getFlowDirectionForPage(page) {
    return inferBalanceFlowDirection(getOrderedRows(page)) || 'ascending';
  }

  function recomputeLocalDisbalanceState(page) {
    const rows = Array.isArray(state.parsedByPage[page]) ? state.parsedByPage[page] : [];
    if (!rows.length) return new Set();
    const flowDirection = inferBalanceFlowDirection(rows) || 'ascending';
    const mismatches = new Set();
    for (let idx = 0; idx < rows.length; idx += 1) {
      const current = rows[idx] || {};
      current.is_disbalanced = false;
      current.disbalance_expected_balance = null;
      current.disbalance_delta = null;
      if (idx === 0) continue;
      const prev = rows[idx - 1] || {};
      const rowId = String(current?.row_id || '').trim() || String(idx + 1).padStart(3, '0');
      const prevBal = parseAmountForFormatting(prev.balance);
      const currBal = parseAmountForFormatting(current.balance);
      const debit = parseAmountForFormatting(current.debit);
      const credit = parseAmountForFormatting(current.credit);
      const hasFlow = Number.isFinite(debit) || Number.isFinite(credit);
      if (!Number.isFinite(prevBal) || !Number.isFinite(currBal) || !hasFlow) continue;
      const expected = expectedBalanceForFlow(prevBal, debit, credit, flowDirection);
      if (!Number.isFinite(expected)) continue;
      const delta = currBal - expected;
      if (Math.abs(delta) > 0.01) {
        current.is_disbalanced = true;
        current.disbalance_expected_balance = Number(expected.toFixed(2));
        current.disbalance_delta = Number(delta.toFixed(2));
        mismatches.add(rowId);
      }
    }
    return mismatches;
  }

  function computeAmountForBalanceTarget(prevBalance, currentBalance, flowDirection, targetField) {
    if (!Number.isFinite(prevBalance) || !Number.isFinite(currentBalance)) return null;

    let amount = null;
    if (targetField === 'debit') {
      amount = flowDirection === 'descending'
        ? currentBalance - prevBalance
        : prevBalance - currentBalance;
    } else if (targetField === 'credit') {
      amount = flowDirection === 'descending'
        ? prevBalance - currentBalance
        : currentBalance - prevBalance;
    }

    if (!Number.isFinite(amount)) return null;
    if (Math.abs(amount) <= 0.005) return 0;
    if (amount < 0) return null;
    return amount;
  }

  function setRowAmountColumns(row, debit, credit) {
    if (!row) return;
    row.debit = Number.isFinite(debit) && Math.abs(debit) > 0.005 ? formatAmountCellValue(debit) : '';
    row.credit = Number.isFinite(credit) && Math.abs(credit) > 0.005 ? formatAmountCellValue(credit) : '';
  }

  function recalculateBalanceFromAmounts(page, rowId) {
    const context = findOrderedRowContext(page, rowId);
    if (!context || !context.row) return false;

    const { row, prevRow } = context;
    const prevBalance = parseAmountForFormatting(prevRow?.balance);
    if (!Number.isFinite(prevBalance)) return false;

    const debitRaw = normalizeEditableCellValue(row.debit).trim();
    const creditRaw = normalizeEditableCellValue(row.credit).trim();
    const debit = parseAmountForFormatting(row.debit);
    const credit = parseAmountForFormatting(row.credit);
    if (!hasUsableAmountValue(debitRaw, debit) || !hasUsableAmountValue(creditRaw, credit)) return false;

    const nextBalance = expectedBalanceForFlow(prevBalance, debit, credit, getFlowDirectionForPage(page));
    if (!Number.isFinite(nextBalance)) return false;
    row.balance = formatAmountCellValue(nextBalance);
    return true;
  }

  function recalculateAmountsFromBalance(page, rowId, preferredField = null) {
    const context = findOrderedRowContext(page, rowId);
    if (!context || !context.row) return false;

    const { row, prevRow } = context;
    const prevBalance = parseAmountForFormatting(prevRow?.balance);
    const balanceRaw = normalizeEditableCellValue(row.balance).trim();
    const balance = parseAmountForFormatting(row.balance);
    if (!Number.isFinite(prevBalance) || !hasUsableAmountValue(balanceRaw, balance) || !Number.isFinite(balance)) {
      return false;
    }

    const flowDirection = getFlowDirectionForPage(page);
    if (preferredField === 'debit' || preferredField === 'credit') {
      const nextAmount = computeAmountForBalanceTarget(prevBalance, balance, flowDirection, preferredField);
      if (nextAmount === null) return false;
      if (preferredField === 'debit') {
        setRowAmountColumns(row, nextAmount, null);
      } else {
        setRowAmountColumns(row, null, nextAmount);
      }
      return true;
    }

    const nextDebit = computeAmountForBalanceTarget(prevBalance, balance, flowDirection, 'debit');
    const nextCredit = computeAmountForBalanceTarget(prevBalance, balance, flowDirection, 'credit');
    if (nextDebit === null && nextCredit === null && !amountsRoughlyEqual(prevBalance, balance)) return false;
    setRowAmountColumns(row, nextDebit, nextCredit);
    return true;
  }

  function syncRowFromRenderedInputs(rowEl, row) {
    if (!(rowEl instanceof HTMLTableRowElement) || !row) return;
    for (const field of EDITABLE_ROW_FIELDS) {
      const input = rowEl.querySelector(`.table-row-input-${field}`);
      if (!(input instanceof HTMLInputElement)) continue;
      row[field] = normalizeCommittedFieldValue(field, input.value);
    }
  }

  function finalizeParsedRowMutation(page, rowId, rowEl, row, before) {
    syncRenderedRowInputs(rowEl, row);
    const after = buildComparableParsedRow(row, rowId);

    if (!areComparableRowsEqual(after, before)) {
      recomputeLocalDisbalanceState(page);
      queuePageRowsSave(page);
    }

    applyBalanceMismatchStyles(page);
    applyDescriptionFlagStyles(page);
    applyParsedRowStateStyles(page);
  }

  function runParsedRowContextAction(page, rowId, field, rowEl) {
    if (!AMOUNT_ROW_FIELDS.has(field)) return;

    const context = findOrderedRowContext(page, rowId);
    if (!context || !context.row) return;

    const before = buildComparableParsedRow(context.row, rowId);
    syncRowFromRenderedInputs(rowEl, context.row);

    let applied = false;
    if (field === 'balance') {
      applied = recalculateBalanceFromAmounts(page, rowId);
      if (!applied) {
        alert('Unable to auto calculate balance from the current debit/credit values.');
      }
    } else {
      applied = recalculateAmountsFromBalance(page, rowId, field);
      if (!applied) {
        alert(`Unable to auto calculate ${field} from the current balance.`);
      }
    }

    finalizeParsedRowMutation(page, rowId, rowEl, context.row, before);
  }

  function reconcileRowAmountsWithRunningBalance(page, rowId, editedField) {
    if (!AMOUNT_ROW_FIELDS.has(editedField)) return;
    const context = findOrderedRowContext(page, rowId);
    if (!context || !context.row) return;

    const { row, prevRow } = context;
    const prevBalance = parseAmountForFormatting(prevRow?.balance);
    if (!Number.isFinite(prevBalance)) return;

    const debitRaw = normalizeEditableCellValue(row.debit).trim();
    const creditRaw = normalizeEditableCellValue(row.credit).trim();
    const balanceRaw = normalizeEditableCellValue(row.balance).trim();
    const debit = parseAmountForFormatting(row.debit);
    const credit = parseAmountForFormatting(row.credit);
    const balance = parseAmountForFormatting(row.balance);

    if (editedField === 'debit' || editedField === 'credit') {
      if (!hasUsableAmountValue(debitRaw, debit) || !hasUsableAmountValue(creditRaw, credit)) return;
      const nextBalance = expectedBalanceForFlow(prevBalance, debit, credit, getFlowDirectionForPage(page));
      if (Number.isFinite(nextBalance)) {
        row.balance = formatAmountCellValue(nextBalance);
      }
      return;
    }

    if (!hasUsableAmountValue(balanceRaw, balance) || !Number.isFinite(balance)) return;
    recalculateAmountsFromBalance(page, rowId);
  }

  function syncRenderedRowInputs(rowEl, row) {
    if (!(rowEl instanceof HTMLTableRowElement) || !row) return;
    for (const field of EDITABLE_ROW_FIELDS) {
      const input = rowEl.querySelector(`.table-row-input-${field}`);
      if (!(input instanceof HTMLInputElement)) continue;
      input.value = getRenderedFieldValue(field, row[field]);
    }
  }

  function commitParsedRowFieldEdit(page, rowId, field, input, rowEl) {
    const context = findOrderedRowContext(page, rowId);
    if (!context || !context.row) return;

    const before = buildComparableParsedRow(context.row, rowId);
    const previousFieldValue = normalizeCommittedFieldValue(field, context.row[field]);
    const nextFieldValue = normalizeCommittedFieldValue(field, input.value);
    context.row[field] = nextFieldValue;

    if (AMOUNT_ROW_FIELDS.has(field) && previousFieldValue !== nextFieldValue) {
      reconcileRowAmountsWithRunningBalance(page, rowId, field);
    }

    finalizeParsedRowMutation(page, rowId, rowEl, context.row, before);
  }

  function buildComparableParsedRow(row, rowId, index = 0) {
    return {
      row_id: String(rowId || row?.row_id || '').trim() || String(index + 1).padStart(3, '0'),
      date: formatParsedRowDate(normalizeEditableCellValue(row?.date)),
      description: normalizeEditableCellValue(row?.description).trim(),
      debit: formatAmountCellValue(row?.debit),
      credit: formatAmountCellValue(row?.credit),
      balance: formatAmountCellValue(row?.balance),
    };
  }

  function snapshotParsedRows(rows) {
    return (Array.isArray(rows) ? rows : []).map((row, idx) => buildComparableParsedRow(row, row?.row_id, idx));
  }

  function ensureBaselineRows(page, rows) {
    const pageName = String(page || '').trim();
    if (!pageName) return;
    if (Object.prototype.hasOwnProperty.call(state.baselineParsedByPage, pageName)) return;
    state.baselineParsedByPage[pageName] = snapshotParsedRows(rows);
  }

  function areComparableRowsEqual(currentRow, baselineRow) {
    if (!currentRow || !baselineRow) return false;
    return currentRow.date === baselineRow.date
      && currentRow.description === baselineRow.description
      && currentRow.debit === baselineRow.debit
      && currentRow.credit === baselineRow.credit
      && currentRow.balance === baselineRow.balance;
  }

  function getStoredDisbalanceRowIds(rows) {
    const mismatches = new Set();
    for (let idx = 0; idx < (Array.isArray(rows) ? rows.length : 0); idx += 1) {
      const row = rows[idx] || {};
      if (!row?.is_disbalanced) continue;
      const rowId = String(row?.row_id || '').trim() || String(idx + 1).padStart(3, '0');
      mismatches.add(rowId);
    }
    return mismatches;
  }

  function applyBalanceMismatchStyles(page) {
    if (!els.rowsBody) return;
    const rows = getOrderedRows(page);
    const mismatches = getStoredDisbalanceRowIds(rows);

    for (const tr of els.rowsBody.querySelectorAll('tr')) {
      const rowId = String(tr.dataset.rowId || '').trim();
      const isMismatch = mismatches.has(rowId);
      tr.classList.toggle('balance-mismatch-row', isMismatch);
      const input = tr.querySelector('.table-row-input-balance');
      if (input) {
        input.classList.toggle('balance-mismatch', isMismatch);
      }
    }
    renderDisbalanceTable();
    updatePreviewSupplementalTabLabels();
  }

  function applyDescriptionFlagStyles(page) {
    if (!els.rowsBody) return;
    const rows = getOrderedRows(page);
    const rowById = new Map(
      rows.map((row, idx) => [String(row?.row_id || '').trim() || String(idx + 1).padStart(3, '0'), row])
    );

    for (const tr of els.rowsBody.querySelectorAll('tr')) {
      const rowId = String(tr.dataset.rowId || '').trim();
      const row = rowById.get(rowId);
      const input = tr.querySelector('.table-row-input-description');
      if (!(input instanceof HTMLInputElement)) continue;
      const match = getDescriptionFlagMatch(normalizeEditableCellValue(row?.description), page);
      const isFlagged = Boolean(row?.is_flagged) || Boolean(match);
      input.classList.toggle('bank-code-flagged', isFlagged);
      tr.classList.toggle('bank-code-flag-row', isFlagged);
      input.title = match ? `Flagged transaction code ${match.code} (${match.bank})` : '';
    }
    renderFlaggedTransactionsTable();
  }

  async function refreshBankCodeFlagUi() {
    updatePreviewSupplementalTabLabels();

    const currentPage = String(state.currentPage || '').trim();
    if (state.jobId && currentPage) {
      try {
        await loadCurrentPageData();
      } catch {
        renderFlaggedTransactionsTable();
      }
    } else {
      renderFlaggedTransactionsTable();
    }

    if (state.previewPanelTab === 'flagged') {
      try {
        await ensureAllPagesParsedLoaded();
      } catch {
        renderFlaggedTransactionsTable();
      }
    }
  }

  function getParsedRowVisualState(page, row, rowId, mismatchIds, baselineRowsById) {
    if (mismatchIds.has(rowId)) return 'error';
    if (Boolean(row?.is_flagged) || getDescriptionFlagMatch(normalizeEditableCellValue(row?.description), page)) return 'error';

    const baselineRow = baselineRowsById.get(rowId);
    if (!baselineRow) return 'added';

    const comparableRow = buildComparableParsedRow(row, rowId);
    return areComparableRowsEqual(comparableRow, baselineRow) ? '' : 'modified';
  }

  function applyParsedRowStateStyles(page) {
    if (!els.rowsBody) return;
    const rows = getOrderedRows(page);
    ensureBaselineRows(page, rows);
    const mismatchIds = getStoredDisbalanceRowIds(rows);
    const baselineRowsById = new Map(
      (Array.isArray(state.baselineParsedByPage[page]) ? state.baselineParsedByPage[page] : [])
        .map((row, idx) => [String(row?.row_id || '').trim() || String(idx + 1).padStart(3, '0'), row])
    );
    const rowById = new Map(
      rows.map((row, idx) => [String(row?.row_id || '').trim() || String(idx + 1).padStart(3, '0'), row])
    );

    for (const tr of els.rowsBody.querySelectorAll('tr')) {
      const rowId = String(tr.dataset.rowId || '').trim();
      const row = rowById.get(rowId);
      const rowState = row ? getParsedRowVisualState(page, row, rowId, mismatchIds, baselineRowsById) : '';
      tr.classList.remove('row-state-error', 'row-state-modified', 'row-state-added');
      if (rowState) {
        tr.classList.add(`row-state-${rowState}`);
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

  function getParsedRowsScrollContainer() {
    if (els.parsedTableWrap instanceof HTMLElement) return els.parsedTableWrap;
    return els.rowsBody?.closest('.table-wrap') || null;
  }

  function snapshotParsedScrollPosition() {
    const container = getParsedRowsScrollContainer();
    state.pendingParsedScrollTop = container instanceof HTMLElement ? container.scrollTop : null;
    state.pendingWindowScrollY = typeof window.scrollY === 'number' ? window.scrollY : null;
  }

  function restoreParsedScrollPosition() {
    const container = getParsedRowsScrollContainer();
    if (container instanceof HTMLElement && Number.isFinite(state.pendingParsedScrollTop)) {
      container.scrollTop = Math.max(0, Number(state.pendingParsedScrollTop));
    }
    if (Number.isFinite(state.pendingWindowScrollY)) {
      window.scrollTo({ top: Math.max(0, Number(state.pendingWindowScrollY)), behavior: 'auto' });
    }
  }

  function scrollParsedRowIntoView(targetTr, { behavior = 'auto', block = 'center' } = {}) {
    if (!(targetTr instanceof HTMLTableRowElement)) return;
    const container = getParsedRowsScrollContainer();
    if (!(container instanceof HTMLElement)) {
      targetTr.scrollIntoView({ block, behavior });
      return;
    }

    const containerRect = container.getBoundingClientRect();
    const targetRect = targetTr.getBoundingClientRect();
    let nextTop = container.scrollTop;

    if (block === 'center') {
      nextTop += (targetRect.top - containerRect.top) - ((container.clientHeight - targetRect.height) / 2);
    } else if (block === 'start') {
      nextTop += targetRect.top - containerRect.top;
    } else if (block === 'end') {
      nextTop += targetRect.bottom - containerRect.bottom;
    } else if (block === 'nearest') {
      if (targetRect.top < containerRect.top) nextTop += targetRect.top - containerRect.top;
      else if (targetRect.bottom > containerRect.bottom) nextTop += targetRect.bottom - containerRect.bottom;
      else return;
    }

    container.scrollTo({ top: Math.max(0, nextTop), behavior });
  }

  function buildPendingRowFocus(page, rowId, overrides = {}) {
    const targetPage = String(page || '').trim();
    const targetRowId = String(rowId || '').trim();
    const normalizedIndex = Number.isFinite(Number(overrides.rowIndex))
      ? Math.max(0, Number.parseInt(overrides.rowIndex, 10))
      : null;
    if (!targetPage || (!targetRowId && normalizedIndex === null)) return null;
    return {
      page: targetPage,
      rowId: targetRowId,
      rowIndex: normalizedIndex,
      focusInput: false,
      behavior: 'auto',
      block: 'center',
      ...overrides,
    };
  }

  function findRenderedRowElement({ rowId = '', rowIndex = null } = {}) {
    if (!els.rowsBody) return null;
    const rows = Array.from(els.rowsBody.querySelectorAll('tr'));
    const targetRowId = String(rowId || '').trim();
    if (targetRowId) {
      const matched = rows.find((rowEl) => String(rowEl.dataset.rowId || '').trim() === targetRowId);
      if (matched instanceof HTMLTableRowElement) return matched;
    }
    if (Number.isInteger(rowIndex) && rowIndex >= 0 && rowIndex < rows.length) {
      const indexed = rows[rowIndex];
      if (indexed instanceof HTMLTableRowElement) return indexed;
    }
    return null;
  }

  function focusParsedRow(
    rowId,
    { behavior = 'auto', block = 'center', focusInput = false, rowIndex = null } = {},
  ) {
    const targetRowId = String(rowId || '').trim();
    const targetRowIndex = Number.isFinite(Number(rowIndex))
      ? Math.max(0, Number.parseInt(rowIndex, 10))
      : null;
    if ((!targetRowId && targetRowIndex === null) || !els.rowsBody) return false;

    const targetTr = findRenderedRowElement({ rowId: targetRowId, rowIndex: targetRowIndex });
    if (!(targetTr instanceof HTMLTableRowElement)) return false;
    selectRow(String(targetTr.dataset.rowId || '').trim());

    scrollParsedRowIntoView(targetTr, { block, behavior });
    if (!focusInput) return true;

    const preferredInput = targetTr.querySelector('.table-row-input-description')
      || targetTr.querySelector('.table-row-input-date')
      || targetTr.querySelector('.table-row-input-debit')
      || targetTr.querySelector('.table-row-input-credit')
      || targetTr.querySelector('.table-row-input-balance');
    if (!(preferredInput instanceof HTMLInputElement)) return true;

    preferredInput.focus({ preventScroll: true });
    preferredInput.select();
    return true;
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
        row_type: String(row?.row_type || 'transaction').trim() || 'transaction',
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
      const activeElement = document.activeElement;
      const activeInput = activeElement instanceof HTMLInputElement ? activeElement : null;
      const activeRow = activeInput?.closest('tr[data-row-id]');
      const activeRowId = String(activeRow?.dataset.rowId || '').trim();
      const activeRowIndex = activeRow instanceof HTMLTableRowElement
        ? Array.from(els.rowsBody?.querySelectorAll('tr') || []).indexOf(activeRow)
        : null;
      const shouldPreserveFocusedInput = Boolean(
        activeInput
        && activeRowId
        && pageName === state.currentPage
      );
      const preservedFocus = pageName === state.currentPage
        ? (
          state.pendingRowFocus
          || buildPendingRowFocus(
            pageName,
            activeRowId || state.selectedRowId,
            {
              rowIndex: activeRowIndex,
              focusInput: shouldPreserveFocusedInput,
              behavior: 'auto',
              block: 'nearest'
            },
          )
        )
        : null;
      if (preservedFocus) state.pendingRowFocus = preservedFocus;
      applyPageUpdatePayload(pageName, payload);
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
    if (!rows.length) return;

    const disbalancedRow = isDisbalancedRow(page, rowId);
    const nextRow = disbalancedRow
      ? buildMissingTransactionRow(page, rowId, rows)
      : {
        row_id: buildNextRowId(rows),
        date: '',
        description: '',
        debit: '',
        credit: '',
        balance: '',
      };
    if (disbalancedRow && !nextRow) {
      alert('Unable to auto-fill missing transaction for this disbalanced row.');
      return;
    }

    const insertIndex = resolveInsertIndex(page, rowId, { beforeCurrent: disbalancedRow });
    if (insertIndex < 0) return;
    rows.splice(insertIndex, 0, nextRow);
    state.parsedByPage[page] = rows;
    state.pendingRowFocus = buildPendingRowFocus(page, nextRow.row_id, {
      rowIndex: insertIndex,
      focusInput: true,
      behavior: 'auto',
      block: 'center',
    });
    snapshotParsedScrollPosition();
    renderRows();
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

  async function reverseAllPagesRows() {
    if (state.reverseRowsBusy) return;
    if (!state.jobId) return;
    state.reverseRowsBusy = true;
    updateReverseRowsActionState();
    try {
      state.reversePageOrder = !state.reversePageOrder;
      state.pages = state.pages.slice().reverse();
      state.currentPage = state.pages[0] || null;
      state.selectedRowId = null;
      await api(`/jobs/${state.jobId}/reverse-order`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ is_reversed: state.reversePageOrder })
      });
      renderPages();
      await loadCurrentPageData();
    } finally {
      state.reverseRowsBusy = false;
      updateReverseRowsActionState();
    }
  }

  function drawSelectedBound() {
    if (!els.overlay || !els.previewImage || !els.previewStage) return;
    const canvas = els.overlay;
    const img = els.previewImage;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    const rect = img.getBoundingClientRect();

    canvas.width = Math.max(1, Math.round(rect.width));
    canvas.height = Math.max(1, Math.round(rect.height));
    canvas.style.width = `${Math.max(1, Math.round(rect.width))}px`;
    canvas.style.height = `${Math.max(1, Math.round(rect.height))}px`;
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

  function syncPreviewZoomLayout() {
    if (!els.previewWrap || !els.previewStage || !els.previewImage) return;
    const zoom = Number.isFinite(state.previewZoom) ? state.previewZoom : 1;
    const safeZoom = Math.min(3, Math.max(1, zoom));
    state.previewZoom = safeZoom;
    els.previewStage.style.width = `${safeZoom * 100}%`;

    const naturalWidth = Number(els.previewImage.naturalWidth || 0);
    const naturalHeight = Number(els.previewImage.naturalHeight || 0);
    if (naturalWidth > 0 && naturalHeight > 0) {
      const availableWidth = Math.max(1, els.previewWrap.clientWidth || els.previewWrap.getBoundingClientRect().width || 1);
      const baseHeight = Math.round((naturalHeight / naturalWidth) * availableWidth);
      els.previewWrap.style.height = `${Math.max(220, baseHeight)}px`;
    } else {
      els.previewWrap.style.removeProperty('height');
    }
    drawSelectedBound();
  }

  function resetPreviewZoom() {
    state.previewZoom = 1;
    syncPreviewZoomLayout();
    if (els.previewWrap) {
      els.previewWrap.scrollTop = 0;
      els.previewWrap.scrollLeft = 0;
    }
  }

  function handlePreviewWheelZoom(event) {
    if (!els.previewWrap || !els.previewImage) return;
    if (state.previewPanelTab !== 'preview') return;
    if (!els.previewImage.getAttribute('src')) return;
    if (!els.previewImage.naturalWidth || !els.previewImage.naturalHeight) return;

    event.preventDefault();
    const rect = els.previewWrap.getBoundingClientRect();
    const offsetX = event.clientX - rect.left + els.previewWrap.scrollLeft;
    const offsetY = event.clientY - rect.top + els.previewWrap.scrollTop;
    const previousZoom = state.previewZoom;
    const nextZoom = Math.min(3, Math.max(1, previousZoom * (event.deltaY < 0 ? 1.1 : 0.9)));
    if (Math.abs(nextZoom - previousZoom) < 0.001) return;

    state.previewZoom = nextZoom;
    syncPreviewZoomLayout();

    const ratio = nextZoom / previousZoom;
    els.previewWrap.scrollLeft = Math.max(0, offsetX * ratio - (event.clientX - rect.left));
    els.previewWrap.scrollTop = Math.max(0, offsetY * ratio - (event.clientY - rect.top));
  }

  function startPreviewPan(event) {
    if (!els.previewWrap || !els.previewImage) return;
    if (state.previewPanelTab !== 'preview') return;
    if (state.previewZoom <= 1.001) return;
    if (event.button !== 0) return;
    if (event.target instanceof HTMLInputElement || event.target instanceof HTMLButtonElement) return;

    state.previewPanActive = true;
    state.previewPanStartX = event.clientX;
    state.previewPanStartY = event.clientY;
    state.previewPanScrollLeft = els.previewWrap.scrollLeft;
    state.previewPanScrollTop = els.previewWrap.scrollTop;
    els.previewWrap.classList.add('is-panning');
    event.preventDefault();
  }

  function movePreviewPan(event) {
    if (!els.previewWrap || !state.previewPanActive) return;
    const deltaX = event.clientX - state.previewPanStartX;
    const deltaY = event.clientY - state.previewPanStartY;
    els.previewWrap.scrollLeft = state.previewPanScrollLeft - deltaX;
    els.previewWrap.scrollTop = state.previewPanScrollTop - deltaY;
  }

  function stopPreviewPan() {
    state.previewPanActive = false;
    if (els.previewWrap) {
      els.previewWrap.classList.remove('is-panning');
    }
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
    const rows = getOrderedRows(page);
    ensureBaselineRows(page, rows);
    const mismatchIds = getStoredDisbalanceRowIds(rows);
    const baselineRowsById = new Map(
      (Array.isArray(state.baselineParsedByPage[page]) ? state.baselineParsedByPage[page] : [])
        .map((row, idx) => [String(row?.row_id || '').trim() || String(idx + 1).padStart(3, '0'), row])
    );
    recomputeTotalParsedRows();
    updateExportAvailability();
    if (els.rowCount) els.rowCount.textContent = `${rows.length} rows`;
    for (const [idx, row] of rows.entries()) {
      const tr = document.createElement('tr');
      tr.className = 'clickable';
      const rowId = String(row?.row_id || '').trim() || String(idx + 1).padStart(3, '0');
      row.row_id = rowId;
      tr.dataset.rowId = rowId;
      const rowState = getParsedRowVisualState(page, row, rowId, mismatchIds, baselineRowsById);
      if (rowState) {
        tr.classList.add(`row-state-${rowState}`);
      }

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
        if (AMOUNT_ROW_FIELDS.has(field)) {
          input.addEventListener('contextmenu', (evt) => {
            evt.preventDefault();
            evt.stopPropagation();
            selectRow(rowId);
            runParsedRowContextAction(page, rowId, field, tr);
          });
        }
        input.addEventListener('blur', () => {
          commitParsedRowFieldEdit(page, rowId, field, input, tr);
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
    applyDescriptionFlagStyles(page);
    applyParsedRowStateStyles(page);
    drawSelectedBound();
    restoreParsedScrollPosition();
    if (
      state.pendingRowFocus
      && state.pendingRowFocus.page === String(page || '').trim()
      && state.pendingRowFocus.rowId
    ) {
      const pendingFocus = { ...state.pendingRowFocus };
      window.requestAnimationFrame(() => {
        const focused = focusParsedRow(pendingFocus.rowId, {
          block: pendingFocus.block || 'center',
          behavior: pendingFocus.behavior || 'auto',
          focusInput: Boolean(pendingFocus.focusInput),
        });
        if (focused !== false) {
          state.pendingRowFocus = null;
          state.pendingParsedScrollTop = null;
          state.pendingWindowScrollY = null;
        }
      });
    } else {
      state.pendingParsedScrollTop = null;
      state.pendingWindowScrollY = null;
    }
  }

  function renderPages() {
    if (state.pages.length && (!state.currentPage || !state.pages.includes(state.currentPage))) {
      state.currentPage = state.pages[0];
    }
    updatePageNav();
  }

  function loadPreview(options = {}) {
    const preserveSelectedRow = Boolean(options && options.preserveSelectedRow);
    const forceReload = options && Object.prototype.hasOwnProperty.call(options, 'forceReload')
      ? Boolean(options.forceReload)
      : false;
    if (!state.jobId || !state.currentPage || !els.previewImage) {
      if (els.previewImage) els.previewImage.removeAttribute('src');
      updatePreviewEmptyState();
      return;
    }
    const url = forceReload
      ? `/jobs/${state.jobId}/preview/${state.currentPage}?v=${Date.now()}`
      : `/jobs/${state.jobId}/preview/${state.currentPage}`;
    const currentSrc = String(els.previewImage.getAttribute('src') || '').trim();
    if (forceReload || currentSrc !== url) {
      state.previewZoom = 1;
      els.previewImage.src = url;
    }
    if (!preserveSelectedRow) {
      state.selectedRowId = null;
    }
    drawSelectedBound();
    updatePreviewEmptyState();
  }

  function updatePageNav() {
    const total = state.pages.length;
    const idx = total ? state.pages.indexOf(state.currentPage) : -1;
    if (els.pageCount) els.pageCount.textContent = String(total);
    if (els.pageNumberInput) els.pageNumberInput.value = String(total ? getCurrentPageNumber() : 0);
    if (els.pageFirstBtn) els.pageFirstBtn.disabled = !(idx > 0);
    if (els.pagePrevBtn) els.pagePrevBtn.disabled = !(idx > 0);
    if (els.pageNextBtn) els.pageNextBtn.disabled = !(idx >= 0 && idx < total - 1);
    if (els.pageLastBtn) els.pageLastBtn.disabled = !(idx >= 0 && idx < total - 1);
    updatePageNotesActionState();
    updatePageAiFixActionState();
  }

  function getCurrentPageNotes() {
    const page = String(state.currentPage || '').trim();
    if (!page) return '';
    const value = state.pageNotesByPage[page];
    return typeof value === 'string' ? value : '';
  }

  function updatePageNotesActionState() {
    if (!els.pageNotesBtn) return;
    const hasJobAndPage = Boolean(state.jobId && state.currentPage);
    els.pageNotesBtn.disabled = !hasJobAndPage || state.pageNotesSaving || state.pageNotesLoading;
    els.pageNotesBtn.classList.toggle('has-notes', Boolean(getCurrentPageNotes().trim()));
    els.pageNotesBtn.title = hasJobAndPage ? 'Page notes' : 'Page notes';
  }

  function getCurrentPageAiFixDraft() {
    const page = String(state.currentPage || '').trim();
    if (!page) return null;
    const draft = state.pageAiFixDraftByPage[page];
    return draft && typeof draft === 'object' ? draft : null;
  }

  function updatePageAiFixActionState() {
    if (!els.pageAiFixBtn) return;
    const hasJobAndPage = Boolean(state.jobId && state.currentPage);
    const visible = Boolean(state.pageAiFixEnabled);
    els.pageAiFixBtn.classList.toggle('hidden', !visible);
    els.pageAiFixBtn.disabled = !visible || !hasJobAndPage || state.pageAiFixLoading || state.pageAiFixApplying;
    els.pageAiFixBtn.classList.toggle('is-busy', Boolean(state.pageAiFixLoading));
    els.pageAiFixBtn.title = state.pageAiFixLoading ? 'AI is analyzing this page...' : 'AI fix page errors';
  }

  function renderPageAiFixDraft() {
    const draft = getCurrentPageAiFixDraft();
    const summary = draft?.proposal?.summary || {};
    const rows = Array.isArray(draft?.proposal?.rows) ? draft.proposal.rows : [];
    if (els.pageAiFixSubtitle) {
      els.pageAiFixSubtitle.textContent = state.currentPage
        ? `Review the proposed parsed-row fixes for ${String(state.currentPage).replace(/^page_/, 'Page ')}.`
        : 'Review the proposed parsed-row fixes for this page.';
    }
    if (els.pageAiFixJson) {
      els.pageAiFixJson.textContent = JSON.stringify(rows, null, 2);
    }
    if (els.pageAiFixStatus) {
      const issues = Array.isArray(summary.issues_found) && summary.issues_found.length
        ? ` Issues: ${summary.issues_found.join(', ')}.`
        : '';
      const rationale = String(summary.rationale || '').trim();
      const changed = typeof summary.changed === 'boolean' ? summary.changed : null;
      const message = rationale || changed !== null
        ? `${changed === false ? 'AI could not confidently improve this page.' : 'AI proposed repairs for this page.'}${issues}${rationale ? ` ${rationale}` : ''}`.trim()
        : '';
      els.pageAiFixStatus.textContent = message;
      els.pageAiFixStatus.classList.toggle('hidden', !message);
      els.pageAiFixStatus.classList.remove('is-error');
    }
    if (els.pageAiFixApplyBtn) {
      els.pageAiFixApplyBtn.disabled = !draft || state.pageAiFixApplying;
      els.pageAiFixApplyBtn.textContent = state.pageAiFixApplying ? 'Applying...' : 'Apply fixes';
    }
  }

  function setPageAiFixModalOpen(open) {
    state.pageAiFixModalOpen = Boolean(open);
    if (els.pageAiFixModal) {
      els.pageAiFixModal.classList.toggle('hidden', !state.pageAiFixModalOpen);
    }
    if (state.pageAiFixModalOpen) {
      renderPageAiFixDraft();
    }
  }

  function closePageAiFixModal() {
    if (state.pageAiFixApplying) return;
    setPageAiFixModalOpen(false);
  }

  async function requestCurrentPageAiFix() {
    const page = String(state.currentPage || '').trim();
    const jobId = String(state.jobId || '').trim();
    if (!jobId || !page || !state.pageAiFixEnabled || state.pageAiFixLoading || state.pageAiFixApplying) return;

    state.pageAiFixLoading = true;
    updatePageAiFixActionState();
    if (els.pageAiFixStatus) {
      els.pageAiFixStatus.textContent = 'AI is reviewing the page image, raw OCR, and parsed rows...';
      els.pageAiFixStatus.classList.remove('hidden', 'is-error');
    }
    if (els.pageAiFixJson) {
      els.pageAiFixJson.textContent = '';
    }
    setPageAiFixModalOpen(true);

    let didFail = false;
    try {
      const payload = await api(`/jobs/${jobId}/pages/${page}/ai-fix`, { method: 'POST' });
      state.pageAiFixDraftByPage[page] = payload;
      renderPageAiFixDraft();
    } catch (err) {
      didFail = true;
      if (els.pageAiFixStatus) {
        els.pageAiFixStatus.textContent = `Unable to generate AI fix: ${normalizeApiErrorMessage(err?.message)}`;
        els.pageAiFixStatus.classList.remove('hidden');
        els.pageAiFixStatus.classList.add('is-error');
      }
      if (els.pageAiFixJson) {
        els.pageAiFixJson.textContent = '';
      }
      showToast(`Unable to generate AI fix: ${normalizeApiErrorMessage(err?.message)}`, 'error', 4200);
    } finally {
      state.pageAiFixLoading = false;
      updatePageAiFixActionState();
      if (!didFail) renderPageAiFixDraft();
    }
  }

  function applyPageUpdatePayload(pageName, payload) {
    if (!payload || !Array.isArray(payload.rows)) return;
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
        existingRows[idx].rownumber = src.rownumber ?? existingRows[idx].rownumber ?? null;
        existingRows[idx].row_number = String(src.row_number || existingRows[idx].row_number || '').trim();
        existingRows[idx].date = normalizeEditableCellValue(src.date).trim();
        existingRows[idx].description = normalizeEditableCellValue(src.description).trim();
        existingRows[idx].debit = formatAmountCellValue(src.debit);
        existingRows[idx].credit = formatAmountCellValue(src.credit);
        existingRows[idx].balance = formatAmountCellValue(src.balance);
        existingRows[idx].row_type = String(src.row_type || existingRows[idx].row_type || 'transaction').trim() || 'transaction';
        existingRows[idx].is_flagged = Boolean(src.is_flagged);
      }
    } else {
      state.parsedByPage[pageName] = payload.rows;
    }

    if (
      state.selectedRowId
      && !payload.rows.some((row) => String(row?.row_id || '') === String(state.selectedRowId))
    ) {
      state.selectedRowId = null;
    }

    recomputeTotalParsedRows();
    updateExportAvailability();
    if (payload.summary && typeof payload.summary === 'object') {
      renderSummary(payload.summary);
    }
    if (pageName === state.currentPage) {
      snapshotParsedScrollPosition();
      renderRows();
    }
    renderDisbalanceTable();
    renderFlaggedTransactionsTable();
  }

  async function applyCurrentPageAiFix() {
    const page = String(state.currentPage || '').trim();
    const jobId = String(state.jobId || '').trim();
    const draft = getCurrentPageAiFixDraft();
    const rows = Array.isArray(draft?.proposal?.rows) ? draft.proposal.rows : null;
    if (!jobId || !page || !rows || state.pageAiFixApplying) return;

    state.pageAiFixApplying = true;
    updatePageAiFixActionState();
    renderPageAiFixDraft();

    let didFail = false;
    try {
      const payload = await api(`/jobs/${jobId}/parsed/${page}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(rows),
      });
      applyPageUpdatePayload(page, payload || {});
      setPageAiFixModalOpen(false);
      showToast('AI fixes applied.', 'success');
    } catch (err) {
      didFail = true;
      if (els.pageAiFixStatus) {
        els.pageAiFixStatus.textContent = `Unable to apply AI fix: ${normalizeApiErrorMessage(err?.message)}`;
        els.pageAiFixStatus.classList.remove('hidden');
        els.pageAiFixStatus.classList.add('is-error');
      }
      showToast(`Unable to apply AI fix: ${normalizeApiErrorMessage(err?.message)}`, 'error', 4200);
    } finally {
      state.pageAiFixApplying = false;
      updatePageAiFixActionState();
      if (!didFail) renderPageAiFixDraft();
    }
  }

  function setPageNotesModalOpen(open) {
    state.pageNotesModalOpen = Boolean(open);
    if (els.pageNotesModal) {
      els.pageNotesModal.classList.toggle('hidden', !state.pageNotesModalOpen);
    }
    if (state.pageNotesModalOpen && els.pageNotesInput) {
      window.setTimeout(() => {
        els.pageNotesInput.focus();
        els.pageNotesInput.select();
      }, 0);
    }
  }

  async function ensureCurrentPageNotesLoaded(forceReload = false) {
    const page = String(state.currentPage || '').trim();
    const jobId = String(state.jobId || '').trim();
    if (!jobId || !page) return '';
    if (!forceReload && Object.prototype.hasOwnProperty.call(state.pageNotesByPage, page)) {
      return getCurrentPageNotes();
    }
    state.pageNotesLoading = true;
    updatePageNotesActionState();
    try {
      const payload = await api(`/jobs/${jobId}/pages/${page}/notes`);
      const notes = typeof payload?.notes === 'string' ? payload.notes : '';
      state.pageNotesByPage[page] = notes;
      return notes;
    } finally {
      state.pageNotesLoading = false;
      updatePageNotesActionState();
    }
  }

  async function openPageNotesModal() {
    if (!state.jobId || !state.currentPage || state.pageNotesSaving) return;
    try {
      const notes = await ensureCurrentPageNotesLoaded();
      if (els.pageNotesSubtitle) {
        els.pageNotesSubtitle.textContent = `Add remarks for ${String(state.currentPage || '').replace(/^page_/, 'Page ')}.`;
      }
      if (els.pageNotesInput) {
        els.pageNotesInput.value = notes;
      }
      setPageNotesModalOpen(true);
    } catch (err) {
      showToast(`Unable to load page notes: ${normalizeApiErrorMessage(err?.message)}`, 'error', 4200);
    }
  }

  function closePageNotesModal() {
    if (state.pageNotesSaving) return;
    setPageNotesModalOpen(false);
  }

  async function saveCurrentPageNotes() {
    const page = String(state.currentPage || '').trim();
    const jobId = String(state.jobId || '').trim();
    if (!jobId || !page || !els.pageNotesInput || state.pageNotesSaving) return;
    state.pageNotesSaving = true;
    updatePageNotesActionState();
    if (els.pageNotesSaveBtn) {
      els.pageNotesSaveBtn.disabled = true;
      els.pageNotesSaveBtn.textContent = 'Saving...';
    }
    try {
      const payload = await api(`/jobs/${jobId}/pages/${page}/notes`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ notes: els.pageNotesInput.value || '' })
      });
      const nextNotes = typeof payload?.notes === 'string' ? payload.notes : '';
      state.pageNotesByPage[page] = nextNotes;
      updatePageNotesActionState();
      setPageNotesModalOpen(false);
      showToast('Page notes saved.', 'success');
    } catch (err) {
      showToast(`Unable to save page notes: ${normalizeApiErrorMessage(err?.message)}`, 'error', 4200);
    } finally {
      state.pageNotesSaving = false;
      updatePageNotesActionState();
      if (els.pageNotesSaveBtn) {
        els.pageNotesSaveBtn.disabled = false;
        els.pageNotesSaveBtn.textContent = 'Save';
      }
    }
  }

  function sanitizePageNumberInput() {
    if (!els.pageNumberInput) return '';
    const digits = els.pageNumberInput.value.replace(/\D+/g, '');
    els.pageNumberInput.value = digits;
    return digits;
  }

  function commitPageNumberInput() {
    if (!els.pageNumberInput) return;
    const total = getTotalPageCount();
    if (total <= 0) {
      els.pageNumberInput.value = '0';
      return;
    }
    const digits = sanitizePageNumberInput();
    const nextPage = findPageKeyByNumber(digits);
    if (!nextPage) {
      updatePageNav();
      return;
    }
    if (state.currentPage === nextPage) {
      updatePageNav();
      return;
    }
    state.currentPage = nextPage;
    state.selectedRowId = null;
    updatePageNav();
    loadCurrentPageData().catch((err) => alert(`Page load failed: ${err.message}`));
  }

  async function loadCurrentPageData() {
    if (!state.jobId || !state.currentPage) return;
    const page = state.currentPage;
    loadPreview();
    updatePageNotesActionState();
    updatePageAiFixActionState();

    const pending = [];
    if (!state.parsedByPage[page]) {
      pending.push(
        api(`/jobs/${state.jobId}/parsed/${page}`).then((payload) => {
          state.parsedByPage[page] = payload;
        })
      );
    }
    if (!state.boundsByPage[page]) {
      pending.push(
        api(`/jobs/${state.jobId}/rows/${page}/bounds`).then((payload) => {
          state.boundsByPage[page] = payload;
        })
      );
    }
    if (pending.length) {
      await Promise.all(pending);
    }
    ensureCurrentPageNotesLoaded().catch(() => {
      // Keep notes loading best-effort so row rendering is never blocked.
    });
    if (state.parsedPanelMode === 'json') {
      await ensureCurrentPageOpenaiRawLoaded();
      renderParsedDebugJson();
    } else {
      renderRows();
    }
  }

  function parsePageProfilesFromDiagnostics(payload) {
    const pages = payload && typeof payload === 'object' ? payload.pages : null;
    if (!pages || typeof pages !== 'object') return {};
    const profileByPage = {};
    for (const [page, item] of Object.entries(pages)) {
      if (!item || typeof item !== 'object') continue;
      const profile = normalizeProfileAlias(item.profile_selected || item.bank_profile || item.profile_detected || '');
      if (!profile) continue;
      profileByPage[String(page)] = profile;
    }
    return profileByPage;
  }

  async function loadResultData() {
    await flushPendingPageSaves();
    if (!state.jobId) return;
    const [cleaned, summary, diagnostics, allRows, allBounds] = await Promise.all([
      api(`/jobs/${state.jobId}/cleaned`),
      api(`/jobs/${state.jobId}/summary`),
      api(`/jobs/${state.jobId}/parse-diagnostics`).catch(() => null),
      api(`/jobs/${state.jobId}/parsed`).catch(() => ({})),
      api(`/jobs/${state.jobId}/bounds`).catch(() => ({}))
    ]);

    const pages = (cleaned.pages || [])
      .map((name) => String(name || '').replace(/\.png$/i, ''))
      .filter(Boolean)
      .sort((a, b) => pageSortValue(a) - pageSortValue(b));

    state.pages = state.reversePageOrder ? pages.slice().reverse() : pages;
    state.currentPage = state.pages[0] || null;
    state.parsedByPage = allRows && typeof allRows === 'object' ? allRows : {};
    state.baselineParsedByPage = {};
    state.boundsByPage = allBounds && typeof allBounds === 'object' ? allBounds : {};
    state.openaiRawByPage = {};
    state.pageProfileByPage = parsePageProfilesFromDiagnostics(diagnostics);
    state.parseDiagnostics = diagnostics && typeof diagnostics === 'object' ? diagnostics : null;
    state.pageNotesByPage = {};
    state.pageNotesLoading = false;
    state.pageNotesSaving = false;
    state.pageAiFixLoading = false;
    state.pageAiFixApplying = false;
    state.pageAiFixDraftByPage = {};
    state.disbalanceLoading = false;
    state.disbalanceLoadPromise = null;
    state.totalParsedRows = Number(summary?.total_transactions || 0);
    setPageNotesModalOpen(false);
    setPageAiFixModalOpen(false);

    for (const page of state.pages) {
      if (!Array.isArray(state.parsedByPage[page])) state.parsedByPage[page] = [];
      if (!Array.isArray(state.boundsByPage[page])) state.boundsByPage[page] = [];
    }

    renderSummary(state.totalParsedRows > 0 ? (summary || null) : null);
    setGoogleVisionReparseVisibility(state.parseDiagnostics);
    renderPages();
    await loadCurrentPageData();
    renderDisbalanceTable();
    renderFlaggedTransactionsTable();
    ensureAllPagesParsedLoaded().catch(() => {
      renderDisbalanceTable();
      renderFlaggedTransactionsTable();
    });
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
      const status = normalizeProcessStatus(payload?.status);
      if (status === 'completed' || status === 'needs_review') {
        stopPolling();
        await loadResultData();
      }
      if (status === 'failed') {
        stopPolling();
        alert(`Job failed: ${payload.message || 'unknown error'}`);
      }
      if (status === 'cancelled') {
        stopPolling();
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

  // Load one job into the processing workspace and start polling if it is still running.
  async function setActiveJob(jobId, switchToProcessing = false) {
    await flushPendingPageSaves();
    const id = String(jobId || '').trim();
    if (!id) return;

    const status = await api(`/jobs/${id}`);

    state.jobId = id;
    state.currentCrmLeadId = String(state.crmLeadByJobId[id] || '').trim();
    if (els.jobId) els.jobId.textContent = id;
    if (els.startBtn) els.startBtn.disabled = false;
    state.isCompleted = false;
    state.totalParsedRows = 0;
    state.reversePageOrder = Boolean(status?.is_reversed);
    state.baselineParsedByPage = {};
    state.openaiRawByPage = {};
    state.pageProfileByPage = {};
    state.pageNotesByPage = {};
    state.pageNotesLoading = false;
    state.pageNotesSaving = false;
    state.pageAiFixLoading = false;
    state.pageAiFixApplying = false;
    state.pageAiFixDraftByPage = {};
    state.disbalanceLoading = false;
    state.disbalanceLoadPromise = null;
    setExportLinks(false);
    setParsedPanelMode(state.parsedPanelMode);
    setPageNotesModalOpen(false);
    setPageAiFixModalOpen(false);
    clearRows();
    renderSummary(null);
    if (switchToProcessing) syncRoute('/processing', false, id);

    setStatus(status);
    setGoogleVisionReparseVisibility(null);
    const normalizedStatus = normalizeProcessStatus(status?.status);
    const terminal = new Set(['completed', 'needs_review', 'failed', 'cancelled']);
    if (terminal.has(normalizedStatus)) {
      stopPolling();
      if (normalizedStatus === 'completed' || normalizedStatus === 'needs_review') await loadResultData();
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
      const mode = getRequestedProcessMode();
      const payload = await uploadWithProgress(file, mode, true);
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
      const mode = getRequestedProcessMode();
      const params = new URLSearchParams();
      params.set('mode', mode);
      const payload = await api(`/jobs/${id}/start?${params.toString()}`, { method: 'POST' });
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

  async function cancelJob(jobId = state.jobId) {
    const id = String(jobId || '').trim();
    if (!id) return false;

    try {
      const payload = await api(`/jobs/${id}/cancel`, { method: 'POST' });
      const latestStatus = await api(`/jobs/${id}`).catch(() => null);
      const resolvedStatus = latestStatus?.status || payload?.status || 'cancelled';
      const resolvedStep = latestStatus?.step || normalizeProcessStepForDisplay(payload?.status) || 'cancelled';
      const existing = state.uploadedJobs.find((item) => String(item?.jobId || '').trim() === id) || null;
      const resolvedProgress = Number(
        latestStatus?.progress ?? existing?.progress ?? 0
      );

      updateUploadedJobIfExists({
        jobId: id,
        status: resolvedStatus,
        step: resolvedStep,
        progress: resolvedProgress,
        parseMode: latestStatus?.parse_mode || existing?.parseMode,
        isReversed: Boolean(latestStatus?.is_reversed)
      });

      if (state.jobId === id) {
        stopPolling();
        if (latestStatus) setStatus(latestStatus);
      }

      if (payload?.cancelled) showToast('Job cancelled.', 'success');
      else showToast(`Job is already ${formatProcessStatusLabel(resolvedStatus)}.`, 'info');
      return true;
    } catch (err) {
      alert(`Cancel failed: ${normalizeApiErrorMessage(err?.message)}`);
      return false;
    }
  }

  async function loadJobById() {
    const value = String(els.jobIdInput?.value || '').trim();
    if (!value) return;
    try {
      await setActiveJob(value, true);
    } catch (err) {
      alert(`Load job failed: ${normalizeApiErrorMessage(err?.message)}`);
    }
  }

  if (els.form) els.form.addEventListener('submit', createJob);
  if (els.startBtn) els.startBtn.addEventListener('click', () => startJob());
  if (els.exportCrm) {
    els.exportCrm.addEventListener('click', (e) => {
      e.preventDefault();
      exportToCrm();
    });
  }
  if (els.summary) {
    els.summary.addEventListener('change', (e) => {
      const target = e.target;
      if (!(target instanceof HTMLInputElement)) return;
      if (!target.matches('input[data-month-key]')) return;
      const key = String(target.getAttribute('data-month-key') || '').trim();
      if (!key) return;
      if (target.checked) state.summaryIncludedMonths.add(key);
      else state.summaryIncludedMonths.delete(key);
      if (state.summaryRaw) renderSummary(state.summaryRaw);
    });
  }
  if (els.reverseRowsBtn) {
    els.reverseRowsBtn.addEventListener('click', () => {
      reverseAllPagesRows();
    });
  }
  if (els.pageNumberInput) {
    els.pageNumberInput.addEventListener('input', sanitizePageNumberInput);
    els.pageNumberInput.addEventListener('change', commitPageNumberInput);
    els.pageNumberInput.addEventListener('blur', commitPageNumberInput);
    els.pageNumberInput.addEventListener('keydown', (evt) => {
      if (evt.key !== 'Enter') return;
      evt.preventDefault();
      commitPageNumberInput();
    });
  }
  if (els.pageFirstBtn) {
    els.pageFirstBtn.addEventListener('click', () => {
      if (!state.pages.length) return;
      state.currentPage = state.pages[0];
      state.selectedRowId = null;
      loadCurrentPageData().catch((err) => alert(`Page load failed: ${err.message}`));
      updatePageNav();
    });
  }
  if (els.pagePrevBtn) {
    els.pagePrevBtn.addEventListener('click', () => {
      const idx = state.pages.indexOf(state.currentPage);
      if (idx <= 0) return;
      state.currentPage = state.pages[idx - 1];
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
      state.selectedRowId = null;
      loadCurrentPageData().catch((err) => alert(`Page load failed: ${err.message}`));
      updatePageNav();
    });
  }
  if (els.pageLastBtn) {
    els.pageLastBtn.addEventListener('click', () => {
      if (!state.pages.length) return;
      state.currentPage = state.pages[state.pages.length - 1];
      state.selectedRowId = null;
      loadCurrentPageData().catch((err) => alert(`Page load failed: ${err.message}`));
      updatePageNav();
    });
  }

  if (els.pageNotesBtn) {
    els.pageNotesBtn.addEventListener('click', () => {
      openPageNotesModal();
    });
  }
  if (els.pageAiFixBtn) {
    els.pageAiFixBtn.addEventListener('click', () => {
      requestCurrentPageAiFix();
    });
  }
  if (els.pageNotesCloseBtn) {
    els.pageNotesCloseBtn.addEventListener('click', () => {
      closePageNotesModal();
    });
  }
  if (els.pageNotesCancelBtn) {
    els.pageNotesCancelBtn.addEventListener('click', () => {
      closePageNotesModal();
    });
  }
  if (els.pageNotesSaveBtn) {
    els.pageNotesSaveBtn.addEventListener('click', () => {
      saveCurrentPageNotes();
    });
  }
  if (els.pageNotesModal) {
    els.pageNotesModal.addEventListener('click', (evt) => {
      if (evt.target === els.pageNotesModal) closePageNotesModal();
    });
  }
  if (els.pageAiFixCloseBtn) {
    els.pageAiFixCloseBtn.addEventListener('click', () => {
      closePageAiFixModal();
    });
  }
  if (els.pageAiFixCancelBtn) {
    els.pageAiFixCancelBtn.addEventListener('click', () => {
      closePageAiFixModal();
    });
  }
  if (els.pageAiFixApplyBtn) {
    els.pageAiFixApplyBtn.addEventListener('click', () => {
      applyCurrentPageAiFix();
    });
  }
  if (els.pageAiFixModal) {
    els.pageAiFixModal.addEventListener('click', (evt) => {
      if (evt.target === els.pageAiFixModal) closePageAiFixModal();
    });
  }
  if (els.pageNotesInput) {
    els.pageNotesInput.addEventListener('keydown', (evt) => {
      if ((evt.metaKey || evt.ctrlKey) && evt.key === 'Enter') {
        evt.preventDefault();
        saveCurrentPageNotes();
      }
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
      if (action === 'cancel') {
        const priorLabel = btn.textContent || 'Cancel';
        btn.disabled = true;
        btn.textContent = 'Cancelling…';
        cancelJob(jobId).then((updated) => {
          if (!updated && document.body.contains(btn)) {
            btn.disabled = false;
            btn.textContent = priorLabel;
          }
        });
        return;
      }
      if (action === 'start') startJob(jobId);
      else setActiveJob(jobId, true).catch((err) => alert(`Load job failed: ${normalizeApiErrorMessage(err?.message)}`));
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
        const leadId = String(openBtn.getAttribute('data-lead-id') || '').trim();
        if (!openJobId) return;
        if (leadId) {
          state.crmLeadByJobId[openJobId] = leadId;
          state.currentCrmLeadId = leadId;
        }
        setActiveJob(openJobId, true).catch((err) => alert(`Load job failed: ${normalizeApiErrorMessage(err?.message)}`));
        return;
      }

      const btn = target.closest('button[data-process-attachment-id]');
      if (!btn) return;
      const attachmentId = String(btn.getAttribute('data-process-attachment-id') || '').trim();
      const leadId = String(btn.getAttribute('data-lead-id') || '').trim();
      if (!attachmentId) return;
      const mode = getRequestedProcessMode();
      const priorLabel = btn.textContent || 'Begin Process';
      btn.disabled = true;
      btn.textContent = 'Starting…';
      (() => {
        const params = new URLSearchParams();
        params.set('mode', mode);
        return api(`/crm/attachments/${encodeURIComponent(attachmentId)}/begin-process?${params.toString()}`, { method: 'POST' });
      })()
        .then((payload) => {
          const jobId = String(payload?.job_id || '').trim();
          if (!jobId) throw new Error('missing_job_id');
          const resolvedLeadId = String(payload?.lead_id || leadId || '').trim();
          if (resolvedLeadId) {
            state.crmLeadByJobId[jobId] = resolvedLeadId;
            state.currentCrmLeadId = resolvedLeadId;
          }
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
      state.crmOffset = 0;
      if (state.crmSearchDebounceTimer) window.clearTimeout(state.crmSearchDebounceTimer);
      state.crmSearchDebounceTimer = window.setTimeout(() => {
        state.crmSearchDebounceTimer = null;
        loadCrmAttachments(true).catch((err) => {
          state.crmAttachmentsError = `CRM load failed: ${normalizeApiErrorMessage(err?.message)}`;
          renderCrmAttachmentRows();
        });
      }, 220);
    });
  }

  if (Array.isArray(els.previewTabButtons) && els.previewTabButtons.length) {
    for (const button of els.previewTabButtons) {
      button.addEventListener('click', () => {
        setPreviewPanelTab(button.dataset.previewTab);
      });
    }
  }

  if (els.disbalanceRowsBody) {
    els.disbalanceRowsBody.addEventListener('click', (e) => {
      const target = e.target;
      if (!(target instanceof HTMLElement)) return;
      const btn = target.closest('.disbalance-jump-btn');
      if (!btn) return;
      const page = String(btn.getAttribute('data-page') || '').trim();
      const rowId = String(btn.getAttribute('data-row-id') || '').trim();
      jumpToParsedRow(page, rowId).catch((err) => {
        alert(`Unable to jump to row: ${normalizeApiErrorMessage(err?.message)}`);
      });
    });
  }

  if (els.flaggedRowsBody) {
    els.flaggedRowsBody.addEventListener('click', (e) => {
      const target = e.target;
      if (!(target instanceof HTMLElement)) return;
      const btn = target.closest('.flagged-jump-btn');
      if (!btn) return;
      const page = String(btn.getAttribute('data-page') || '').trim();
      const rowId = String(btn.getAttribute('data-row-id') || '').trim();
      jumpToParsedRow(page, rowId).catch((err) => {
        alert(`Unable to jump to row: ${normalizeApiErrorMessage(err?.message)}`);
      });
    });
  }

  if (Array.isArray(els.crmStatusTabs) && els.crmStatusTabs.length) {
    for (const button of els.crmStatusTabs) {
      button.addEventListener('click', () => {
        setCrmStatusTab(button.dataset.crmStatusTab);
      });
    }
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
  window.addEventListener('keydown', (evt) => {
    if (evt.key === 'Escape' && state.pageNotesModalOpen) {
      evt.preventDefault();
      closePageNotesModal();
      return;
    }
    if (evt.key === 'Escape' && state.pageAiFixModalOpen) {
      evt.preventDefault();
      closePageAiFixModal();
    }
  });

  if (els.previewImage) els.previewImage.addEventListener('load', drawSelectedBound);
  if (els.previewImage) els.previewImage.addEventListener('load', resetPreviewZoom);
  if (els.previewImage) els.previewImage.addEventListener('load', syncParsedSectionHeightToPreview);
  if (els.previewWrap) els.previewWrap.addEventListener('wheel', handlePreviewWheelZoom, { passive: false });
  if (els.previewWrap) els.previewWrap.addEventListener('mousedown', startPreviewPan);
  window.addEventListener('mousemove', movePreviewPan);
  window.addEventListener('mouseup', stopPreviewPan);
  window.addEventListener('mouseleave', stopPreviewPan);
  window.addEventListener('resize', () => {
    syncPreviewZoomLayout();
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
  initProcessingGoogleVisionParser();
  initRequestedProcessMode();
  setGoogleVisionReparseVisibility(null);
  setCrmRefreshState();
  setCrmLoadMoreState();
  setCrmPaginationState();
  renderCrmAttachmentRows();
  setParsedPanelMode('table');
  setPreviewPanelTab('preview');
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

  // Use XHR instead of fetch so upload progress events can drive the live progress bar.
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
    await ensureUiSettingsLoaded();
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
    if (!hasData) {
      state.summaryRaw = null;
      state.summaryIncludedMonths = new Set();
      state.summaryKnownMonthKeys = new Set();
      state.summarySelectionInitialized = false;
    } else {
      state.summaryRaw = summary;
    }

    if (els.summary) {
      els.summary.classList.toggle('hidden', !hasData);
      if (!hasData) {
        els.summary.innerHTML = '';
      } else {
        const monthlyRows = syncSummaryMonthSelection(summary.monthly);
        const includedMonthlyRows = monthlyRows.filter((row) => state.summaryIncludedMonths.has(row.__monthKey));
        const hasMonthlyRows = monthlyRows.length > 0;

        const computedDebitTransactions = hasMonthlyRows
          ? includedMonthlyRows.reduce((sum, row) => sum + toFiniteNumber(row.debit_count), 0)
          : toFiniteNumber(summary.debit_transactions);
        const computedCreditTransactions = hasMonthlyRows
          ? includedMonthlyRows.reduce((sum, row) => sum + toFiniteNumber(row.credit_count), 0)
          : toFiniteNumber(summary.credit_transactions);
        const computedTotalTransactions = hasMonthlyRows
          ? computedDebitTransactions + computedCreditTransactions
          : toFiniteNumber(summary.total_transactions);
        const computedTotalDebit = hasMonthlyRows
          ? includedMonthlyRows.reduce((sum, row) => sum + toFiniteNumber(row.debit), 0)
          : toFiniteNumber(summary.total_debit);
        const computedTotalCredit = hasMonthlyRows
          ? includedMonthlyRows.reduce((sum, row) => sum + toFiniteNumber(row.credit), 0)
          : toFiniteNumber(summary.total_credit);
        const computedAdb = hasMonthlyRows
          ? (includedMonthlyRows.length
            ? includedMonthlyRows.reduce((sum, row) => sum + toFiniteNumber(row.adb), 0) / includedMonthlyRows.length
            : 0)
          : toFiniteNumber(summary.adb);

        const totalCreditNumber = computedTotalCredit;
        const computedMonthlyCreditAverage = Number.isFinite(totalCreditNumber)
          ? (totalCreditNumber / Math.max(includedMonthlyRows.length, 1))
          : summary.monthly_credit_average;
        const computedMonthlyDisposableIncome = Number.isFinite(computedMonthlyCreditAverage)
          ? computedMonthlyCreditAverage * 0.30
          : summary.monthly_disposable_income;
        const metrics = [
          { label: 'Total Transactions', value: formatNumber(computedTotalTransactions), negative: computedTotalTransactions < 0 },
          { label: 'Debit Transactions', value: formatNumber(computedDebitTransactions), negative: computedDebitTransactions < 0 },
          { label: 'Credit Transactions', value: formatNumber(computedCreditTransactions), negative: computedCreditTransactions < 0 },
          { label: 'Total Debit', value: formatCurrencyOrDash(computedTotalDebit), negative: computedTotalDebit < 0 },
          { label: 'Total Credit', value: formatCurrencyOrDash(computedTotalCredit), negative: computedTotalCredit < 0 },
          { label: 'Monthly Credit Average', value: formatCurrencyOrDash(computedMonthlyCreditAverage), negative: Number(computedMonthlyCreditAverage) < 0 },
          { label: 'Monthly Disposable Income', value: formatCurrencyOrDash(computedMonthlyDisposableIncome), negative: Number(computedMonthlyDisposableIncome) < 0 },
          { label: 'Average Daily Balance (ADB)', value: formatCurrencyOrDash(computedAdb), negative: Number(computedAdb) < 0 }
        ];

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
                    <th class="include">Include</th>
                    <th>Month</th>
                    <th class="num">Total Debit</th>
                    <th class="num">Total Credit</th>
                    <th class="num">Debit Count</th>
                    <th class="num">Credit Count</th>
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
                            <td class="include">
                              <input
                                type="checkbox"
                                class="summary-monthly-checkbox"
                                data-month-key="${escapeHtml(row.__monthKey)}"
                                ${state.summaryIncludedMonths.has(row.__monthKey) ? 'checked' : ''}
                              />
                            </td>
                            <td>${escapeHtml(row.month || '-')}</td>
                            <td class="num${Number(row.debit) < 0 ? ' is-negative' : ''}">${escapeHtml(formatCurrencyOrDash(row.debit))}</td>
                            <td class="num${Number(row.credit) < 0 ? ' is-negative' : ''}">${escapeHtml(formatCurrencyOrDash(row.credit))}</td>
                            <td class="num">${escapeHtml(formatNumber(row.debit_count || 0))}</td>
                            <td class="num">${escapeHtml(formatNumber(row.credit_count || 0))}</td>
                            <td class="num${Number(row.avg_debit) < 0 ? ' is-negative' : ''}">${escapeHtml(formatCurrencyOrDash(row.avg_debit))}</td>
                            <td class="num${Number(row.avg_credit) < 0 ? ' is-negative' : ''}">${escapeHtml(formatCurrencyOrDash(row.avg_credit))}</td>
                            <td class="num${Number(row.adb) < 0 ? ' is-negative' : ''}">${escapeHtml(formatCurrencyOrDash(row.adb))}</td>
                          </tr>
                        `).join('')
                      : '<tr><td colspan="9" class="summary-table-empty">No monthly data available.</td></tr>'
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
