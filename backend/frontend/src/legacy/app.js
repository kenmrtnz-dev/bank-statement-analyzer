const uploadArea = document.getElementById('uploadArea');
const browseButton = document.getElementById('browseButton');
const fileInput = document.getElementById('fileInput');
const fileInfo = document.getElementById('fileInfo');
const fileName = document.getElementById('fileName');
const fileSize = document.getElementById('fileSize');
const removeFileBtn = document.getElementById('removeFile');
const progressFill = document.getElementById('progressFill');
const progressPercent = document.getElementById('progressPercent');
const progressLabel = document.getElementById('progressLabel');
const progressStep = document.getElementById('progressStep');
const progressElapsed = document.getElementById('progressElapsed');
const progressModel = document.getElementById('progressModel');
const analyzerStatus = document.getElementById('analyzerStatus');
const progressAnalyzerRow = document.getElementById('progressAnalyzerRow');
const analyzerModel = document.getElementById('analyzerModel');
const analyzerProfile = document.getElementById('analyzerProfile');
const analyzerReason = document.getElementById('analyzerReason');
const downloadCSV = document.getElementById('downloadCSV');
const exportExcelBtn = document.getElementById('exportExcelBtn');
const finishReviewBtn = document.getElementById('finishReviewBtn');
const viewDetails = document.getElementById('viewDetails');
const finishSave = document.getElementById('finishSave');
const tableBody = document.querySelector('.table-body');
const totalTransactions = document.getElementById('totalTransactions');
const totalDebitTransactions = document.getElementById('totalDebitTransactions');
const totalCreditTransactions = document.getElementById('totalCreditTransactions');
const endingBalance = document.getElementById('endingBalance');
const accountNameSummary = document.getElementById('accountNameSummary');
const accountNumberSummary = document.getElementById('accountNumberSummary');
const previewImage = document.getElementById('previewImage');
const previewCanvas = document.getElementById('previewCanvas');
const prevPageBtn = document.getElementById('prevPageBtn');
const nextPageBtn = document.getElementById('nextPageBtn');
const ocrToolsToggleBtn = document.getElementById('ocrToolsToggleBtn');
const flattenModeBtn = document.getElementById('flattenModeBtn');
const applyFlattenBtn = document.getElementById('applyFlattenBtn');
const resetFlattenBtn = document.getElementById('resetFlattenBtn');
const resetViewBtn = document.getElementById('resetViewBtn');
const zoomOutBtn = document.getElementById('zoomOutBtn');
const zoomInBtn = document.getElementById('zoomInBtn');
const addHorizontalGuideBtn = document.getElementById('addHorizontalGuideBtn');
const clearGuideLinesBtn = document.getElementById('clearGuideLinesBtn');
const guideUndoBtn = document.getElementById('guideUndoBtn');
const guideRedoBtn = document.getElementById('guideRedoBtn');
const runSectionOcrBtn = document.getElementById('runSectionOcrBtn');
const imageToolButtons = Array.from(document.querySelectorAll('.preview-image-tool-btn'));
const guideSectionsInfo = document.getElementById('guideSectionsInfo');
const sectionOcrResult = document.getElementById('sectionOcrResult');
const previewColumnsRuler = document.getElementById('previewColumnsRuler');
const previewRowsRuler = document.getElementById('previewRowsRuler');
const pageIndicator = document.getElementById('pageIndicator');
const pageSelect = document.getElementById('pageSelect');
const previewWrap = document.querySelector('.preview-canvas-wrap');
const previewPanel = document.querySelector('.preview-panel');
const tablePanel = document.querySelector('.table-panel');
const transactionsTable = document.querySelector('.transactions-table');
const transactionsTableHeader = document.querySelector('.transactions-table .table-header');
const zoomLevel = document.getElementById('zoomLevel');
const resultsSection = document.querySelector('.results-section');
const previewPageSavedMark = document.getElementById('previewPageSavedMark');
const summaryLockedNote = document.getElementById('summaryLockedNote');
const summaryCard = document.querySelector('.summary-card');
const ocrToggle = document.getElementById('ocrToggle');
const modeToggleText = document.getElementById('modeToggleText');
const legacyMain = document.getElementById('legacyMain');
const authEmail = document.getElementById('authEmail');
const authPassword = document.getElementById('authPassword');
const authLoginBtn = document.getElementById('authLoginBtn');
const authLogoutBtn = document.getElementById('authLogoutBtn');
const authUserBadge = document.getElementById('authUserBadge');
const agentWorkflowPanel = document.getElementById('agentWorkflowPanel');
const evaluatorWorkflowPanel = document.getElementById('evaluatorWorkflowPanel');
const agentFileInput = document.getElementById('agentFileInput');
const agentBrowseButton = document.getElementById('agentBrowseButton');
const agentDropZone = document.getElementById('agentDropZone');
const agentSelectedFiles = document.getElementById('agentSelectedFiles');
const agentSearchInput = document.getElementById('agentSearchInput');
const agentUploadProgressWrap = document.getElementById('agentUploadProgressWrap');
const agentUploadProgressFill = document.getElementById('agentUploadProgressFill');
const agentUploadProgressLabel = document.getElementById('agentUploadProgressLabel');
const agentUploadProgressPercent = document.getElementById('agentUploadProgressPercent');
const agentBorrowerName = document.getElementById('agentBorrowerName');
const agentLeadReference = document.getElementById('agentLeadReference');
const agentSubmitBtn = document.getElementById('agentSubmitBtn');
const agentRefreshBtn = document.getElementById('agentRefreshBtn');
const agentSubmissionList = document.getElementById('agentSubmissionList');
const evaluatorRefreshBtn = document.getElementById('evaluatorRefreshBtn');
const evaluatorSearchInput = document.getElementById('evaluatorSearchInput');
const evaluatorCombineBtn = document.getElementById('evaluatorCombineBtn');
const evaluatorSubmissionList = document.getElementById('evaluatorSubmissionList');
const evaluatorActionBar = document.getElementById('evaluatorActionBar');
const evaluatorStartBtn = document.getElementById('evaluatorStartBtn');
const evaluatorOpenBtn = document.getElementById('evaluatorOpenBtn');
const evaluatorSaveBtn = document.getElementById('evaluatorSaveBtn');
const evaluatorAnalyzeBtn = document.getElementById('evaluatorAnalyzeBtn');
const evaluatorReportBtn = document.getElementById('evaluatorReportBtn');
const evaluatorReadyBtn = document.getElementById('evaluatorReadyBtn');
const forcedPageRole = (document.body && document.body.dataset && document.body.dataset.pageRole) || 'all';

let selectedFile = null;
let currentJobId = null;
let pageList = [];
let parsedRows = [];
let rowsByPage = {};
let boundsByPage = {};
let pageRowToGlobal = {};
let identityBoundsByPage = {};
let currentPageIndex = 0;
let activeRowKey = null;
let shouldAutoScrollToResults = false;
let hasSeenInFlightStatus = false;
let rowKeyCounter = 1;
let flattenMode = false;
let flattenPoints = [];
let flattenBusy = false;
let ocrStarted = false;
let pageImageVersion = {};
let elapsedStartMs = 0;
let elapsedTimer = null;
let previewOverlayDataUrl = '';
const TABLE_EDIT_FIELDS = ['date', 'description', 'debit', 'credit', 'balance'];
let pendingFocusActiveRow = false;
const PREVIEW_ZOOM_MIN = 1;
const PREVIEW_ZOOM_MAX = 3.5;
const PREVIEW_ZOOM_STEP = 0.12;
let previewZoom = 1;
let previewPanX = 0;
let previewPanY = 0;
let isPreviewPanning = false;
let panStartX = 0;
let panStartY = 0;
let panOriginX = 0;
let panOriginY = 0;
const prefetchedPreviewSrcs = new Set();
const previewBlobUrlCache = new Map();
let authToken = localStorage.getItem('auth_token') || '';
let authRole = localStorage.getItem('auth_role') || '';
let authUserEmail = localStorage.getItem('auth_email') || '';
let evaluatorSelectedSubmission = null;
let evaluatorManifest = [];
let evaluatorReviewProgress = { total_pages: 0, parsed_pages: 0, reviewed_pages: 0, percent: 0 };
let evaluatorCanExport = false;
let activePageKey = '';
let activePageReviewStatus = 'pending';
let activePageUpdatedAt = null;
let activePageDirty = false;
let activePageDirtyRevision = 0;
let activePageSaveInFlight = false;
let activePageSavePromise = null;
let activePageAutosaveTimer = null;
let evaluatorManifestTimer = null;
let isAgentSubmitting = false;
let agentSubmissionsCache = [];
let agentSubmissionsPage = 1;
const AGENT_SUBMISSIONS_PAGE_SIZE = 15;
let evaluatorSubmissionsCache = [];
let evaluatorSubmissionsPage = 1;
const EVALUATOR_SUBMISSIONS_PAGE_SIZE = 15;
let evaluatorSelectedSubmissionIds = new Set();
let evaluatorCombineInFlight = false;
let authRedirectInProgress = false;
let statusStallSignature = '';
let statusStallCount = 0;
let progressWatchSignature = '';
let progressWatchUpdatedAt = 0;
let progressWatchStatus = 'queued';
let progressWatchHandled = false;
const GUIDE_LINE_DUP_TOLERANCE = 0.006;
const GUIDE_HISTORY_LIMIT = 100;
const ACTIVE_PAGE_AUTOSAVE_DEBOUNCE_MS = 700;
const COLUMN_LAYOUT_MIN_WIDTH = 0.08;
const HORIZONTAL_LINE_MIN_GAP = 0.01;
const MAX_AUTO_HORIZONTAL_GUIDES = 40;
const DEFAULT_COLUMN_LAYOUT = [
  { key: 'date', label: 'Date', width: 0.16 },
  { key: 'description', label: 'Description', width: 0.34 },
  { key: 'debit', label: 'Debit', width: 0.16 },
  { key: 'credit', label: 'Credit', width: 0.16 },
  { key: 'balance', label: 'Balance', width: 0.18 },
];
let guideLinesByPage = {};
let guideHistoryByPage = {};
let columnLayoutByPage = {};
let columnDragState = { sourceKey: '', targetKey: '' };
let columnResizeState = null;
let columnSwapSelectKey = '';
let horizontalGuideDragState = null;
let horizontalGuideTouchedByPage = {};
let activeGuideTool = 'none';
let sectionOcrResultsByPage = {};
let sectionOcrInFlight = false;
let sectionOcrProgressTimer = null;
let sectionOcrProgressValue = 0;
let imageToolInFlight = false;
let activeParseMode = 'text';
let ocrToolsUnlocked = false;
let toastContainer = null;

function ensureToastContainer() {
  if (toastContainer && document.body.contains(toastContainer)) return toastContainer;
  toastContainer = document.getElementById('appToastContainer');
  if (!toastContainer) {
    toastContainer = document.createElement('div');
    toastContainer.id = 'appToastContainer';
    toastContainer.className = 'app-toast-container';
    document.body.appendChild(toastContainer);
  }
  return toastContainer;
}

function inferToastType(message) {
  const text = String(message || '').toLowerCase();
  if (!text) return 'info';
  if (text.includes('failed') || text.includes('error') || text.includes('forbidden') || text.includes('timeout')) {
    return 'error';
  }
  if (text.includes('warning') || text.includes('stalled') || text.includes('select') || text.includes('blocked')) {
    return 'warning';
  }
  if (text.includes('done') || text.includes('saved') || text.includes('completed') || text.includes('enabled') || text.includes('submitted')) {
    return 'success';
  }
  return 'info';
}

function showToast(message, type = 'info', durationMs = 3200) {
  const container = ensureToastContainer();
  const toast = document.createElement('div');
  toast.className = `app-toast app-toast-${type}`;
  toast.setAttribute('role', 'status');

  const textEl = document.createElement('div');
  textEl.className = 'app-toast-text';
  textEl.textContent = String(message || '');
  toast.appendChild(textEl);

  const closeBtn = document.createElement('button');
  closeBtn.className = 'app-toast-close';
  closeBtn.type = 'button';
  closeBtn.setAttribute('aria-label', 'Dismiss notification');
  closeBtn.textContent = '×';
  closeBtn.addEventListener('click', () => {
    toast.classList.add('closing');
    setTimeout(() => toast.remove(), 140);
  });
  toast.appendChild(closeBtn);

  container.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add('show'));

  if (durationMs > 0) {
    setTimeout(() => {
      if (!toast.isConnected) return;
      toast.classList.add('closing');
      setTimeout(() => toast.remove(), 140);
    }, durationMs);
  }
}

if (typeof window !== 'undefined') {
  const nativeAlert = window.alert ? window.alert.bind(window) : null;
  window.__nativeAlert = nativeAlert;
  window.showToast = showToast;
  window.alert = (message) => {
    showToast(message, inferToastType(message));
  };
}

if ((forcedPageRole === 'agent' || forcedPageRole === 'credit_evaluator') && (!authToken || !authRole)) {
  window.location.href = '/login';
}
if (forcedPageRole === 'agent' && authRole && authRole !== 'agent') {
  if (authRole === 'credit_evaluator') {
    window.location.href = '/evaluator';
  } else if (authRole === 'admin') {
    window.location.href = '/admin';
  } else {
    window.location.href = '/login';
  }
}
if (forcedPageRole === 'credit_evaluator' && authRole && authRole !== 'credit_evaluator') {
  if (authRole === 'agent') {
    window.location.href = '/agent';
  } else if (authRole === 'admin') {
    window.location.href = '/admin';
  } else {
    window.location.href = '/login';
  }
}

function getSelectedParseMode() {
  return ocrToggle && ocrToggle.checked ? 'ocr' : 'text';
}

function normalizeParseMode(mode) {
  return String(mode || '').trim().toLowerCase() === 'ocr' ? 'ocr' : 'text';
}

function canUseOcrTools() {
  return activeParseMode === 'ocr' || ocrToolsUnlocked;
}

function isTextOnlyToolsMode() {
  return Boolean(authRole === 'credit_evaluator' && evaluatorSelectedSubmission) && !canUseOcrTools();
}

function renderOcrToolsToggle() {
  if (!ocrToolsToggleBtn) return;
  const unlocked = canUseOcrTools();
  ocrToolsToggleBtn.textContent = unlocked ? 'OCR' : 'TEXT';
  ocrToolsToggleBtn.classList.toggle('is-active', unlocked);
  ocrToolsToggleBtn.setAttribute('aria-label', unlocked ? 'OCR tools enabled' : 'OCR tools disabled');
  ocrToolsToggleBtn.title = unlocked ? 'OCR tools enabled' : 'OCR tools disabled';
}

function setActiveParseMode(mode) {
  const nextMode = normalizeParseMode(mode || activeParseMode);
  if (nextMode !== activeParseMode) {
    // Default lock when parser mode is text; default unlock when parser mode is OCR.
    ocrToolsUnlocked = nextMode === 'ocr';
  }
  activeParseMode = nextMode;
  if (isTextOnlyToolsMode()) {
    activeGuideTool = 'none';
    if (flattenMode) {
      flattenMode = false;
      flattenPoints = [];
    }
  }
  renderOcrToolsToggle();
  updateFlattenButtons();
  updateGuideToolButtons();
}

function syncModeToggleText() {
  if (!modeToggleText) return;
  modeToggleText.textContent = getSelectedParseMode().toUpperCase();
}

browseButton.addEventListener('click', (e) => {
  e.stopPropagation();
  openFilePicker();
});

if (ocrToggle) {
  ocrToggle.addEventListener('click', (e) => e.stopPropagation());
  ocrToggle.addEventListener('change', syncModeToggleText);
  const toggleWrap = ocrToggle.closest('.mode-toggle');
  if (toggleWrap) {
    toggleWrap.addEventListener('click', (e) => e.stopPropagation());
  }
}

syncModeToggleText();
updateGuideToolButtons();

function setLegacyEditorVisible(visible) {
  if (!legacyMain) return;
  legacyMain.style.display = visible ? '' : 'none';
}

function setPreviewAspectRatioFromImage() {
  if (!previewImage || !previewImage.naturalWidth || !previewImage.naturalHeight) return;
  const w = Math.max(1, Number(previewImage.naturalWidth));
  const h = Math.max(1, Number(previewImage.naturalHeight));
  const ratio = `${w} / ${h}`;
  if (previewPanel) {
    previewPanel.style.setProperty('--preview-aspect-ratio', ratio);
  }
  if (previewWrap) {
    previewWrap.style.setProperty('aspect-ratio', ratio);
    const wrapWidth = previewWrap.clientWidth || previewWrap.getBoundingClientRect().width || 0;
    if (wrapWidth > 0) {
      const nextHeight = Math.max(120, Math.round((wrapWidth * h) / w));
      previewWrap.style.height = `${nextHeight}px`;
    }
  }
  requestAnimationFrame(syncTablePanelHeightToPreview);
}

function clearPreviewAspectRatio() {
  if (!previewPanel) return;
  previewPanel.style.removeProperty('--preview-aspect-ratio');
  if (previewWrap) {
    previewWrap.style.removeProperty('aspect-ratio');
    previewWrap.style.removeProperty('height');
  }
  clearTablePanelHeightClamp();
}

function clearTablePanelHeightClamp() {
  if (tablePanel) {
    tablePanel.style.removeProperty('height');
    tablePanel.style.removeProperty('max-height');
  }
  if (transactionsTable) {
    transactionsTable.style.removeProperty('height');
    transactionsTable.style.removeProperty('max-height');
  }
  if (tableBody) {
    tableBody.style.removeProperty('height');
    tableBody.style.removeProperty('max-height');
    tableBody.style.removeProperty('overflow-y');
  }
}

function syncTablePanelHeightToPreview() {
  if (!tablePanel || !transactionsTable || !tableBody || !previewPanel || !previewImage || !previewImage.naturalWidth) {
    clearTablePanelHeightClamp();
    return;
  }

  const previewHeight = Math.round(previewPanel.getBoundingClientRect().height || 0);
  if (!Number.isFinite(previewHeight) || previewHeight <= 0) {
    clearTablePanelHeightClamp();
    return;
  }

  tablePanel.style.height = `${previewHeight}px`;
  tablePanel.style.maxHeight = `${previewHeight}px`;
  transactionsTable.style.height = `${previewHeight}px`;
  transactionsTable.style.maxHeight = `${previewHeight}px`;

  const headerHeight = Math.round(
    (transactionsTableHeader && transactionsTableHeader.getBoundingClientRect().height) || 0
  );
  const bodyHeight = Math.max(80, previewHeight - headerHeight - 2);
  tableBody.style.height = `${bodyHeight}px`;
  tableBody.style.maxHeight = `${bodyHeight}px`;
  tableBody.style.overflowY = 'auto';
}

async function fetchAuthed(url, opts = {}) {
  const headers = new Headers(opts.headers || {});
  if (authToken) headers.set('Authorization', `Bearer ${authToken}`);
  const res = await fetch(url, { ...opts, headers });
  if (res.status === 401) {
    const body = await safeParseJson(res.clone());
    const detail = String((body && body.detail) || '').trim().toLowerCase();
    const isExpired =
      detail === 'token_expired' ||
      detail === 'invalid_token' ||
      detail === 'invalid_token_signature' ||
      detail === 'missing_auth_token';
    if (isExpired) {
      handleAuthExpired();
    }
  }
  return res;
}

function handleAuthExpired() {
  if (authRedirectInProgress) return;
  authRedirectInProgress = true;
  clearActivePageAutosaveTimer();
  if (sectionOcrProgressTimer) {
    clearInterval(sectionOcrProgressTimer);
    sectionOcrProgressTimer = null;
  }
  evaluatorSelectedSubmission = null;
  clearEvaluatorManifestTimer();
  clearPreviewBlobCache();
  setAuthState('', '', '');
  setLegacyEditorVisible(false);
  window.location.href = '/login';
}

function setAuthState(token, role, email) {
  authToken = token || '';
  authRole = role || '';
  authUserEmail = email || '';
  if (authToken) {
    localStorage.setItem('auth_token', authToken);
    localStorage.setItem('auth_role', authRole);
    localStorage.setItem('auth_email', authUserEmail);
  } else {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('auth_role');
    localStorage.removeItem('auth_email');
  }
  updateWorkflowVisibility();
}

function updateWorkflowVisibility() {
  if (authUserBadge) {
    const roleLabel = String(authRole || '')
      .split('_')
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(' ');
    authUserBadge.textContent = authToken ? `${authUserEmail || 'User'} (${roleLabel || 'User'})` : 'Not logged in';
  }
  if (authLoginBtn) authLoginBtn.style.display = authToken ? 'none' : '';
  if (authLogoutBtn) authLogoutBtn.style.display = authToken ? '' : 'none';
  const allowAgentPanel = forcedPageRole === 'all' || forcedPageRole === 'agent';
  const allowEvaluatorPanel = forcedPageRole === 'all' || forcedPageRole === 'credit_evaluator';
  if (agentWorkflowPanel) {
    agentWorkflowPanel.style.display = allowAgentPanel && authRole === 'agent' ? '' : 'none';
  }
  if (evaluatorWorkflowPanel) {
    evaluatorWorkflowPanel.style.display = allowEvaluatorPanel && authRole === 'credit_evaluator' ? '' : 'none';
  }
  const showAgentMain = false;
  const showEvaluatorMain = authRole === 'credit_evaluator' && forcedPageRole === 'credit_evaluator' && Boolean(evaluatorSelectedSubmission);
  const showUnifiedMain = authRole === 'credit_evaluator' && forcedPageRole === 'all' && Boolean(evaluatorSelectedSubmission);
  setLegacyEditorVisible(showAgentMain || showEvaluatorMain || showUnifiedMain);
}

function clearEvaluatorManifestTimer() {
  if (evaluatorManifestTimer) {
    clearInterval(evaluatorManifestTimer);
    evaluatorManifestTimer = null;
  }
}

function resetStatusStallTracker() {
  statusStallSignature = '';
  statusStallCount = 0;
  progressWatchSignature = '';
  progressWatchUpdatedAt = Date.now();
  progressWatchStatus = 'queued';
  progressWatchHandled = false;
}

function isStatusStalled(statusBody) {
  const state = String(statusBody && statusBody.status ? statusBody.status : '').toLowerCase();
  const mode = String(statusBody && statusBody.parse_mode ? statusBody.parse_mode : '').toLowerCase();
  if (state !== 'processing' || mode !== 'text') {
    resetStatusStallTracker();
    return false;
  }
  const sig = [
    state,
    String(statusBody.step || ''),
    String(statusBody.progress ?? ''),
    String(statusBody.page || ''),
  ].join('|');
  if (sig === statusStallSignature) {
    statusStallCount += 1;
  } else {
    statusStallSignature = sig;
    statusStallCount = 0;
  }
  return statusStallCount >= 25;
}

function checkProgressWatchdog() {
  if (progressWatchHandled) return;
  const state = String(progressWatchStatus || '').toLowerCase();
  if (state !== 'processing' && state !== 'queued') return;
  if (!progressWatchUpdatedAt) return;
  if ((Date.now() - progressWatchUpdatedAt) < 45000) return;
  progressWatchHandled = true;
  updateProgressUI(
    100,
    'Processing appears stalled',
    'failed',
    null,
    activeParseMode,
    { status: 'failed', message: 'processing_stale_timeout' }
  );
  stopElapsedTimer();
  clearEvaluatorManifestTimer();
}

function areAllManifestPagesParsed() {
  const total = Number(evaluatorReviewProgress && evaluatorReviewProgress.total_pages ? evaluatorReviewProgress.total_pages : evaluatorManifest.length);
  const parsed = Number(evaluatorReviewProgress && evaluatorReviewProgress.parsed_pages ? evaluatorReviewProgress.parsed_pages : 0);
  return total > 0 && parsed >= total;
}

function setSummaryLocked(locked) {
  if (summaryCard) summaryCard.classList.toggle('is-locked', locked);
  if (summaryLockedNote) summaryLockedNote.style.display = locked ? '' : 'none';
  if (downloadCSV) downloadCSV.disabled = !!locked;
  if (exportExcelBtn) exportExcelBtn.disabled = !!locked;
  if (finishReviewBtn) finishReviewBtn.disabled = !locked;
}

function clearActivePageAutosaveTimer() {
  if (activePageAutosaveTimer) {
    clearTimeout(activePageAutosaveTimer);
    activePageAutosaveTimer = null;
  }
}

function scheduleActivePageAutosave() {
  if (!(authRole === 'credit_evaluator' && evaluatorSelectedSubmission && activePageKey)) return;
  clearActivePageAutosaveTimer();
  activePageAutosaveTimer = setTimeout(async () => {
    activePageAutosaveTimer = null;
    if (!activePageDirty || !activePageKey) return;
    await saveActivePageIfDirty({ silent: true });
  }, ACTIVE_PAGE_AUTOSAVE_DEBOUNCE_MS);
}

function markActivePageDirty() {
  if (authRole === 'credit_evaluator' && evaluatorSelectedSubmission) {
    activePageDirty = true;
    activePageDirtyRevision += 1;
    renderActivePageSavedMark();
    scheduleActivePageAutosave();
  }
}

function renderActivePageSavedMark() {
  if (!previewPageSavedMark) return;
  const currentKey = activePageKey || currentPageKey();
  const manifestPage = evaluatorManifest.find((p) => String(p.page_key || '') === String(currentKey || ''));
  const reviewStatus = String(
    (manifestPage && manifestPage.review_status) || activePageReviewStatus || ''
  ).toLowerCase();
  const reviewSaved = reviewStatus === 'saved' || reviewStatus === 'reviewed';
  const isSaved = reviewSaved && !activePageDirty;
  const label = isSaved ? 'Saved' : 'Not saved';
  previewPageSavedMark.classList.toggle('is-saved', isSaved);
  previewPageSavedMark.classList.toggle('is-unsaved', !isSaved);
  previewPageSavedMark.setAttribute('title', label);
  previewPageSavedMark.setAttribute('aria-label', label);
}

function normalizeGuidePosition(value) {
  return clamp(Number(value), 0, 1);
}

function dedupeSortedGuidePositions(values) {
  const sorted = [...values].map(normalizeGuidePosition).sort((a, b) => a - b);
  const out = [];
  sorted.forEach((v) => {
    if (!out.length || Math.abs(v - out[out.length - 1]) > GUIDE_LINE_DUP_TOLERANCE) {
      out.push(v);
    }
  });
  return out;
}

function getGuideStateForPage(pageKey, create = false) {
  if (!pageKey) return { vertical: [], horizontal: [] };
  const key = String(pageKey);
  if (!guideLinesByPage[key] && create) {
    guideLinesByPage[key] = { vertical: [], horizontal: [] };
  }
  return guideLinesByPage[key] || { vertical: [], horizontal: [] };
}

function cloneDefaultColumnLayout() {
  return DEFAULT_COLUMN_LAYOUT.map((col) => ({ ...col }));
}

function normalizeColumnLayout(layout) {
  const defaults = cloneDefaultColumnLayout();
  const defaultByKey = new Map(defaults.map((col) => [col.key, col]));
  const normalized = [];
  const seen = new Set();

  (Array.isArray(layout) ? layout : []).forEach((item) => {
    const key = String(item && item.key ? item.key : '').trim();
    if (!defaultByKey.has(key) || seen.has(key)) return;
    seen.add(key);
    const fallback = defaultByKey.get(key);
    const widthRaw = Number(item && item.width);
    normalized.push({
      key,
      label: fallback.label,
      width: Number.isFinite(widthRaw) ? widthRaw : fallback.width,
    });
  });

  defaults.forEach((col) => {
    if (seen.has(col.key)) return;
    normalized.push({ ...col });
  });

  const clamped = normalized.map((col) => ({
    ...col,
    width: Math.max(COLUMN_LAYOUT_MIN_WIDTH, Number(col.width || 0)),
  }));
  const total = clamped.reduce((sum, col) => sum + col.width, 0) || 1;
  return clamped.map((col) => ({ ...col, width: col.width / total }));
}

function computeVerticalLinesFromColumnLayout(layout) {
  const lines = [];
  let cursor = 0;
  for (let i = 0; i < layout.length - 1; i += 1) {
    cursor += Number(layout[i].width || 0);
    lines.push(cursor);
  }
  return dedupeSortedGuidePositions(lines);
}

function getColumnLayoutForPage(pageKey, create = false) {
  if (!pageKey) return cloneDefaultColumnLayout();
  const key = String(pageKey);
  if (!columnLayoutByPage[key] && create) {
    columnLayoutByPage[key] = normalizeColumnLayout(cloneDefaultColumnLayout());
  }
  const current = columnLayoutByPage[key] || cloneDefaultColumnLayout();
  return normalizeColumnLayout(current);
}

function setColumnLayoutForPage(pageKey, layout) {
  if (!pageKey) return;
  const key = String(pageKey);
  columnLayoutByPage[key] = normalizeColumnLayout(layout);
}

function buildGuideStatePayload(pageKey) {
  if (!pageKey) {
    return { column_layout: [], horizontal: [] };
  }
  const layout = normalizeColumnLayout(getColumnLayoutForPage(pageKey, true));
  const state = getGuideStateForPage(pageKey, true);
  return {
    column_layout: layout.map((col) => ({
      key: col.key,
      width: Number(Number(col.width || 0).toFixed(6)),
    })),
    horizontal: sanitizeHorizontalGuideLines(state.horizontal || []).map((value) => Number(value.toFixed(6))),
  };
}

function applyGuideStatePayload(pageKey, payload) {
  if (!pageKey || !payload || typeof payload !== 'object') return;
  const state = getGuideStateForPage(pageKey, true);
  const incomingLayout = Array.isArray(payload.column_layout)
    ? payload.column_layout.map((item) => ({
      key: item && item.key ? String(item.key) : '',
      width: Number(item && item.width),
    }))
    : [];
  if (incomingLayout.length) {
    setColumnLayoutForPage(pageKey, incomingLayout);
    state.vertical = computeVerticalLinesFromColumnLayout(getColumnLayoutForPage(pageKey, true));
  } else {
    ensureColumnLayoutForPage(pageKey);
  }
  const hasHorizontal = Array.isArray(payload.horizontal);
  state.horizontal = sanitizeHorizontalGuideLines(hasHorizontal ? payload.horizontal : []);
  if (hasHorizontal) {
    markHorizontalGuideTouched(pageKey);
  }
}

function syncColumnLayoutFromGuideState(pageKey) {
  if (!pageKey) return;
  const state = getGuideStateForPage(pageKey, false);
  const lines = dedupeSortedGuidePositions((state && state.vertical) || []);
  const current = getColumnLayoutForPage(pageKey, true);
  if (lines.length !== current.length - 1) return;

  const bounds = [0, ...lines, 1];
  const adjusted = current.map((col, idx) => ({
    ...col,
    width: Math.max(COLUMN_LAYOUT_MIN_WIDTH, bounds[idx + 1] - bounds[idx]),
  }));
  setColumnLayoutForPage(pageKey, adjusted);
}

function applyColumnLayoutToGuides(pageKey, options = {}) {
  if (!pageKey) return false;
  const opts = {
    recordHistory: false,
    invalidate: true,
    redraw: true,
    ...options,
  };
  const state = getGuideStateForPage(pageKey, true);
  const before = cloneGuideStateSnapshot(state);
  const layout = getColumnLayoutForPage(pageKey, true);
  const next = {
    vertical: computeVerticalLinesFromColumnLayout(layout),
    horizontal: (state.horizontal || []).slice(),
  };
  if (guideStatesEqual(before, next)) {
    if (opts.redraw) drawBoundingBoxes();
    return false;
  }
  if (opts.recordHistory) {
    pushGuideUndoSnapshot(pageKey, before);
  }
  applyGuideStateSnapshot(pageKey, next);
  if (opts.invalidate) {
    invalidateGuideDerivedData(pageKey);
  }
  updateGuideSectionsInfo();
  updateGuideToolButtons();
  if (opts.redraw) drawBoundingBoxes();
  return true;
}

function ensureColumnLayoutForPage(pageKey) {
  if (isTextOnlyToolsMode()) return;
  if (!pageKey) return;
  const key = String(pageKey);
  if (!columnLayoutByPage[key]) {
    setColumnLayoutForPage(key, cloneDefaultColumnLayout());
    applyColumnLayoutToGuides(key, { recordHistory: false, invalidate: false, redraw: false });
    return;
  }
  applyColumnLayoutToGuides(key, { recordHistory: false, invalidate: false, redraw: false });
}

function getColumnRolesForPage(pageKey) {
  const layout = getColumnLayoutForPage(pageKey, true);
  return layout.map((col) => col.key);
}

function renderPreviewColumnsRuler() {
  if (!previewColumnsRuler) return;
  if (isTextOnlyToolsMode()) {
    previewColumnsRuler.innerHTML = '';
    previewColumnsRuler.classList.add('is-hidden');
    resetPreviewColumnsRulerGeometry();
    return;
  }
  const pageKey = currentPageKey();
  if (!pageKey) {
    previewColumnsRuler.innerHTML = '';
    previewColumnsRuler.classList.add('is-hidden');
    resetPreviewColumnsRulerGeometry();
    return;
  }

  const layout = getColumnLayoutForPage(pageKey, true);
  if (columnSwapSelectKey && !layout.some((col) => col.key === columnSwapSelectKey)) {
    columnSwapSelectKey = '';
  }
  previewColumnsRuler.classList.remove('is-hidden');
  previewColumnsRuler.classList.toggle('is-resizing', Boolean(columnResizeState));
  previewColumnsRuler.innerHTML = '';
  const track = document.createElement('div');
  track.className = 'preview-columns-track';
  const boundaries = [0];
  let cursor = 0;
  layout.forEach((col, idx) => {
    cursor += Number(col.width || 0);
    boundaries.push(idx === layout.length - 1 ? 1 : cursor);
  });
  if (boundaries.length !== layout.length + 1) {
    boundaries.length = 0;
    boundaries.push(0);
    for (let i = 0; i < layout.length; i += 1) {
      boundaries.push((i + 1) / layout.length);
    }
  }

  layout.forEach((col, idx) => {
    const start = clamp(boundaries[idx], 0, 1);
    const end = clamp(boundaries[idx + 1], 0, 1);
    const width = Math.max(0, end - start);
    const item = document.createElement('div');
    item.className = 'preview-col-item';
    item.draggable = true;
    item.dataset.colKey = col.key;
    item.style.left = `${(start * 100).toFixed(6)}%`;
    item.style.width = `${(width * 100).toFixed(6)}%`;
    if (columnDragState.sourceKey && columnDragState.sourceKey === col.key) {
      item.classList.add('is-drag-source');
    }
    if (columnDragState.targetKey && columnDragState.targetKey === col.key) {
      item.classList.add('is-drop-target');
    }
    if (columnSwapSelectKey && columnSwapSelectKey === col.key) {
      item.classList.add('is-selected');
    }

    const label = document.createElement('span');
    label.className = 'preview-col-label';
    label.textContent = col.label;
    item.appendChild(label);

    if (idx < layout.length - 1) {
      const resizer = document.createElement('button');
      resizer.type = 'button';
      resizer.className = 'preview-col-resizer';
      resizer.dataset.resizeIndex = String(idx);
      resizer.setAttribute('aria-label', `Resize ${col.label} column`);
      resizer.title = `Resize ${col.label}`;
      item.appendChild(resizer);
    }
    track.appendChild(item);
  });

  for (let i = 1; i < boundaries.length - 1; i += 1) {
    const divider = document.createElement('span');
    divider.className = 'preview-col-divider';
    divider.style.left = `${(clamp(boundaries[i], 0, 1) * 100).toFixed(6)}%`;
    track.appendChild(divider);
  }
  previewColumnsRuler.appendChild(track);
  syncPreviewColumnsRulerGeometryFromCurrentView();
}

function updateRulerDragClasses() {
  if (!previewColumnsRuler) return;
  const items = previewColumnsRuler.querySelectorAll('.preview-col-item');
  items.forEach((item) => {
    const key = String(item.dataset && item.dataset.colKey ? item.dataset.colKey : '');
    item.classList.toggle('is-drag-source', Boolean(columnDragState.sourceKey && columnDragState.sourceKey === key));
    item.classList.toggle('is-drop-target', Boolean(columnDragState.targetKey && columnDragState.targetKey === key));
    item.classList.toggle('is-selected', Boolean(columnSwapSelectKey && columnSwapSelectKey === key));
  });
}

function resetPreviewColumnsRulerGeometry() {
  if (!previewColumnsRuler) return;
  previewColumnsRuler.style.paddingLeft = '';
  previewColumnsRuler.style.paddingRight = '';
  const track = previewColumnsRuler.querySelector('.preview-columns-track');
  if (track) {
    track.style.left = '0px';
    track.style.width = '100%';
  }
}

function syncPreviewColumnsRulerGeometry(canvasLeft, drawW) {
  if (!previewColumnsRuler || !previewWrap) return;
  previewColumnsRuler.style.paddingLeft = '';
  previewColumnsRuler.style.paddingRight = '';
  const track = previewColumnsRuler.querySelector('.preview-columns-track');
  if (!track) return;
  const rulerRect = previewColumnsRuler.getBoundingClientRect();
  const wrapRect = previewWrap.getBoundingClientRect();
  if (!rulerRect.width || !wrapRect.width || !Number.isFinite(drawW) || drawW <= 0) {
    resetPreviewColumnsRulerGeometry();
    return;
  }

  const rulerStyles = window.getComputedStyle(previewColumnsRuler);
  const rulerBorderLeft = Number.parseFloat(rulerStyles.borderLeftWidth || '0') || 0;
  const rulerBorderRight = Number.parseFloat(rulerStyles.borderRightWidth || '0') || 0;

  const wrapOffsetX = wrapRect.left - rulerRect.left;
  const targetLeft = wrapOffsetX + (Number.isFinite(canvasLeft) ? canvasLeft : 0);
  const trackLeft = targetLeft - rulerBorderLeft;
  track.style.left = `${trackLeft.toFixed(3)}px`;
  track.style.width = `${Number(drawW).toFixed(3)}px`;
}

function getPreviewColumnsRulerActiveWidth() {
  if (!previewColumnsRuler) return 0;
  const track = previewColumnsRuler.querySelector('.preview-columns-track');
  if (track) {
    const rect = track.getBoundingClientRect();
    if (rect.width) return Math.max(1, rect.width);
  }
  return Math.max(1, previewColumnsRuler.clientWidth || 1);
}

function syncPreviewColumnsRulerGeometryFromCurrentView() {
  if (!previewColumnsRuler || !previewWrap || !previewImage || !previewImage.naturalWidth) {
    resetPreviewColumnsRulerGeometry();
    return;
  }
  const wrapRect = previewWrap.getBoundingClientRect();
  const rect = getRenderedImageRect(previewImage);
  const baseLeft = rect.left - wrapRect.left;
  const drawW = Math.max(1, Math.round(rect.width * previewZoom));
  const centerX = baseLeft + (rect.width / 2) + previewPanX;
  const canvasLeft = Math.round(centerX - (drawW / 2));
  syncPreviewColumnsRulerGeometry(canvasLeft, drawW);
}

function markHorizontalGuideTouched(pageKey) {
  if (!pageKey) return;
  horizontalGuideTouchedByPage[String(pageKey)] = true;
}

function sanitizeHorizontalGuideLines(values) {
  const filtered = dedupeSortedGuidePositions((values || []).map(normalizeGuidePosition))
    .filter((v) => v > HORIZONTAL_LINE_MIN_GAP && v < 1 - HORIZONTAL_LINE_MIN_GAP);
  const out = [];
  filtered.forEach((v) => {
    if (!out.length || v - out[out.length - 1] >= HORIZONTAL_LINE_MIN_GAP) {
      out.push(v);
    }
  });
  if (out.length <= MAX_AUTO_HORIZONTAL_GUIDES) return out;
  const reduced = [];
  const stride = out.length / MAX_AUTO_HORIZONTAL_GUIDES;
  for (let i = 0; i < MAX_AUTO_HORIZONTAL_GUIDES; i += 1) {
    reduced.push(out[Math.floor(i * stride)]);
  }
  return dedupeSortedGuidePositions(reduced);
}

function inferAutoHorizontalGuidesForPage(pageKey) {
  const bounds = Array.isArray(boundsByPage[pageKey]) ? boundsByPage[pageKey] : [];
  const rows = bounds
    .map((b) => ({
      y1: clamp(Number(b && b.y1), 0, 1),
      y2: clamp(Number(b && b.y2), 0, 1),
    }))
    .filter((r) => Number.isFinite(r.y1) && Number.isFinite(r.y2) && r.y2 > r.y1)
    .sort((a, b) => ((a.y1 + a.y2) / 2) - ((b.y1 + b.y2) / 2));

  if (rows.length < 2) return [];
  const lines = [];
  for (let i = 0; i < rows.length - 1; i += 1) {
    const current = rows[i];
    const next = rows[i + 1];
    const currMid = (current.y1 + current.y2) / 2;
    const nextMid = (next.y1 + next.y2) / 2;
    const split = next.y1 > current.y2
      ? (current.y2 + next.y1) / 2
      : (currMid + nextMid) / 2;
    if (Number.isFinite(split)) {
      lines.push(split);
    }
  }
  return sanitizeHorizontalGuideLines(lines);
}

function maybeAutoSeedHorizontalGuides(pageKey, options = {}) {
  if (isTextOnlyToolsMode()) return false;
  if (!pageKey) return false;
  const key = String(pageKey);
  if (horizontalGuideTouchedByPage[key]) return false;

  const state = getGuideStateForPage(key, true);
  if ((state.horizontal || []).length) return false;

  const suggested = inferAutoHorizontalGuidesForPage(key);
  if (!suggested.length) return false;

  state.horizontal = suggested.slice();
  const opts = { redraw: false, invalidate: false, ...options };
  if (opts.invalidate) {
    invalidateGuideDerivedData(key);
  }
  updateGuideSectionsInfo();
  updateGuideToolButtons();
  if (opts.redraw) {
    drawBoundingBoxes();
  }
  return true;
}

function renderPreviewRowsRuler() {
  if (!previewRowsRuler) return;
  if (isTextOnlyToolsMode()) {
    previewRowsRuler.innerHTML = '';
    previewRowsRuler.classList.add('is-hidden');
    return;
  }
  const pageKey = currentPageKey();
  if (!pageKey) {
    previewRowsRuler.innerHTML = '';
    previewRowsRuler.classList.add('is-hidden');
    return;
  }

  previewRowsRuler.classList.remove('is-hidden');
  previewRowsRuler.classList.toggle('is-resizing', Boolean(horizontalGuideDragState));
  const state = getGuideStateForPage(pageKey, true);
  const lines = sanitizeHorizontalGuideLines(state.horizontal || []);
  if (!guideStatesEqual({ vertical: [], horizontal: state.horizontal || [] }, { vertical: [], horizontal: lines })) {
    state.horizontal = lines.slice();
  }

  previewRowsRuler.innerHTML = '';
  lines.forEach((y, idx) => {
    const handle = document.createElement('button');
    handle.type = 'button';
    handle.className = 'preview-row-handle';
    handle.dataset.rowIndex = String(idx);
    handle.style.top = `${(y * 100).toFixed(3)}%`;
    handle.title = `Move row guide ${idx + 1}`;
    handle.setAttribute('aria-label', `Move row guide ${idx + 1}`);
    previewRowsRuler.appendChild(handle);
  });
}

function reorderColumnLayout(pageKey, sourceKey, targetKey) {
  const layout = getColumnLayoutForPage(pageKey, true);
  const from = layout.findIndex((col) => col.key === sourceKey);
  const to = layout.findIndex((col) => col.key === targetKey);
  if (from < 0 || to < 0 || from === to) return false;
  const next = layout.slice();
  const [moved] = next.splice(from, 1);
  next.splice(to, 0, moved);
  setColumnLayoutForPage(pageKey, next);
  applyColumnLayoutToGuides(pageKey, { recordHistory: false, invalidate: true, redraw: true });
  renderPreviewColumnsRuler();
  markActivePageDirty();
  return true;
}

function applyColumnResize(pageKey, index, deltaRatio, options = {}) {
  const opts = { redraw: true, invalidate: false, ...options };
  const layout = getColumnLayoutForPage(pageKey, true);
  if (index < 0 || index >= layout.length - 1) return false;
  const left = layout[index];
  const right = layout[index + 1];
  if (!left || !right) return false;

  const totalPair = left.width + right.width;
  let leftWidth = left.width + deltaRatio;
  let rightWidth = right.width - deltaRatio;

  if (leftWidth < COLUMN_LAYOUT_MIN_WIDTH) {
    leftWidth = COLUMN_LAYOUT_MIN_WIDTH;
    rightWidth = totalPair - leftWidth;
  }
  if (rightWidth < COLUMN_LAYOUT_MIN_WIDTH) {
    rightWidth = COLUMN_LAYOUT_MIN_WIDTH;
    leftWidth = totalPair - rightWidth;
  }
  if (leftWidth < COLUMN_LAYOUT_MIN_WIDTH || rightWidth < COLUMN_LAYOUT_MIN_WIDTH) return false;

  const next = layout.map((col) => ({ ...col }));
  next[index].width = leftWidth;
  next[index + 1].width = rightWidth;
  setColumnLayoutForPage(pageKey, next);
  applyColumnLayoutToGuides(pageKey, { recordHistory: false, invalidate: opts.invalidate, redraw: opts.redraw });
  renderPreviewColumnsRuler();
  return true;
}

function cloneGuideStateSnapshot(state) {
  return {
    vertical: dedupeSortedGuidePositions((state && state.vertical) || []),
    horizontal: dedupeSortedGuidePositions((state && state.horizontal) || []),
  };
}

function guideStatesEqual(a, b) {
  const av = (a && a.vertical) || [];
  const bv = (b && b.vertical) || [];
  const ah = (a && a.horizontal) || [];
  const bh = (b && b.horizontal) || [];
  if (av.length !== bv.length || ah.length !== bh.length) return false;
  for (let i = 0; i < av.length; i += 1) {
    if (Math.abs(av[i] - bv[i]) > GUIDE_LINE_DUP_TOLERANCE) return false;
  }
  for (let i = 0; i < ah.length; i += 1) {
    if (Math.abs(ah[i] - bh[i]) > GUIDE_LINE_DUP_TOLERANCE) return false;
  }
  return true;
}

function getGuideHistoryForPage(pageKey, create = false) {
  if (!pageKey) return null;
  const key = String(pageKey);
  if (!guideHistoryByPage[key] && create) {
    guideHistoryByPage[key] = { undo: [], redo: [] };
  }
  return guideHistoryByPage[key] || null;
}

function pushGuideUndoSnapshot(pageKey, snapshot) {
  const history = getGuideHistoryForPage(pageKey, true);
  history.undo.push(cloneGuideStateSnapshot(snapshot));
  if (history.undo.length > GUIDE_HISTORY_LIMIT) {
    history.undo.shift();
  }
  history.redo = [];
}

function applyGuideStateSnapshot(pageKey, snapshot) {
  const state = getGuideStateForPage(pageKey, true);
  const cloned = cloneGuideStateSnapshot(snapshot);
  state.vertical = cloned.vertical;
  state.horizontal = cloned.horizontal;
}

function invalidateGuideDerivedData(pageKey) {
  delete sectionOcrResultsByPage[pageKey];
  renderSectionOcrResultForPage(pageKey);
}

function canUndoGuides(pageKey) {
  const history = getGuideHistoryForPage(pageKey, false);
  return Boolean(history && history.undo.length);
}

function canRedoGuides(pageKey) {
  const history = getGuideHistoryForPage(pageKey, false);
  return Boolean(history && history.redo.length);
}

function getGuideSectionsForPage(pageKey) {
  if (isTextOnlyToolsMode()) return [];
  ensureColumnLayoutForPage(pageKey);
  const state = getGuideStateForPage(pageKey, false);
  const xs = dedupeSortedGuidePositions([0, ...(state.vertical || []), 1]);
  const ys = dedupeSortedGuidePositions([0, ...(state.horizontal || []), 1]);
  const sections = [];
  for (let yi = 0; yi < ys.length - 1; yi += 1) {
    for (let xi = 0; xi < xs.length - 1; xi += 1) {
      sections.push({
        x1: xs[xi],
        y1: ys[yi],
        x2: xs[xi + 1],
        y2: ys[yi + 1],
      });
    }
  }
  return sections;
}

function updateGuideSectionsInfo() {
  if (!guideSectionsInfo) return;
  const pageKey = currentPageKey();
  if (!pageKey) {
    guideSectionsInfo.textContent = '';
    return;
  }
  const state = getGuideStateForPage(pageKey, false);
  const lineCount = (state.vertical || []).length + (state.horizontal || []).length;
  if (!lineCount) {
    guideSectionsInfo.textContent = 'No guides';
    return;
  }
  const sections = getGuideSectionsForPage(pageKey);
  guideSectionsInfo.textContent = `${lineCount} line${lineCount === 1 ? '' : 's'} • ${sections.length} section${sections.length === 1 ? '' : 's'}`;
}

function setSectionOcrBusy(active) {
  sectionOcrInFlight = !!active;
  if (runSectionOcrBtn) {
    runSectionOcrBtn.disabled = sectionOcrInFlight || !pageList.length || !currentJobId;
    runSectionOcrBtn.textContent = sectionOcrInFlight ? 'OCR...' : 'OCR';
  }
}

function setImageToolBusy(active) {
  imageToolInFlight = !!active;
  updateGuideToolButtons();
}

function imageToolLabel(tool) {
  const key = String(tool || '').toLowerCase();
  const map = {
    deskew: 'Deskew',
    contrast: 'Contrast',
    binarize: 'Binarize',
    denoise: 'Denoise',
    sharpen: 'Sharpen',
    remove_lines: 'Remove lines',
    reset: 'Reset cleanup',
  };
  return map[key] || key;
}

async function applyImageToolForCurrentPage(tool) {
  if (!tool || !currentJobId || !pageList.length || imageToolInFlight) return;
  const pageKey = currentPageKey();
  if (!pageKey) return;

  try {
    setImageToolBusy(true);
    const res = await fetchAuthed(`/jobs/${currentJobId}/pages/${pageKey}/image-tool`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tool }),
    });
    const body = await safeParseJson(res);
    if (!res.ok) {
      throw new Error((body && body.detail) || `Failed to apply ${imageToolLabel(tool)}`);
    }
    await refreshCurrentPageData(pageKey);
    renderCurrentPage();
  } catch (err) {
    alert(err.message || `Failed to apply ${imageToolLabel(tool)}`);
  } finally {
    setImageToolBusy(false);
  }
}

function startSectionOcrProgress(sectionCount) {
  if (sectionOcrProgressTimer) {
    clearInterval(sectionOcrProgressTimer);
    sectionOcrProgressTimer = null;
  }
  sectionOcrProgressValue = 10;
  startElapsedTimer();
  const label = `Running section OCR (${sectionCount} section${sectionCount === 1 ? '' : 's'})`;
  updateProgressUI(sectionOcrProgressValue, label, 'section_ocr', 'tesseract', 'ocr');
  sectionOcrProgressTimer = setInterval(() => {
    const bump = sectionOcrProgressValue < 60 ? 4 : 2;
    sectionOcrProgressValue = Math.min(92, sectionOcrProgressValue + bump);
    updateProgressUI(sectionOcrProgressValue, label, 'section_ocr', 'tesseract', 'ocr');
  }, 700);
}

function finishSectionOcrProgress(ok, sectionCount = 0, errMessage = '') {
  if (sectionOcrProgressTimer) {
    clearInterval(sectionOcrProgressTimer);
    sectionOcrProgressTimer = null;
  }
  if (ok) {
    updateProgressUI(
      100,
      `Section OCR completed (${sectionCount} section${sectionCount === 1 ? '' : 's'})`,
      'section_ocr',
      'tesseract',
      'ocr'
    );
  } else {
    const fallback = errMessage || 'Section OCR failed';
    updateProgressUI(Math.max(0, sectionOcrProgressValue), fallback, 'failed', 'tesseract', 'ocr');
  }
  stopElapsedTimer();
}

function renderSectionOcrResultForPage(pageKey) {
  if (!sectionOcrResult) return;
  sectionOcrResult.innerHTML = '';
  sectionOcrResult.classList.add('is-hidden');
}

function normalizeSectionText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function detectSectionHeaderRole(text) {
  const t = normalizeSectionText(text).toLowerCase();
  if (!t) return '';
  if (/\b(book\s+date|value\s+date|posting\s+date|date)\b/.test(t)) return 'date';
  if (/\b(description|particulars?|details?|transaction)\b/.test(t)) return 'description';
  if (/\b(debit|withdraw(al)?|debits|dr)\b/.test(t)) return 'debit';
  if (/\b(credit|deposit|credits|cr)\b/.test(t)) return 'credit';
  if (/\b(balance|ending\s+balance|closing\s+balance|end\s+balance)\b/.test(t)) return 'balance';
  return '';
}

function inferSectionRoleByIndex(index, totalCols) {
  if (totalCols >= 5) {
    return ['date', 'description', 'debit', 'credit', 'balance'][index] || 'description';
  }
  if (totalCols === 4) {
    return ['date', 'description', 'debit', 'balance'][index] || 'description';
  }
  if (totalCols === 3) {
    return ['date', 'description', 'balance'][index] || 'description';
  }
  if (totalCols === 2) {
    return ['date', 'description'][index] || 'description';
  }
  return 'description';
}

function groupOcrSectionsByRow(sections) {
  const bands = [];
  const tol = 0.01;
  const sorted = [...sections].sort((a, b) => {
    const ay = Number(a.y1 || 0);
    const by = Number(b.y1 || 0);
    if (Math.abs(ay - by) > 0.0001) return ay - by;
    return Number(a.x1 || 0) - Number(b.x1 || 0);
  });

  sorted.forEach((cell) => {
    const y1 = Number(cell.y1 || 0);
    const y2 = Number(cell.y2 || 0);
    let band = bands.find((b) => Math.abs(b.y1 - y1) <= tol && Math.abs(b.y2 - y2) <= tol);
    if (!band) {
      band = { y1, y2, cells: [] };
      bands.push(band);
    } else {
      band.y1 = Math.min(band.y1, y1);
      band.y2 = Math.max(band.y2, y2);
    }
    band.cells.push(cell);
  });

  bands.forEach((band) => {
    band.cells.sort((a, b) => Number(a.x1 || 0) - Number(b.x1 || 0));
  });
  bands.sort((a, b) => a.y1 - b.y1);
  return bands;
}

function detectSectionHeaderBandIndex(bands) {
  let bestIdx = -1;
  let bestScore = 0;
  bands.forEach((band, idx) => {
    const roles = new Set();
    band.cells.forEach((cell) => {
      const role = detectSectionHeaderRole(cell.text);
      if (role) roles.add(role);
    });
    if (roles.size < 2) return;
    let score = roles.size;
    if (roles.has('date')) score += 1;
    if (roles.has('balance')) score += 1;
    if (score > bestScore) {
      bestScore = score;
      bestIdx = idx;
    }
  });
  return bestIdx;
}

function buildSectionColumnRoles(headerBand) {
  const fallback = ['date', 'description', 'debit', 'credit', 'balance'];
  const roles = headerBand.cells.map((cell, idx) => {
    const detected = detectSectionHeaderRole(cell.text);
    return detected || fallback[idx] || 'description';
  });
  return roles;
}

function extractAmountLikeText(text) {
  const raw = normalizeSectionText(text);
  if (!raw) return '';
  const parsed = normalizeAmount(raw);
  if (!Number.isFinite(parsed)) return '';
  return String(parsed);
}

function rowSeemsHeaderLike(row) {
  const combined = normalizeSectionText(`${row.date} ${row.description} ${row.debit} ${row.credit} ${row.balance}`).toLowerCase();
  let headerHints = 0;
  if (/\bdate\b/.test(combined)) headerHints += 1;
  if (/\bdescription|particulars?|details?\b/.test(combined)) headerHints += 1;
  if (/\bdebit|credit|balance\b/.test(combined)) headerHints += 1;
  return headerHints >= 2;
}

function convertSectionOcrToTableRows(pageKey, payload) {
  const rawSections = Array.isArray(payload && payload.sections) ? payload.sections : [];
  if (!rawSections.length) return { rows: [], bounds: [] };

  const sections = rawSections.map((sec) => ({
    x1: Number(sec.x1 || 0),
    y1: Number(sec.y1 || 0),
    x2: Number(sec.x2 || 0),
    y2: Number(sec.y2 || 0),
    text: normalizeSectionText(sec.text),
  }));

  const bands = groupOcrSectionsByRow(sections);
  if (!bands.length) return { rows: [], bounds: [] };

  const manualRoles = getColumnRolesForPage(pageKey);
  const headerIdx = manualRoles && manualRoles.length ? -1 : detectSectionHeaderBandIndex(bands);
  const columnRoles = (manualRoles && manualRoles.length)
    ? manualRoles
    : (headerIdx >= 0 ? buildSectionColumnRoles(bands[headerIdx]) : null);
  const dataBands = headerIdx >= 0 ? bands.slice(headerIdx + 1) : bands;

  const rows = [];
  const bounds = [];
  let rowCounter = 1;

  dataBands.forEach((band) => {
    const cells = band.cells || [];
    if (!cells.length) return;

    const row = {
      row_id: String(rowCounter).padStart(3, '0'),
      page: pageKey,
      date: '',
      description: '',
      debit: '',
      credit: '',
      balance: '',
    };

    cells.forEach((cell, idx) => {
      const text = normalizeSectionText(cell.text);
      if (!text) return;
      const role = (columnRoles && columnRoles[idx]) || inferSectionRoleByIndex(idx, cells.length);
      if (role === 'description') {
        row.description = row.description ? `${row.description} ${text}` : text;
        return;
      }
      if (role === 'date') {
        if (!row.date) row.date = text;
        return;
      }
      if (role === 'debit') {
        if (!row.debit) row.debit = extractAmountLikeText(text) || text;
        return;
      }
      if (role === 'credit') {
        if (!row.credit) row.credit = extractAmountLikeText(text) || text;
        return;
      }
      if (role === 'balance') {
        if (!row.balance) row.balance = extractAmountLikeText(text) || text;
        return;
      }
      row.description = row.description ? `${row.description} ${text}` : text;
    });

    row.description = normalizeSectionText(row.description);
    const hasAny = Boolean(
      normalizeSectionText(row.date) ||
      normalizeSectionText(row.description) ||
      normalizeSectionText(row.debit) ||
      normalizeSectionText(row.credit) ||
      normalizeSectionText(row.balance)
    );
    if (!hasAny) return;
    if (rowSeemsHeaderLike(row)) return;

    const x1 = Math.max(0, Math.min(1, Math.min(...cells.map((c) => Number(c.x1 || 0)))));
    const y1 = Math.max(0, Math.min(1, Math.min(...cells.map((c) => Number(c.y1 || 0)))));
    const x2 = Math.max(0, Math.min(1, Math.max(...cells.map((c) => Number(c.x2 || 0)))));
    const y2 = Math.max(0, Math.min(1, Math.max(...cells.map((c) => Number(c.y2 || 0)))));

    row.x1 = x1;
    row.y1 = y1;
    row.x2 = x2;
    row.y2 = y2;

    rows.push(row);
    bounds.push({
      row_id: row.row_id,
      x1,
      y1,
      x2,
      y2,
    });
    rowCounter += 1;
  });

  return { rows, bounds };
}

async function runSectionOcrForCurrentPage() {
  if (!currentJobId || !pageList.length) return;
  const pageKey = currentPageKey();
  if (!pageKey) return;
  const sections = getGuideSectionsForPage(pageKey);
  if (!sections.length) {
    alert('No sections available for OCR.');
    return;
  }
  try {
    setSectionOcrBusy(true);
    startSectionOcrProgress(sections.length);
    const res = await fetchAuthed(`/jobs/${currentJobId}/pages/${pageKey}/ocr-sections`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sections,
        guide_state: buildGuideStatePayload(pageKey),
      }),
    });
    const body = await safeParseJson(res);
    if (!res.ok) {
      throw new Error((body && body.detail) || 'Section OCR failed');
    }
    const analyzerMeta = body && body.profile_analyzer ? body.profile_analyzer : null;
    sectionOcrResultsByPage[pageKey] = body || { sections: [] };
    renderSectionOcrResultForPage(pageKey);
    const parsedRowsFromBackend = Array.isArray(body && body.parsed_rows) ? body.parsed_rows : [];
    const parsedBoundsFromBackend = Array.isArray(body && body.parsed_bounds) ? body.parsed_bounds : [];
    const converted = convertSectionOcrToTableRows(pageKey, body || {});
    const finalRows = parsedRowsFromBackend.length ? parsedRowsFromBackend : converted.rows;
    const finalBounds = parsedRowsFromBackend.length ? parsedBoundsFromBackend : converted.bounds;
    if (finalRows.length) {
      rowsByPage[pageKey] = finalRows;
      boundsByPage[pageKey] = finalBounds;
      setActiveEditorRows(finalRows, pageKey);
      activeRowKey = parsedRows.length ? parsedRows[0].row_key : null;
      markActivePageDirty();
      renderCurrentPage();
    } else {
      alert('Section OCR completed, but no table rows were detected from the selected sections.');
    }
    finishSectionOcrProgress(true, Array.isArray(body && body.sections) ? body.sections.length : sections.length);
    if (analyzerMeta) {
      applyAnalyzerMetaToProgress(analyzerMeta, 'tesseract', 'ocr');
    }
  } catch (err) {
    finishSectionOcrProgress(false, 0, err.message || 'Section OCR failed');
    alert(err.message || 'Section OCR failed');
  } finally {
    setSectionOcrBusy(false);
    updateGuideToolButtons();
  }
}

function updateGuideToolButtons() {
  const key = currentPageKey();
  const textOnlyMode = isTextOnlyToolsMode();
  renderOcrToolsToggle();
  if (addHorizontalGuideBtn) {
    addHorizontalGuideBtn.classList.toggle('is-active', activeGuideTool === 'horizontal');
    addHorizontalGuideBtn.disabled = !key || textOnlyMode;
  }
  if (guideUndoBtn) {
    guideUndoBtn.disabled = !key || !canUndoGuides(key) || textOnlyMode;
  }
  if (guideRedoBtn) {
    guideRedoBtn.disabled = !key || !canRedoGuides(key) || textOnlyMode;
  }
  if (previewWrap) {
    previewWrap.classList.toggle('guide-selecting', activeGuideTool !== 'none');
  }
  if (runSectionOcrBtn) {
    runSectionOcrBtn.disabled = sectionOcrInFlight || !pageList.length || !currentJobId || textOnlyMode;
  }
  if (imageToolButtons.length) {
    const disableImageTools = imageToolInFlight || sectionOcrInFlight || !pageList.length || !currentJobId || textOnlyMode;
    imageToolButtons.forEach((btn) => {
      btn.disabled = disableImageTools;
    });
  }
  updateGuideSectionsInfo();
  renderPreviewColumnsRuler();
  renderPreviewRowsRuler();
}

function setActiveGuideTool(mode) {
  const nextMode = (activeGuideTool === mode) ? 'none' : mode;
  activeGuideTool = nextMode;
  if (activeGuideTool !== 'none' && flattenMode) {
    flattenMode = false;
    flattenPoints = [];
    updateFlattenButtons();
  }
  stopPreviewPan();
  updateGuideToolButtons();
  updatePreviewInteractionMode();
}

function clearGuideLinesForCurrentPage() {
  const key = currentPageKey();
  if (!key) return;
  const state = getGuideStateForPage(key, true);
  const before = cloneGuideStateSnapshot(state);
  const columnVertical = computeVerticalLinesFromColumnLayout(getColumnLayoutForPage(key, true));
  const hasHorizontal = before.horizontal.length > 0;
  const sameVertical = guideStatesEqual(
    { vertical: before.vertical, horizontal: [] },
    { vertical: columnVertical, horizontal: [] }
  );
  if (!hasHorizontal && sameVertical) {
    updateGuideToolButtons();
    return;
  }
  pushGuideUndoSnapshot(key, before);
  state.vertical = columnVertical;
  state.horizontal = [];
  markHorizontalGuideTouched(key);
  invalidateGuideDerivedData(key);
  syncColumnLayoutFromGuideState(key);
  updateGuideSectionsInfo();
  updateGuideToolButtons();
  markActivePageDirty();
  drawBoundingBoxes();
}

function addGuideLineForCurrentPage(orientation, position) {
  if (orientation === 'vertical') return false;
  const key = currentPageKey();
  if (!key) return false;
  const state = getGuideStateForPage(key, true);
  const before = cloneGuideStateSnapshot(state);
  const lines = orientation === 'vertical' ? state.vertical : state.horizontal;
  const pos = normalizeGuidePosition(position);
  if (lines.some((v) => Math.abs(v - pos) <= GUIDE_LINE_DUP_TOLERANCE)) {
    return false;
  }
  lines.push(pos);
  const after = cloneGuideStateSnapshot(state);
  if (guideStatesEqual(before, after)) {
    return false;
  }
  applyGuideStateSnapshot(key, after);
  pushGuideUndoSnapshot(key, before);
  markHorizontalGuideTouched(key);
  invalidateGuideDerivedData(key);
  updateGuideSectionsInfo();
  updateGuideToolButtons();
  markActivePageDirty();
  return true;
}

function undoGuideLinesForCurrentPage() {
  const key = currentPageKey();
  if (!key) return false;
  const history = getGuideHistoryForPage(key, false);
  if (!history || !history.undo.length) {
    updateGuideToolButtons();
    return false;
  }
  const current = cloneGuideStateSnapshot(getGuideStateForPage(key, true));
  const previous = history.undo.pop();
  history.redo.push(current);
  if (history.redo.length > GUIDE_HISTORY_LIMIT) {
    history.redo.shift();
  }
  applyGuideStateSnapshot(key, previous);
  syncColumnLayoutFromGuideState(key);
  invalidateGuideDerivedData(key);
  updateGuideSectionsInfo();
  updateGuideToolButtons();
  markActivePageDirty();
  drawBoundingBoxes();
  return true;
}

function redoGuideLinesForCurrentPage() {
  const key = currentPageKey();
  if (!key) return false;
  const history = getGuideHistoryForPage(key, false);
  if (!history || !history.redo.length) {
    updateGuideToolButtons();
    return false;
  }
  const current = cloneGuideStateSnapshot(getGuideStateForPage(key, true));
  const next = history.redo.pop();
  history.undo.push(current);
  if (history.undo.length > GUIDE_HISTORY_LIMIT) {
    history.undo.shift();
  }
  applyGuideStateSnapshot(key, next);
  syncColumnLayoutFromGuideState(key);
  invalidateGuideDerivedData(key);
  updateGuideSectionsInfo();
  updateGuideToolButtons();
  markActivePageDirty();
  drawBoundingBoxes();
  return true;
}

function updatePreviewInteractionMode() {
  if (!previewCanvas) return;
  const canPlaceGuide = activeGuideTool !== 'none' && pageList.length && previewImage.naturalWidth;
  const isFlattenInteractive = flattenMode && !flattenBusy;
  previewCanvas.style.pointerEvents = (canPlaceGuide || isFlattenInteractive) ? 'auto' : 'none';
}

function getNormalizedPreviewPointFromEvent(e) {
  if (!previewImage.naturalWidth || !previewWrap) return null;
  const wrapRect = previewWrap.getBoundingClientRect();
  const baseRect = getRenderedImageRect(previewImage);
  const baseLeft = baseRect.left - wrapRect.left;
  const baseTop = baseRect.top - wrapRect.top;
  const baseW = baseRect.width;
  const baseH = baseRect.height;
  if (baseW <= 0 || baseH <= 0) return null;

  const localX = e.clientX - wrapRect.left;
  const localY = e.clientY - wrapRect.top;
  const cx = baseLeft + (baseW / 2) + previewPanX;
  const cy = baseTop + (baseH / 2) + previewPanY;
  const xOnBase = ((localX - cx) / previewZoom) + (baseW / 2);
  const yOnBase = ((localY - cy) / previewZoom) + (baseH / 2);
  const x = xOnBase / baseW;
  const y = yOnBase / baseH;
  if (x < 0 || x > 1 || y < 0 || y > 1) return null;
  return { x, y };
}

function drawGuideLinesAndSections(ctx, pageKey, drawW, drawH) {
  if (isTextOnlyToolsMode()) return;
  const state = getGuideStateForPage(pageKey, false);
  const vertical = dedupeSortedGuidePositions(state.vertical || []);
  const horizontal = dedupeSortedGuidePositions(state.horizontal || []);
  if (!vertical.length && !horizontal.length) return;

  const xs = dedupeSortedGuidePositions([0, ...vertical, 1]);
  const ys = dedupeSortedGuidePositions([0, ...horizontal, 1]);

  ctx.save();

  // Subtle section tinting to make segmentation zones visible.
  for (let yi = 0; yi < ys.length - 1; yi += 1) {
    for (let xi = 0; xi < xs.length - 1; xi += 1) {
      if ((xi + yi) % 2 !== 0) continue;
      const x1 = xs[xi] * drawW;
      const y1 = ys[yi] * drawH;
      const x2 = xs[xi + 1] * drawW;
      const y2 = ys[yi + 1] * drawH;
      ctx.fillStyle = 'rgba(20, 184, 166, 0.04)';
      ctx.fillRect(x1, y1, Math.max(1, x2 - x1), Math.max(1, y2 - y1));
    }
  }

  ctx.strokeStyle = 'rgba(13, 148, 136, 0.95)';
  ctx.lineWidth = 1;

  vertical.forEach((xNorm) => {
    const x = xNorm * drawW;
    ctx.beginPath();
    ctx.moveTo(x, 0);
    ctx.lineTo(x, drawH);
    ctx.stroke();
  });

  horizontal.forEach((yNorm) => {
    const y = yNorm * drawH;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(drawW, y);
    ctx.stroke();
  });

  ctx.restore();
}

async function doLogin() {
  const email = (authEmail && authEmail.value || '').trim();
  const password = (authPassword && authPassword.value || '').trim();
  if (!email || !password) {
    alert('Enter email and password.');
    return;
  }
  const res = await fetch('/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password }),
  });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Login failed');
    return;
  }
  const nextRole = body.role || '';
  if (nextRole === 'agent' && forcedPageRole === 'credit_evaluator') {
    window.location.href = '/agent';
    return;
  }
  if (nextRole === 'credit_evaluator' && forcedPageRole === 'agent') {
    window.location.href = '/evaluator';
    return;
  }
  if (nextRole === 'admin') {
    window.location.href = '/admin';
    return;
  }
  setAuthState(body.access_token || '', body.role || '', email);
  if (authRole === 'agent') {
    await loadAgentSubmissions();
  } else if (authRole === 'credit_evaluator') {
    await loadEvaluatorSubmissions();
  }
}

function doLogout() {
  authRedirectInProgress = false;
  clearActivePageAutosaveTimer();
  evaluatorSelectedSubmission = null;
  clearEvaluatorManifestTimer();
  clearPreviewBlobCache();
  setAuthState('', '', '');
  setLegacyEditorVisible(false);
  window.location.href = '/login';
}

function renderWorkflowItem(item, rightActionsHtml = '') {
  const title = item.lead_reference || item.borrower_name || item.id;
  const job = item.current_job_id ? `Job: ${item.current_job_id}` : 'No job';
  return `<div class="workflow-item">
    <div class="workflow-item-meta">
      <div class="workflow-item-title">${escapeHtml(title)}</div>
      <div class="workflow-item-sub">Status: ${escapeHtml(item.status || '-')} | ${escapeHtml(job)}</div>
    </div>
    <div class="workflow-item-actions">${rightActionsHtml}</div>
  </div>`;
}

function formatAgentSubmissionDate(value) {
  if (!value) return '-';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return '-';
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function getSubmissionFilename(item) {
  const original = String((item && item.original_filename) || '').trim();
  if (original) return original;
  const key = String((item && item.input_pdf_key) || '').trim();
  if (!key) return '-';
  const parts = key.split('/').filter(Boolean);
  return parts.length ? parts[parts.length - 1] : key;
}

function normalizeBorrowerNameForCompare(value) {
  return String(value || '').trim().replace(/\s+/g, ' ').toLowerCase();
}

function getPageConcurrencyToken(pageStatus) {
  const saved = String((pageStatus && pageStatus.saved_at) || '').trim();
  return saved || null;
}

function pruneSelectedEvaluatorSubmissionIds() {
  const validIds = new Set((evaluatorSubmissionsCache || []).map((item) => String(item.id || '')));
  evaluatorSelectedSubmissionIds.forEach((id) => {
    if (!validIds.has(id)) {
      evaluatorSelectedSubmissionIds.delete(id);
    }
  });
}

function updateEvaluatorCombineButtonState() {
  if (!evaluatorCombineBtn) return;
  const count = evaluatorSelectedSubmissionIds.size;
  evaluatorCombineBtn.disabled = evaluatorCombineInFlight || count < 2;
  evaluatorCombineBtn.textContent = count > 0 ? `Combine Selected (${count})` : 'Combine Selected';
}

function getSelectedEvaluatorSubmissionItems() {
  if (!evaluatorSelectedSubmissionIds.size) return [];
  const mapById = new Map((evaluatorSubmissionsCache || []).map((item) => [String(item.id || ''), item]));
  return Array.from(evaluatorSelectedSubmissionIds)
    .map((id) => mapById.get(String(id)))
    .filter(Boolean);
}

async function combineSelectedEvaluatorSubmissions() {
  const selectedItems = getSelectedEvaluatorSubmissionItems();
  if (selectedItems.length < 2) {
    alert('Select at least two submissions to combine.');
    return;
  }
  const borrowerKeys = selectedItems.map((item) => normalizeBorrowerNameForCompare(item.borrower_name));
  if (borrowerKeys.some((key) => !key) || borrowerKeys.some((key) => key !== borrowerKeys[0])) {
    alert('Combine only supports submissions with the same borrower name.');
    return;
  }

  evaluatorCombineInFlight = true;
  updateEvaluatorCombineButtonState();
  try {
    const selectedIds = selectedItems.map((item) => String(item.id));
    const res = await fetchAuthed('/evaluator/submissions/combine', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ submission_ids: selectedIds }),
    });
    const body = await safeParseJson(res);
    if (!res.ok) {
      alert((body && body.detail) || 'Failed to combine submissions');
      return;
    }
    evaluatorSelectedSubmissionIds = new Set();
    updateEvaluatorCombineButtonState();
    await loadEvaluatorSubmissions();
    if (body && body.submission_id) {
      alert(`Combined successfully. New submission: ${body.submission_id}`);
    } else {
      alert('Combined successfully.');
    }
  } finally {
    evaluatorCombineInFlight = false;
    updateEvaluatorCombineButtonState();
  }
}

function renderAgentSubmissionTable(items) {
  if (!items.length) {
    return '<div class="table-empty">No submissions yet</div>';
  }
  const totalPages = Math.max(1, Math.ceil(items.length / AGENT_SUBMISSIONS_PAGE_SIZE));
  if (agentSubmissionsPage > totalPages) agentSubmissionsPage = totalPages;
  if (agentSubmissionsPage < 1) agentSubmissionsPage = 1;
  const start = (agentSubmissionsPage - 1) * AGENT_SUBMISSIONS_PAGE_SIZE;
  const pageItems = items.slice(start, start + AGENT_SUBMISSIONS_PAGE_SIZE);

  const rowHtml = pageItems.map((item) => {
    return `<tr>
      <td>${escapeHtml(formatAgentSubmissionDate(item.created_at))}</td>
      <td>${escapeHtml(getSubmissionFilename(item))}</td>
      <td>${escapeHtml(item.borrower_name || '-')}</td>
      <td>${escapeHtml(item.lead_reference || '-')}</td>
      <td class="agent-submission-id" title="${escapeHtml(item.id || '-')}">${escapeHtml(item.id || '-')}</td>
    </tr>`;
  });
  const fillerCount = Math.max(0, AGENT_SUBMISSIONS_PAGE_SIZE - pageItems.length);
  for (let i = 0; i < fillerCount; i += 1) {
    rowHtml.push('<tr class="agent-row-filler"><td colspan="5"></td></tr>');
  }
  const rows = rowHtml.join('');

  const pages = Array.from({ length: totalPages }, (_, idx) => idx + 1);
  const pageButtons = pages.map((p) => {
    const active = p === agentSubmissionsPage ? ' is-active' : '';
    return `<button class="agent-page-btn${active}" data-page="${p}">${p}</button>`;
  }).join('');

  return `<div class="agent-submissions-table-inner">
    <table class="agent-submissions-grid agent-submissions-grid-agent">
      <thead>
        <tr>
          <th>Date</th>
          <th>Filename</th>
          <th>Borrower Name</th>
          <th>Lead Reference</th>
          <th>Submission ID</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
    <div class="agent-pagination">
      <button class="agent-page-btn" data-page-nav="prev" ${agentSubmissionsPage <= 1 ? 'disabled' : ''}>Prev</button>
      <div class="agent-page-list">${pageButtons}</div>
      <button class="agent-page-btn" data-page-nav="next" ${agentSubmissionsPage >= totalPages ? 'disabled' : ''}>Next</button>
    </div>
  </div>`;
}

function renderAgentSubmissionTableFiltered() {
  const query = (agentSearchInput && agentSearchInput.value || '').trim().toLowerCase();
  if (!query) {
    agentSubmissionList.innerHTML = renderAgentSubmissionTable(agentSubmissionsCache);
    return;
  }
  const filtered = agentSubmissionsCache.filter((item) => {
    const borrower = String(item.borrower_name || '').toLowerCase();
    return borrower.includes(query);
  });
  agentSubmissionList.innerHTML = renderAgentSubmissionTable(filtered);
}

function renderEvaluatorQueueRow(item) {
  const title = item.lead_reference || item.borrower_name || item.id;
  const status = String(item.status || '-');
  const statusLabel = formatSubmissionStatus(status);
  const statusClass = `workflow-status workflow-status-${status.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
  const jobShort = item.current_job_id ? item.current_job_id.slice(0, 8) : '-';
  const assigned = Boolean(item.assigned_evaluator_id);
  const assignBtn = assigned ? '' : `<button class="preview-nav workflow-mini-btn" data-action="assign" data-id="${item.id}">Assign</button>`;
  const openBtn = assigned ? `<button class="preview-nav workflow-mini-btn" data-action="open" data-id="${item.id}">Open</button>` : '';
  return `<div class="workflow-queue-row">
    <div class="workflow-queue-col workflow-queue-col-title" title="${escapeHtml(item.id)}">${escapeHtml(title)}</div>
    <div class="workflow-queue-col"><span class="${statusClass}">${escapeHtml(statusLabel)}</span></div>
    <div class="workflow-queue-col workflow-queue-col-job" title="${escapeHtml(item.current_job_id || '-')}">${escapeHtml(jobShort)}</div>
    <div class="workflow-queue-col workflow-item-actions">${assignBtn}${openBtn}</div>
  </div>`;
}

function renderEvaluatorSubmissionTable(items) {
  if (!items.length) {
    return '<div class="table-empty">No submissions in queue</div>';
  }
  const totalPages = Math.max(1, Math.ceil(items.length / EVALUATOR_SUBMISSIONS_PAGE_SIZE));
  if (evaluatorSubmissionsPage > totalPages) evaluatorSubmissionsPage = totalPages;
  if (evaluatorSubmissionsPage < 1) evaluatorSubmissionsPage = 1;
  const start = (evaluatorSubmissionsPage - 1) * EVALUATOR_SUBMISSIONS_PAGE_SIZE;
  const pageItems = items.slice(start, start + EVALUATOR_SUBMISSIONS_PAGE_SIZE);

  const rowHtml = pageItems.map((item) => {
    const status = String(item.status || '-');
    const statusLabel = formatSubmissionStatus(status);
    const statusClass = `workflow-status workflow-status-${status.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
    const assignBtn = item.assigned_evaluator_id ? '' : `<button class="preview-nav workflow-mini-btn" data-action="assign" data-id="${item.id}">Assign</button>`;
    const openBtn = item.assigned_evaluator_id ? `<button class="preview-nav workflow-mini-btn" data-action="open" data-id="${item.id}">Open</button>` : '';
    const filename = getSubmissionFilename(item);
    const checked = evaluatorSelectedSubmissionIds.has(String(item.id || '')) ? ' checked' : '';
    return `<tr>
      <td><input type="checkbox" class="evaluator-select-checkbox" data-submission-id="${escapeHtml(String(item.id || ''))}"${checked}></td>
      <td>${escapeHtml(formatAgentSubmissionDate(item.created_at))}</td>
      <td class="submission-filename-cell" title="${escapeHtml(filename)}">${escapeHtml(filename)}</td>
      <td title="${escapeHtml(item.borrower_name || '-')}">${escapeHtml(item.borrower_name || '-')}</td>
      <td class="agent-submission-id" title="${escapeHtml(item.id || '-')}">${escapeHtml(item.id || '-')}</td>
      <td><span class="${statusClass}">${escapeHtml(statusLabel)}</span></td>
      <td class="workflow-item-actions">${assignBtn}${openBtn}</td>
    </tr>`;
  });
  const fillerCount = Math.max(0, EVALUATOR_SUBMISSIONS_PAGE_SIZE - pageItems.length);
  for (let i = 0; i < fillerCount; i += 1) {
    rowHtml.push('<tr class="agent-row-filler"><td colspan="7"></td></tr>');
  }
  const rows = rowHtml.join('');

  const pages = Array.from({ length: totalPages }, (_, idx) => idx + 1);
  const pageButtons = pages.map((p) => {
    const active = p === evaluatorSubmissionsPage ? ' is-active' : '';
    return `<button class="agent-page-btn${active}" data-evaluator-page="${p}">${p}</button>`;
  }).join('');

  return `<div class="agent-submissions-table-inner">
    <table class="agent-submissions-grid agent-submissions-grid-evaluator">
      <thead>
        <tr>
          <th>Select</th>
          <th>Date</th>
          <th>Filename</th>
          <th>Borrower Name</th>
          <th>Submission ID</th>
          <th>Status</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
    <div class="agent-pagination">
      <button class="agent-page-btn" data-evaluator-page-nav="prev" ${evaluatorSubmissionsPage <= 1 ? 'disabled' : ''}>Prev</button>
      <div class="agent-page-list">${pageButtons}</div>
      <button class="agent-page-btn" data-evaluator-page-nav="next" ${evaluatorSubmissionsPage >= totalPages ? 'disabled' : ''}>Next</button>
    </div>
  </div>`;
}

function renderEvaluatorSubmissionTableFiltered() {
  const query = (evaluatorSearchInput && evaluatorSearchInput.value || '').trim().toLowerCase();
  if (!query) {
    evaluatorSubmissionList.innerHTML = renderEvaluatorSubmissionTable(evaluatorSubmissionsCache);
    updateEvaluatorCombineButtonState();
    return;
  }
  const filtered = evaluatorSubmissionsCache.filter((item) => {
    const borrower = String(item.borrower_name || '').toLowerCase();
    return borrower.includes(query);
  });
  evaluatorSubmissionList.innerHTML = renderEvaluatorSubmissionTable(filtered);
  updateEvaluatorCombineButtonState();
}

async function loadAgentSubmissions() {
  if (!agentSubmissionList || !authToken) return;
  const res = await fetchAuthed('/agent/submissions');
  const body = await safeParseJson(res);
  if (!res.ok) {
    agentSubmissionList.innerHTML = `<div class="table-empty">${escapeHtml((body && body.detail) || 'Failed to load submissions')}</div>`;
    return;
  }
  agentSubmissionsCache = (body && body.items) || [];
  renderAgentSubmissionTableFiltered();
}

function getAgentSelectedFiles() {
  return agentFileInput && agentFileInput.files ? Array.from(agentFileInput.files) : [];
}

function updateAgentSubmitButtonState() {
  if (!agentSubmitBtn) return;
  if (isAgentSubmitting) return;
  const count = getAgentSelectedFiles().length;
  const borrowerName = agentBorrowerName && agentBorrowerName.value ? agentBorrowerName.value.trim() : '';
  agentSubmitBtn.disabled = count === 0 || !borrowerName;
  agentSubmitBtn.textContent = count > 0 ? `Submit Files (${count})` : 'Submit Files (0)';
}

function setAgentUploadProgress(percent, label) {
  const safe = Math.max(0, Math.min(100, Math.round(percent)));
  if (agentUploadProgressFill) agentUploadProgressFill.style.width = `${safe}%`;
  if (agentUploadProgressPercent) agentUploadProgressPercent.textContent = `${safe}%`;
  if (agentUploadProgressLabel && label) agentUploadProgressLabel.textContent = label;
}

function setAgentSubmittingState(active) {
  isAgentSubmitting = active;
  if (agentSubmitBtn) {
    const borrowerName = agentBorrowerName && agentBorrowerName.value ? agentBorrowerName.value.trim() : '';
    agentSubmitBtn.disabled = active || getAgentSelectedFiles().length === 0 || !borrowerName;
    agentSubmitBtn.classList.toggle('is-loading', active);
    if (active) {
      agentSubmitBtn.textContent = 'Submitting';
    } else {
      updateAgentSubmitButtonState();
    }
  }
  if (agentUploadProgressWrap) {
    agentUploadProgressWrap.classList.toggle('is-hidden', !active);
  }
  if (!active) {
    setAgentUploadProgress(0, 'Uploading...');
  }
}

function uploadAgentFile(file, parseMode, borrowerName, leadReference, onProgress) {
  return new Promise((resolve, reject) => {
    const fd = new FormData();
    fd.append('file', file);
    fd.append('mode', parseMode);
    if (borrowerName) fd.append('borrower_name', borrowerName);
    if (leadReference) fd.append('lead_reference', leadReference);

    const xhr = new XMLHttpRequest();
    xhr.open('POST', '/agent/submissions');
    if (authToken) xhr.setRequestHeader('Authorization', `Bearer ${authToken}`);
    xhr.upload.onprogress = (e) => {
      if (!e.lengthComputable) return;
      const ratio = e.total > 0 ? (e.loaded / e.total) : 0;
      if (onProgress) onProgress(Math.max(0, Math.min(1, ratio)));
    };
    xhr.onerror = () => reject(new Error('Network error during upload'));
    xhr.onload = () => {
      let body = null;
      try {
        body = xhr.responseText ? JSON.parse(xhr.responseText) : null;
      } catch (_err) {
        body = null;
      }
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(body || {});
      } else {
        if (xhr.status === 401) {
          const detail = String((body && body.detail) || '').trim().toLowerCase();
          if (
            detail === 'token_expired' ||
            detail === 'invalid_token' ||
            detail === 'invalid_token_signature' ||
            detail === 'missing_auth_token'
          ) {
            handleAuthExpired();
          }
        }
        reject(new Error((body && body.detail) || `Upload failed for ${file.name}`));
      }
    };
    xhr.send(fd);
  });
}

function setAgentSelectedFiles(files) {
  if (!agentFileInput) return;
  const dt = new DataTransfer();
  files.forEach((f) => dt.items.add(f));
  agentFileInput.files = dt.files;
}

function renderAgentSelectedFiles() {
  if (!agentSelectedFiles) {
    updateAgentSubmitButtonState();
    return;
  }
  const files = getAgentSelectedFiles();
  if (!files.length) {
    agentSelectedFiles.innerHTML = '<div class="agent-files-empty">No file selected</div>';
    updateAgentSubmitButtonState();
    return;
  }
  agentSelectedFiles.innerHTML = files.map((file, idx) => `
    <div class="agent-file-item">
      <i class="far fa-file-pdf"></i>
      <div class="agent-file-meta">
        <div class="agent-file-name">${escapeHtml(file.name)}</div>
        <div class="agent-file-size">${formatFileSize(file.size)}</div>
      </div>
      <button class="agent-file-remove" data-index="${idx}" aria-label="Remove file">×</button>
    </div>
  `).join('');
  updateAgentSubmitButtonState();
}

async function submitAgentFile() {
  const files = getAgentSelectedFiles();
  if (!files.length) {
    alert('Select at least one PDF first.');
    return;
  }

  const borrowerName = agentBorrowerName && agentBorrowerName.value.trim() ? agentBorrowerName.value.trim() : '';
  if (!borrowerName) {
    alert('Borrower name is required.');
    return;
  }
  const leadReference = agentLeadReference && agentLeadReference.value.trim() ? agentLeadReference.value.trim() : '';
  const parseMode = getSelectedParseMode();

  setAgentSubmittingState(true);
  let submitted = 0;
  try {
    const total = files.length;
    for (let i = 0; i < total; i += 1) {
      const file = files[i];
      await uploadAgentFile(file, parseMode, borrowerName, leadReference, (singleRatio) => {
        const overall = ((i + singleRatio) / total) * 100;
        setAgentUploadProgress(overall, `Uploading ${i + 1}/${total}`);
      });
      submitted += 1;
      setAgentUploadProgress(((i + 1) / total) * 100, `Uploading ${i + 1}/${total}`);
    }
  } catch (err) {
    setAgentSubmittingState(false);
    alert(err.message || 'Submission failed');
    return;
  }

  agentFileInput.value = '';
  renderAgentSelectedFiles();
  if (agentBorrowerName) agentBorrowerName.value = '';
  if (agentLeadReference) agentLeadReference.value = '';
  setAgentSubmittingState(false);
  await loadAgentSubmissions();
  alert(`Submitted ${submitted} file(s).`);
}

async function loadEvaluatorSubmissions() {
  if (!evaluatorSubmissionList || !authToken) return;
  const res = await fetchAuthed('/evaluator/submissions?include_unassigned=true');
  const body = await safeParseJson(res);
  if (!res.ok) {
    evaluatorSubmissionList.innerHTML = `<div class="table-empty">${escapeHtml((body && body.detail) || 'Failed to load queue')}</div>`;
    return;
  }
  evaluatorSubmissionsCache = (body && body.items) || [];
  pruneSelectedEvaluatorSubmissionIds();
  renderEvaluatorSubmissionTableFiltered();
}

async function assignEvaluatorSubmission(submissionId) {
  const res = await fetchAuthed(`/evaluator/submissions/${submissionId}/assign`, { method: 'POST' });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Assign failed');
    return;
  }
  await loadEvaluatorSubmissions();
}

function normalizePageRowForEditor(row, pageKey, idx) {
  const rowId = String(row.row_id || row.row_index || idx + 1).padStart(3, '0');
  const mapped = {
    row_key: createRowKey(),
    global_row_id: rowId,
    row_id: rowId,
    date: row.date || '',
    description: row.description || '',
    debit: row.debit != null ? String(row.debit) : '',
    credit: row.credit != null ? String(row.credit) : '',
    balance: row.balance != null ? String(row.balance) : '',
    page: pageKey,
    page_row_id: rowId,
    x1: row.x1,
    y1: row.y1,
    x2: row.x2,
    y2: row.y2,
  };
  normalizeRowDisplayValues(mapped);
  return mapped;
}

function toFiniteNumberOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeBoundRowId(value, fallbackIndex = 1) {
  const raw = String(value != null ? value : fallbackIndex).trim();
  if (!raw) return String(fallbackIndex).padStart(3, '0');
  if (/^\d+$/.test(raw)) return raw.padStart(3, '0');
  return raw;
}

function findBoundForRowOnPage(pageKey, row, rowIndex) {
  const bounds = Array.isArray(boundsByPage[pageKey]) ? boundsByPage[pageKey] : [];
  if (bounds.length) {
    const candidateIds = new Set();
    [row && row.page_row_id, row && row.row_id, row && row.global_row_id].forEach((value) => {
      const token = normalizeBoundRowId(value, rowIndex + 1);
      if (token) candidateIds.add(token);
    });

    for (let idx = 0; idx < bounds.length; idx += 1) {
      const entry = bounds[idx];
      const boundId = normalizeBoundRowId(entry && entry.row_id, idx + 1);
      if (candidateIds.has(boundId)) return entry;
      const mapped = normalizeBoundRowId(pageRowToGlobal[`${pageKey}|${boundId}`], idx + 1);
      if (candidateIds.has(mapped)) return entry;
    }
    if (rowIndex >= 0 && rowIndex < bounds.length) {
      return bounds[rowIndex];
    }
  }
  const derived = deriveRowBoxFromGuideState(pageKey, row, rowIndex);
  if (derived) {
    return {
      row_id: normalizeBoundRowId(row && (row.page_row_id || row.row_id || row.global_row_id), rowIndex + 1),
      ...derived,
    };
  }
  return null;
}

function findActiveBoundForRow(pageKey, activeRow, bounds) {
  if (!activeRow || !Array.isArray(bounds) || !bounds.length) return null;

  const candidateIds = new Set();
  [activeRow.page_row_id, activeRow.row_id, activeRow.global_row_id].forEach((value) => {
    const token = normalizeBoundRowId(value, 1);
    if (token) candidateIds.add(token);
  });
  if (!candidateIds.size) return null;

  for (let idx = 0; idx < bounds.length; idx += 1) {
    const entry = bounds[idx];
    const boundId = normalizeBoundRowId(entry && entry.row_id, idx + 1);
    if (candidateIds.has(boundId)) return entry;
    const mapped = normalizeBoundRowId(pageRowToGlobal[`${pageKey}|${boundId}`], idx + 1);
    if (candidateIds.has(mapped)) return entry;
  }

  // Fallback: if ids drift after autosave/reload, keep visual mapping by row order.
  const rowIndex = parsedRows.findIndex((row) => row && row.row_key === activeRow.row_key);
  if (rowIndex >= 0 && rowIndex < bounds.length) {
    return bounds[rowIndex];
  }
  return null;
}

function normalizeRowBoxToUnit(box, naturalW, naturalH) {
  if (!box || typeof box !== 'object') return null;
  const x1Raw = Number(box.x1);
  const y1Raw = Number(box.y1);
  const x2Raw = Number(box.x2);
  const y2Raw = Number(box.y2);
  if (![x1Raw, y1Raw, x2Raw, y2Raw].every(Number.isFinite)) return null;

  let x1 = x1Raw;
  let y1 = y1Raw;
  let x2 = x2Raw;
  let y2 = y2Raw;

  const looksUnit =
    x1 >= -0.01 && y1 >= -0.01 && x2 <= 1.01 && y2 <= 1.01;

  if (!looksUnit) {
    if (!Number.isFinite(naturalW) || !Number.isFinite(naturalH) || naturalW <= 0 || naturalH <= 0) {
      return null;
    }
    x1 /= naturalW;
    x2 /= naturalW;
    y1 /= naturalH;
    y2 /= naturalH;
  }

  const nx1 = clamp(Math.min(x1, x2), 0, 1);
  const nx2 = clamp(Math.max(x1, x2), 0, 1);
  const ny1 = clamp(Math.min(y1, y2), 0, 1);
  const ny2 = clamp(Math.max(y1, y2), 0, 1);
  if ((nx2 - nx1) < 0.0005 || (ny2 - ny1) < 0.0005) return null;
  return { x1: nx1, y1: ny1, x2: nx2, y2: ny2 };
}

function getRowIndexHint(row, fallbackIndex = 0) {
  const fallback = Number.isInteger(fallbackIndex) ? fallbackIndex : Number(fallbackIndex || 0);
  const values = [row && row.page_row_id, row && row.row_id, row && row.global_row_id];
  for (let i = 0; i < values.length; i += 1) {
    const raw = String(values[i] != null ? values[i] : '').trim();
    if (!/^\d+$/.test(raw)) continue;
    const idx = Number(raw) - 1;
    if (Number.isInteger(idx) && idx >= 0) {
      return idx;
    }
  }
  return Number.isInteger(fallback) && fallback >= 0 ? fallback : 0;
}

function deriveRowBoxFromGuideState(pageKey, row, rowIndex = 0) {
  if (!pageKey) return null;
  const state = getGuideStateForPage(pageKey, false);
  if (!state || !Array.isArray(state.horizontal) || !state.horizontal.length) return null;
  const bands = dedupeSortedGuidePositions([0, ...state.horizontal, 1]);
  if (bands.length < 2) return null;
  const idx = clamp(getRowIndexHint(row, rowIndex), 0, Math.max(0, bands.length - 2));
  const y1 = Number(bands[idx]);
  const y2 = Number(bands[idx + 1]);
  if (!Number.isFinite(y1) || !Number.isFinite(y2) || (y2 - y1) < 0.0005) return null;
  return { x1: 0, y1, x2: 1, y2 };
}

function synthesizeBoundsFromGuideState(pageKey, rows) {
  const out = [];
  (Array.isArray(rows) ? rows : []).forEach((row, idx) => {
    const box = deriveRowBoxFromGuideState(pageKey, row, idx);
    if (!box) return;
    const rowId = normalizeBoundRowId(row && (row.page_row_id || row.row_id || row.global_row_id), idx + 1);
    out.push({ row_id: rowId, ...box });
  });
  return out;
}

function resolveActiveRowBox(pageKey, activeRow) {
  if (!activeRow) return null;
  const bounds = Array.isArray(boundsByPage[pageKey]) ? boundsByPage[pageKey] : [];
  const boundFromMap = findActiveBoundForRow(pageKey, activeRow, bounds);
  const activeIdx = parsedRows.findIndex((row) => row && row.row_key === activeRow.row_key);
  const derivedFromGuides = deriveRowBoxFromGuideState(pageKey, activeRow, activeIdx >= 0 ? activeIdx : 0);
  const naturalW = previewImage && previewImage.naturalWidth ? previewImage.naturalWidth : 0;
  const naturalH = previewImage && previewImage.naturalHeight ? previewImage.naturalHeight : 0;
  return (
    normalizeRowBoxToUnit(boundFromMap, naturalW, naturalH)
    || normalizeRowBoxToUnit(activeRow, naturalW, naturalH)
    || normalizeRowBoxToUnit(derivedFromGuides, naturalW, naturalH)
  );
}

function mergeBoundsWithRowGeometry(rows, bounds) {
  const out = [];
  const seen = new Set();

  (Array.isArray(bounds) ? bounds : []).forEach((entry, idx) => {
    const rowId = normalizeBoundRowId(entry && entry.row_id, idx + 1);
    const x1 = toFiniteNumberOrNull(entry && entry.x1);
    const y1 = toFiniteNumberOrNull(entry && entry.y1);
    const x2 = toFiniteNumberOrNull(entry && entry.x2);
    const y2 = toFiniteNumberOrNull(entry && entry.y2);
    if (x1 == null || y1 == null || x2 == null || y2 == null) return;
    out.push({ row_id: rowId, x1, y1, x2, y2 });
    seen.add(rowId);
  });

  (Array.isArray(rows) ? rows : []).forEach((row, idx) => {
    const rowId = normalizeBoundRowId(row && (row.row_id || row.page_row_id), idx + 1);
    if (seen.has(rowId)) return;
    const x1 = toFiniteNumberOrNull(row && row.x1);
    const y1 = toFiniteNumberOrNull(row && row.y1);
    const x2 = toFiniteNumberOrNull(row && row.x2);
    const y2 = toFiniteNumberOrNull(row && row.y2);
    if (x1 == null || y1 == null || x2 == null || y2 == null) return;
    out.push({ row_id: rowId, x1, y1, x2, y2 });
    seen.add(rowId);
  });

  return out;
}

function setActiveEditorRows(rows, pageKey) {
  parsedRows = (rows || []).map((row, idx) => normalizePageRowForEditor(row, pageKey, idx));
  rowKeyCounter = 1;
  parsedRows.forEach((row) => {
    row.row_key = createRowKey();
  });
  pageRowToGlobal = {};
  parsedRows.forEach((row) => {
    pageRowToGlobal[`${pageKey}|${row.page_row_id}`] = row.global_row_id;
  });
  renderRows(parsedRows);
  if (parsedRows.length) {
    activeRowKey = parsedRows[0].row_key;
    highlightSelectedTableRow();
  } else {
    activeRowKey = null;
  }
}

function updateSummaryFromSnapshot(summary) {
  if (!summary || typeof summary !== 'object') {
    totalTransactions.textContent = '0';
    totalDebitTransactions.textContent = '0';
    totalCreditTransactions.textContent = '0';
    endingBalance.textContent = '-';
    renderMonthlySummary([]);
    return;
  }
  totalTransactions.textContent = String(summary.total_transactions || 0);
  totalDebitTransactions.textContent = String(summary.debit_transactions || 0);
  totalCreditTransactions.textContent = String(summary.credit_transactions || 0);
  endingBalance.textContent = Number.isFinite(Number(summary.adb))
    ? formatPesoValue(Math.abs(Number(summary.adb)), true)
    : '-';

  const monthlySummaryBody = document.getElementById('monthlySummaryBody');
  const monthlySummaryWrap = document.querySelector('.monthly-summary-wrap');
  const monthly = Array.isArray(summary.monthly) ? summary.monthly : [];
  if (!monthlySummaryBody) return;
  if (!monthly.length) {
    if (monthlySummaryWrap) monthlySummaryWrap.classList.add('is-empty');
    monthlySummaryBody.innerHTML = '<tr><td class="monthly-empty" colspan="6">No monthly data</td></tr>';
    return;
  }
  if (monthlySummaryWrap) monthlySummaryWrap.classList.remove('is-empty');
  monthlySummaryBody.innerHTML = monthly.map((item) => (
    `<tr>
      <td>${escapeHtml(String(item.month || item.monthLabel || '-'))}</td>
      <td>${escapeHtml(formatPesoValue(Math.abs(Number(item.debit || 0)), true))}</td>
      <td>${escapeHtml(formatPesoValue(Math.abs(Number(item.credit || 0)), true))}</td>
      <td>${escapeHtml(formatPesoValue(Math.abs(Number(item.avg_debit || item.avgDebit || 0)), true))}</td>
      <td>${escapeHtml(formatPesoValue(Math.abs(Number(item.avg_credit || item.avgCredit || 0)), true))}</td>
      <td>${escapeHtml(formatPesoValue(Number(item.adb || 0), true))}</td>
    </tr>`
  )).join('');
}

async function refreshSummarySnapshot() {
  if (!evaluatorSelectedSubmission || !evaluatorSelectedSubmission.id) return;
  const res = await fetchAuthed(`/evaluator/submissions/${evaluatorSelectedSubmission.id}`);
  const body = await safeParseJson(res);
  if (!res.ok) return;
  const summary = body && body.summary ? body.summary : null;
  updateSummaryFromSnapshot(summary);
}

async function loadPageManifest() {
  if (!evaluatorSelectedSubmission || !evaluatorSelectedSubmission.id) return false;
  const res = await fetchAuthed(`/evaluator/submissions/${evaluatorSelectedSubmission.id}/pages`);
  const body = await safeParseJson(res);
  if (!res.ok) {
    const detail = (body && body.detail) ? String(body.detail) : `HTTP ${res.status}`;
    throw new Error(`Page manifest failed: ${detail}`);
  }
  evaluatorManifest = Array.isArray(body.pages) ? body.pages : [];
  evaluatorReviewProgress = body.review_progress || { total_pages: evaluatorManifest.length, parsed_pages: 0, reviewed_pages: 0, percent: 0 };
  evaluatorCanExport = Boolean(body.can_export);
  setSummaryLocked(!evaluatorCanExport);
  if (evaluatorCanExport) {
    await refreshSummarySnapshot();
  }

  pageList = evaluatorManifest.map((p) => `${p.page_key}.png`);
  if (!pageList.length) {
    activePageReviewStatus = 'pending';
    renderActivePageSavedMark();
    currentPageIndex = 0;
    renderCurrentPage();
    return true;
  }
  if (currentPageIndex >= pageList.length) {
    currentPageIndex = pageList.length - 1;
  }
  syncPageSelect();
  return true;
}

async function openEvaluatorSubmissionLegacyFallback(submission) {
  if (!submission || !submission.current_job_id) return;
  currentJobId = submission.current_job_id;
  const statusRes = await fetchAuthed(`/jobs/${currentJobId}`);
  const statusBody = await safeParseJson(statusRes);
  if (statusRes.ok && statusBody && statusBody.status === 'done') {
    updateProgressUI(
      100,
      'Results ready',
      statusBody.step || 'completed',
      statusBody.ocr_backend,
      statusBody.parse_mode,
      statusBody
    );
    stopElapsedTimer();
    await loadResults();
    return;
  }
  if (statusRes.ok && statusBody && statusBody.status === 'failed') {
    const failedStep = statusBody.step || statusBody.status || 'failed';
    const failedProgress = Number.isFinite(statusBody.progress) ? statusBody.progress : inferProgress(statusBody.status, failedStep);
    updateProgressUI(
      failedProgress,
      statusBody.message || 'OCR job failed',
      failedStep,
      statusBody.ocr_backend,
      statusBody.parse_mode,
      statusBody
    );
    stopElapsedTimer();
    alert('Job failed. Check diagnostics.');
    return;
  }
  if (statusRes.ok && statusBody && statusBody.status === 'processing') {
    startElapsedTimer();
    try {
      await pollJobUntilDone();
    } finally {
      stopElapsedTimer();
    }
    return;
  }
  const startRes = await fetchAuthed(`/jobs/${currentJobId}/start`, { method: 'POST' });
  const startBody = await safeParseJson(startRes);
  if (!startRes.ok) {
    throw new Error((startBody && startBody.detail) || 'Failed to start processing');
  }
  await loadEvaluatorSubmissions();
  updateProgressUI(2, 'Processing started', 'processing');
  startElapsedTimer();
  try {
    await pollJobUntilDone();
  } finally {
    stopElapsedTimer();
  }
}

async function loadActivePage(pageKey) {
  if (!evaluatorSelectedSubmission || !evaluatorSelectedSubmission.id) return;
  clearActivePageAutosaveTimer();
  const key = pageKey || currentPageKey();
  if (!key) return;
  const res = await fetchAuthed(`/evaluator/submissions/${evaluatorSelectedSubmission.id}/pages/${key}`);
  const body = await safeParseJson(res);
  if (!res.ok) {
    activePageKey = key;
    activePageReviewStatus = 'pending';
    activePageDirty = false;
    activePageDirtyRevision = 0;
    renderActivePageSavedMark();
    return;
  }
  const incomingRows = Array.isArray(body.rows) ? body.rows : [];
  const incomingBounds = Array.isArray(body.bounds) ? body.bounds : [];
  rowsByPage[key] = incomingRows;
  applyGuideStatePayload(key, body.guide_state || {});
  const mergedBounds = mergeBoundsWithRowGeometry(incomingRows, incomingBounds);
  boundsByPage[key] = mergedBounds.length ? mergedBounds : synthesizeBoundsFromGuideState(key, incomingRows);
  identityBoundsByPage[key] = Array.isArray(body.identity_bounds) ? body.identity_bounds : [];
  activePageUpdatedAt = getPageConcurrencyToken(body.page_status);
  activePageReviewStatus = body.page_status && body.page_status.review_status ? String(body.page_status.review_status) : 'pending';
  activePageKey = key;
  activePageDirty = false;
  activePageDirtyRevision = 0;
  renderActivePageSavedMark();
  setActiveEditorRows(incomingRows, key);
  renderCurrentPage();
}

async function saveActivePageIfDirty(options = {}) {
  const opts = { silent: false, ...options };
  if (!evaluatorSelectedSubmission || !evaluatorSelectedSubmission.id) return true;
  const pageKey = activePageKey || currentPageKey();
  if (!pageKey) return true;
  const status = String(activePageReviewStatus || '').toLowerCase();
  const alreadySaved = status === 'saved' || status === 'reviewed';
  if (!activePageDirty && alreadySaved) return true;
  if (activePageSavePromise) {
    return activePageSavePromise;
  }
  clearActivePageAutosaveTimer();
  const doSave = async () => {
    const saveRevision = activePageDirtyRevision;
    const saveKey = pageKey;
    const payloadRows = parsedRows.map((row, idx) => ({
      ...(function buildRowBox() {
        const direct = normalizeRowBoxToUnit(row, 1, 1);
        if (direct) {
          return {
            x1: direct.x1,
            y1: direct.y1,
            x2: direct.x2,
            y2: direct.y2,
          };
        }
        const fallback = findBoundForRowOnPage(pageKey, row, idx);
        const normalized = normalizeRowBoxToUnit(fallback, 1, 1);
        if (normalized) {
          return {
            x1: normalized.x1,
            y1: normalized.y1,
            x2: normalized.x2,
            y2: normalized.y2,
          };
        }
        return { x1: row.x1, y1: row.y1, x2: row.x2, y2: row.y2 };
      }()),
      row_id: row.page_row_id || row.row_id || String(idx + 1).padStart(3, '0'),
      page: pageKey,
      date: row.date || '',
      description: row.description || '',
      debit: row.debit || '',
      credit: row.credit || '',
      balance: row.balance || '',
    }));
    const res = await fetchAuthed(`/evaluator/submissions/${evaluatorSelectedSubmission.id}/pages/${pageKey}/transactions`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        rows: payloadRows,
        expected_updated_at: activePageUpdatedAt,
        guide_state: buildGuideStatePayload(pageKey),
      }),
    });
    const body = await safeParseJson(res);
    if (!res.ok) {
      const detail = (body && body.detail) || 'Failed to auto-save page';
      if (opts.silent) {
        console.warn(detail);
      } else {
        alert(detail);
      }
      return false;
    }
    activePageUpdatedAt = getPageConcurrencyToken(body.page_status) || activePageUpdatedAt;
    if (String(activePageKey || '') === String(saveKey || '') && activePageDirtyRevision === saveRevision) {
      activePageDirty = false;
    } else {
      activePageDirty = true;
    }
    activePageReviewStatus = body.page_status && body.page_status.review_status ? String(body.page_status.review_status) : activePageReviewStatus;
    if (pageKey && body.page_status) {
      const idx = evaluatorManifest.findIndex((p) => String(p.page_key || '') === String(pageKey));
      if (idx >= 0) {
        evaluatorManifest[idx] = {
          ...evaluatorManifest[idx],
          review_status: body.page_status.review_status || evaluatorManifest[idx].review_status,
          parse_status: body.page_status.parse_status || evaluatorManifest[idx].parse_status,
          saved_at: body.page_status.saved_at || evaluatorManifest[idx].saved_at,
          updated_at: body.page_status.updated_at || evaluatorManifest[idx].updated_at,
          rows_count: Number.isFinite(body.page_status.rows_count) ? body.page_status.rows_count : evaluatorManifest[idx].rows_count,
          has_unsaved: Boolean(body.page_status.has_unsaved),
        };
      }
    }
    renderActivePageSavedMark();
    if (activePageDirty) {
      scheduleActivePageAutosave();
    }
    evaluatorCanExport = Boolean(body.can_export);
    setSummaryLocked(!evaluatorCanExport);
    if (body.summary && evaluatorCanExport) {
      updateSummaryFromSnapshot(body.summary);
    }
    return true;
  };
  activePageSaveInFlight = true;
  activePageSavePromise = doSave();
  try {
    return await activePageSavePromise;
  } finally {
    activePageSaveInFlight = false;
    activePageSavePromise = null;
  }
}

async function switchToPageByIndex(nextIndex) {
  if (!pageList.length) return;
  const bounded = Math.max(0, Math.min(pageList.length - 1, nextIndex));
  if (bounded === currentPageIndex && activePageKey) return;
  clearActivePageAutosaveTimer();
  const sourcePageKey = activePageKey || currentPageKey();
  const ok = await saveActivePageIfDirty();
  if (!ok) return;
  if (String(activePageKey || currentPageKey() || '') === String(sourcePageKey || '') && activePageDirty) {
    const secondPassOk = await saveActivePageIfDirty();
    if (!secondPassOk || activePageDirty) return;
  }
  currentPageIndex = bounded;
  const key = currentPageKey();
  await loadActivePage(key);
}

async function triggerBackgroundParseForNextPendingPage() {
  if (!evaluatorSelectedSubmission || !evaluatorSelectedSubmission.id) return;
  const target = evaluatorManifest.find((page) => String(page.parse_status || '').toLowerCase() === 'pending');
  if (!target) return;
  try {
    await fetchAuthed(`/evaluator/submissions/${evaluatorSelectedSubmission.id}/pages/${target.page_key}/parse`, { method: 'POST' });
  } catch (_err) {
    // Non-blocking by design.
  }
}

async function openEvaluatorSubmission(submissionId) {
  const res = await fetchAuthed(`/evaluator/submissions/${submissionId}`);
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Open failed');
    return;
  }
  evaluatorSelectedSubmission = body.submission || null;
  if (!evaluatorSelectedSubmission) return;
  updateWorkflowVisibility();
  scrollToResults();
  clearEvaluatorManifestTimer();
  resetResults();
  resetStatusStallTracker();
  resetProgressUI();
  updateProgressUI(0, 'For Review', 'for_review');
  setSummaryLocked(true);
  currentJobId = evaluatorSelectedSubmission.current_job_id;
  if (!currentJobId) {
    parsedRows = (body.transactions || []).map((row, idx) => ({
      row_key: createRowKey(),
      global_row_id: String(idx + 1).padStart(3, '0'),
      row_id: String(idx + 1).padStart(3, '0'),
      date: row.date || '',
      description: row.description || '',
      debit: row.debit != null ? String(row.debit) : '',
      credit: row.credit != null ? String(row.credit) : '',
      balance: row.balance != null ? String(row.balance) : '',
      page: row.page || '',
      page_row_id: '',
    }));
    renderRows(parsedRows);
    const canExport = Boolean(body.review_status && body.review_status.can_export);
    setSummaryLocked(!canExport);
    if (body.summary && canExport) {
      updateSummaryFromSnapshot(body.summary);
    }
    return;
  }

  const shouldStartProcessing = String(evaluatorSelectedSubmission.status || '').toLowerCase() === 'for_review';
  if (shouldStartProcessing) {
    let canStart = true;
    const preStatusRes = await fetchAuthed(`/jobs/${currentJobId}`);
    const preStatusBody = await safeParseJson(preStatusRes);
    if (preStatusRes.ok && preStatusBody) {
      const preState = String(preStatusBody.status || '').toLowerCase();
      if (preState === 'done') {
        canStart = false;
        stopElapsedTimer();
      } else if (preState === 'processing' || preState === 'queued') {
        canStart = false;
        startElapsedTimer();
      }
    }
    if (canStart) {
      const startRes = await fetchAuthed(`/jobs/${currentJobId}/start`, { method: 'POST' });
      const startBody = await safeParseJson(startRes);
      if (!startRes.ok) {
        alert((startBody && startBody.detail) || 'Failed to start processing');
        return;
      }
      evaluatorSelectedSubmission.status = 'processing';
      await loadEvaluatorSubmissions();
      updateProgressUI(2, 'Processing started', 'processing');
      startElapsedTimer();
    }
  }

  try {
    await loadPageManifest();
  } catch (err) {
    console.warn(err.message || 'Manifest flow unavailable, falling back to legacy flow');
    setSummaryLocked(true);
    await openEvaluatorSubmissionLegacyFallback(evaluatorSelectedSubmission);
    return;
  }
  await loadAccountSummary();

  if (areAllManifestPagesParsed()) {
    updateProgressUI(100, 'Results ready', 'completed');
    stopElapsedTimer();
  }

  const firstReadyIndex = evaluatorManifest.findIndex((page) => {
    const state = String(page.parse_status || '').toLowerCase();
    return state === 'done' || state === 'failed';
  });
  const fallbackIndex = firstReadyIndex >= 0 ? firstReadyIndex : 0;
  currentPageIndex = fallbackIndex;
  const targetKey = currentPageKey();
  if (targetKey) {
    await loadActivePage(targetKey);
  } else {
    renderCurrentPage();
  }

  const initialStatusRes = await fetchAuthed(`/jobs/${currentJobId}`);
  const initialStatus = await safeParseJson(initialStatusRes);
  if (initialStatusRes.ok && initialStatus) {
    if ((initialStatus.status === 'processing' || initialStatus.status === 'queued') && areAllManifestPagesParsed()) {
      initialStatus.status = 'done';
      initialStatus.step = 'completed';
      initialStatus.progress = 100;
    }
    const step = initialStatus.step || initialStatus.status || 'processing';
    const progress = Number.isFinite(initialStatus.progress) ? initialStatus.progress : inferProgress(initialStatus.status, step);
    updateProgressUI(progress, stepToLabel(step), step, initialStatus.ocr_backend, initialStatus.parse_mode, initialStatus);
    if (initialStatus.status === 'processing' || initialStatus.status === 'queued') {
      startElapsedTimer();
    } else {
      stopElapsedTimer();
    }
  } else if (areAllManifestPagesParsed()) {
    updateProgressUI(100, 'Results ready', 'completed');
    stopElapsedTimer();
  }

  evaluatorManifestTimer = setInterval(async () => {
    if (!evaluatorSelectedSubmission || evaluatorSelectedSubmission.id !== submissionId) {
      clearEvaluatorManifestTimer();
      return;
    }
    await loadPageManifest();
    const activeKey = currentPageKey();
    const activeManifest = evaluatorManifest[currentPageIndex] || null;
    if (!activePageKey && activeKey) {
      await loadActivePage(activeKey);
    } else if (activeKey && activeManifest && String(activeManifest.parse_status || '').toLowerCase() === 'done' && !parsedRows.length) {
      await loadActivePage(activeKey);
    }
    const statusRes = await fetchAuthed(`/jobs/${currentJobId}`);
    const statusBody = await safeParseJson(statusRes);
    if (statusRes.ok && statusBody) {
      if (isStatusStalled(statusBody)) {
        updateProgressUI(100, 'Processing appears stalled', 'failed', statusBody.ocr_backend, statusBody.parse_mode, statusBody);
        stopElapsedTimer();
        clearEvaluatorManifestTimer();
        return;
      }
      if ((statusBody.status === 'processing' || statusBody.status === 'queued') && areAllManifestPagesParsed()) {
        statusBody.status = 'done';
        statusBody.step = 'completed';
        statusBody.progress = 100;
      }
      const step = statusBody.step || statusBody.status || 'processing';
      const progress = Number.isFinite(statusBody.progress) ? statusBody.progress : inferProgress(statusBody.status, step);
      updateProgressUI(progress, stepToLabel(step), step, statusBody.ocr_backend, statusBody.parse_mode, statusBody);
      if (statusBody.status === 'done' || statusBody.status === 'failed') {
        stopElapsedTimer();
        clearEvaluatorManifestTimer();
      }
    } else if (areAllManifestPagesParsed()) {
      updateProgressUI(100, 'Results ready', 'completed');
      stopElapsedTimer();
      clearEvaluatorManifestTimer();
    }
  }, 1500);
}

async function runEvaluatorStartProcessing() {
  if (!evaluatorSelectedSubmission || !evaluatorSelectedSubmission.current_job_id) {
    alert('Open a submission first.');
    return;
  }
  currentJobId = evaluatorSelectedSubmission.current_job_id;
  const res = await fetchAuthed(`/jobs/${currentJobId}/start`, { method: 'POST' });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Failed to start processing');
    return;
  }
  startElapsedTimer();
  try {
    await pollJobUntilDone();
  } finally {
    stopElapsedTimer();
  }
}

async function saveEvaluatorEdits() {
  if (!evaluatorSelectedSubmission) {
    alert('Open a submission first.');
    return;
  }
  const ok = await saveActivePageIfDirty();
  if (!ok) return;
  alert('Page saved.');
}

async function runEvaluatorAnalyze() {
  if (!evaluatorSelectedSubmission) return;
  const res = await fetchAuthed(`/evaluator/submissions/${evaluatorSelectedSubmission.id}/analyze`, { method: 'POST' });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Analyze failed');
    return;
  }
  alert('Summary recomputed.');
}

async function runEvaluatorReport() {
  if (!evaluatorSelectedSubmission) return;
  const res = await fetchAuthed(`/evaluator/submissions/${evaluatorSelectedSubmission.id}/reports`, { method: 'POST' });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Report generation failed');
    return;
  }
  if (body.download_url) {
    window.open(body.download_url, '_blank');
  }
}

async function runEvaluatorSummaryReady() {
  if (!evaluatorSelectedSubmission) return;
  const res = await fetchAuthed(`/evaluator/submissions/${evaluatorSelectedSubmission.id}/mark-summary-ready`, { method: 'POST' });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Failed to mark summary ready');
    return;
  }
  evaluatorSelectedSubmission = body;
  await loadEvaluatorSubmissions();
  alert('Marked as summary_ready.');
}

async function finishEvaluatorReview() {
  if (!evaluatorSelectedSubmission || !evaluatorSelectedSubmission.id) {
    alert('Open a submission first.');
    return;
  }
  const saved = await saveActivePageIfDirty();
  if (!saved) return;
  if (finishReviewBtn) finishReviewBtn.disabled = true;
  try {
    const res = await fetchAuthed(
      `/evaluator/submissions/${evaluatorSelectedSubmission.id}/finish-review`,
      { method: 'POST' }
    );
    const body = await safeParseJson(res);
    if (!res.ok) {
      alert((body && body.detail) || 'Failed to finish review');
      return;
    }
    evaluatorCanExport = Boolean(body && body.can_export);
    setSummaryLocked(!evaluatorCanExport);
    if (body && body.summary) {
      updateSummaryFromSnapshot(body.summary);
    } else {
      await refreshSummarySnapshot();
    }
    await loadPageManifest();
    alert(evaluatorCanExport
      ? 'Review finished. Summary unlocked and exports enabled.'
      : 'Review finished, but no transactions were found for export.');
  } finally {
    if (finishReviewBtn) finishReviewBtn.disabled = !summaryCard || !summaryCard.classList.contains('is-locked');
  }
}

if (authLoginBtn) authLoginBtn.addEventListener('click', doLogin);
if (authLogoutBtn) authLogoutBtn.addEventListener('click', doLogout);
if (agentSubmitBtn) agentSubmitBtn.addEventListener('click', submitAgentFile);
if (agentRefreshBtn) agentRefreshBtn.addEventListener('click', loadAgentSubmissions);
if (agentBrowseButton && agentFileInput) {
  agentBrowseButton.addEventListener('click', () => {
    agentFileInput.click();
  });
}
if (agentFileInput) {
  agentFileInput.addEventListener('change', () => {
    renderAgentSelectedFiles();
  });
}
if (agentBorrowerName) {
  agentBorrowerName.addEventListener('input', () => {
    updateAgentSubmitButtonState();
  });
}
if (agentSearchInput) {
  agentSearchInput.addEventListener('input', () => {
    agentSubmissionsPage = 1;
    renderAgentSubmissionTableFiltered();
  });
}
if (evaluatorSearchInput) {
  evaluatorSearchInput.addEventListener('input', () => {
    evaluatorSubmissionsPage = 1;
    renderEvaluatorSubmissionTableFiltered();
  });
}
if (agentSubmissionList) {
  agentSubmissionList.addEventListener('click', (e) => {
    const pageBtn = e.target.closest('button.agent-page-btn');
    if (!pageBtn) return;
    const nav = pageBtn.dataset.pageNav || '';
    if (nav) {
      if (nav === 'prev') agentSubmissionsPage -= 1;
      if (nav === 'next') agentSubmissionsPage += 1;
      renderAgentSubmissionTableFiltered();
      return;
    }
    const page = Number.parseInt(pageBtn.dataset.page || '', 10);
    if (Number.isFinite(page) && page > 0) {
      agentSubmissionsPage = page;
      renderAgentSubmissionTableFiltered();
    }
  });
}
if (agentSelectedFiles) {
  agentSelectedFiles.addEventListener('click', (e) => {
    const btn = e.target.closest('.agent-file-remove');
    if (!btn) return;
    const idx = Number.parseInt(btn.dataset.index || '-1', 10);
    if (!Number.isFinite(idx) || idx < 0) return;
    const files = getAgentSelectedFiles();
    files.splice(idx, 1);
    setAgentSelectedFiles(files);
    renderAgentSelectedFiles();
  });
}
if (agentDropZone && agentFileInput) {
  agentDropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    agentDropZone.classList.add('dragover');
  });
  agentDropZone.addEventListener('dragleave', () => {
    agentDropZone.classList.remove('dragover');
  });
  agentDropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    agentDropZone.classList.remove('dragover');
    const dropped = e.dataTransfer && e.dataTransfer.files ? Array.from(e.dataTransfer.files) : [];
    if (!dropped.length) return;
    const existing = getAgentSelectedFiles();
    const nextFiles = existing.concat(dropped.filter((f) => /\.pdf$/i.test(f.name)));
    setAgentSelectedFiles(nextFiles);
    renderAgentSelectedFiles();
  });
}
renderAgentSelectedFiles();
updateAgentSubmitButtonState();
updateEvaluatorCombineButtonState();
if (evaluatorRefreshBtn) evaluatorRefreshBtn.addEventListener('click', loadEvaluatorSubmissions);
if (evaluatorCombineBtn) {
  evaluatorCombineBtn.addEventListener('click', async () => {
    await combineSelectedEvaluatorSubmissions();
  });
}
if (evaluatorSubmissionList) {
  evaluatorSubmissionList.addEventListener('change', (e) => {
    const checkbox = e.target.closest('input.evaluator-select-checkbox');
    if (!checkbox) return;
    const submissionId = String(checkbox.dataset.submissionId || '').trim();
    if (!submissionId) return;
    if (checkbox.checked) {
      evaluatorSelectedSubmissionIds.add(submissionId);
    } else {
      evaluatorSelectedSubmissionIds.delete(submissionId);
    }
    updateEvaluatorCombineButtonState();
  });
  evaluatorSubmissionList.addEventListener('click', async (e) => {
    const pageBtn = e.target.closest('button.agent-page-btn');
    if (pageBtn) {
      const nav = pageBtn.dataset.evaluatorPageNav || '';
      if (nav) {
        if (nav === 'prev') evaluatorSubmissionsPage -= 1;
        if (nav === 'next') evaluatorSubmissionsPage += 1;
        renderEvaluatorSubmissionTableFiltered();
        return;
      }
      const page = Number.parseInt(pageBtn.dataset.evaluatorPage || '', 10);
      if (Number.isFinite(page) && page > 0) {
        evaluatorSubmissionsPage = page;
        renderEvaluatorSubmissionTableFiltered();
      }
      return;
    }
    const btn = e.target.closest('button[data-action]');
    if (!btn) return;
    const submissionId = btn.dataset.id;
    const action = btn.dataset.action;
    if (!submissionId || !action) return;
    if (action === 'assign') {
      await assignEvaluatorSubmission(submissionId);
      return;
    }
    if (action === 'open') {
      if (evaluatorSelectedSubmission && evaluatorSelectedSubmission.id && evaluatorSelectedSubmission.id !== submissionId) {
        await saveActivePageIfDirty();
      }
      await openEvaluatorSubmission(submissionId);
    }
  });
}
if (authToken && authRole) {
  if (forcedPageRole === 'agent' && authRole !== 'agent') {
    if (authRole === 'credit_evaluator') {
      window.location.href = '/evaluator';
    } else {
      window.location.href = '/admin';
    }
  }
  if (forcedPageRole === 'credit_evaluator' && authRole !== 'credit_evaluator') {
    if (authRole === 'agent') {
      window.location.href = '/agent';
    } else {
      window.location.href = '/admin';
    }
  }
  updateWorkflowVisibility();
  if (authRole === 'agent') {
    loadAgentSubmissions();
  } else if (authRole === 'credit_evaluator') {
    loadEvaluatorSubmissions();
  }
} else {
  updateWorkflowVisibility();
  setLegacyEditorVisible(false);
}

fileInput.addEventListener('change', () => {
  if (!fileInput.files || !fileInput.files[0]) return;
  handleFileSelection(fileInput.files[0]);
});

uploadArea.addEventListener('dragover', (e) => {
  e.preventDefault();
  uploadArea.classList.add('dragover');
});

uploadArea.addEventListener('dragleave', () => {
  uploadArea.classList.remove('dragover');
});

uploadArea.addEventListener('drop', (e) => {
  e.preventDefault();
  uploadArea.classList.remove('dragover');
  if (!e.dataTransfer.files || !e.dataTransfer.files[0]) return;

  const file = e.dataTransfer.files[0];
  const dataTransfer = new DataTransfer();
  dataTransfer.items.add(file);
  fileInput.files = dataTransfer.files;
  handleFileSelection(file);
});

removeFileBtn.addEventListener('click', () => {
  selectedFile = null;
  currentJobId = null;
  fileInput.value = '';
  fileInfo.style.display = 'none';
  shouldAutoScrollToResults = false;
  resetProgressUI();
  resetResults();
});

viewDetails.addEventListener('click', () => {
  if (!currentJobId) {
    alert('Upload a statement first.');
    return;
  }
  alert(`OCR job ID: ${currentJobId}`);
});

finishSave.addEventListener('click', () => {
  if (!currentJobId) {
    alert('Upload a statement first.');
    return;
  }
  if (!ocrStarted) {
    startProcessingFromDraft();
    return;
  }
  if (!parsedRows.length) {
    alert('OCR is still running or no rows extracted yet.');
    return;
  }
  alert('OCR complete. You can review rows and download CSV.');
});

if (downloadCSV) {
  downloadCSV.addEventListener('click', async () => {
    await exportToPdf();
  });
}

if (finishReviewBtn) {
  finishReviewBtn.addEventListener('click', async () => {
    await finishEvaluatorReview();
  });
}

if (exportExcelBtn) {
  exportExcelBtn.addEventListener('click', async () => {
    await exportToExcel();
  });
}

if (ocrToolsToggleBtn) {
  ocrToolsToggleBtn.addEventListener('click', () => {
    ocrToolsUnlocked = !canUseOcrTools();
    if (!canUseOcrTools()) {
      activeGuideTool = 'none';
      flattenMode = false;
      flattenPoints = [];
    }
    renderOcrToolsToggle();
    updateFlattenButtons();
    updateGuideToolButtons();
    drawBoundingBoxes();
  });
}

prevPageBtn.addEventListener('click', () => {
  if (currentPageIndex <= 0) return;
  if (authRole === 'credit_evaluator' && evaluatorSelectedSubmission) {
    switchToPageByIndex(currentPageIndex - 1);
    return;
  }
  currentPageIndex -= 1;
  renderCurrentPage();
});

nextPageBtn.addEventListener('click', () => {
  if (currentPageIndex >= pageList.length - 1) return;
  if (authRole === 'credit_evaluator' && evaluatorSelectedSubmission) {
    switchToPageByIndex(currentPageIndex + 1);
    return;
  }
  currentPageIndex += 1;
  renderCurrentPage();
});

if (pageSelect) {
  pageSelect.addEventListener('change', () => {
    const idx = Number.parseInt(pageSelect.value, 10);
    if (!Number.isFinite(idx) || idx < 0 || idx >= pageList.length) return;
    if (authRole === 'credit_evaluator' && evaluatorSelectedSubmission) {
      switchToPageByIndex(idx);
      return;
    }
    currentPageIndex = idx;
    renderCurrentPage();
  });
}

previewImage.addEventListener('load', () => {
  previewImage.style.display = 'block';
  setPreviewAspectRatioFromImage();
  resetPreviewTransform();
  updatePreviewInteractionMode();
  drawBoundingBoxes();
  maybeFocusActiveRowInPreview();
});

previewImage.addEventListener('error', () => {
  setPreviewEmptyState();
});

window.addEventListener('resize', () => {
  if (previewImage && previewImage.naturalWidth) {
    setPreviewAspectRatioFromImage();
  }
  syncTablePanelHeightToPreview();
  if (pageList.length) {
    drawBoundingBoxes();
  }
  clampPreviewPan();
  applyPreviewTransform();
});

document.addEventListener('click', (e) => {
  if (!e.target.closest('.row-menu')) {
    closeAllRowMenus();
  }
});

previewCanvas.addEventListener('click', (e) => {
  if (isTextOnlyToolsMode()) return;
  const point = getNormalizedPreviewPointFromEvent(e);
  if (!point) return;

  if (flattenMode && !flattenBusy) {
    if (flattenPoints.length >= 4) return;
    flattenPoints.push({ x: point.x, y: point.y });
    updateFlattenButtons();
    drawBoundingBoxes();
    return;
  }

  if (activeGuideTool === 'horizontal') {
    if (addGuideLineForCurrentPage('horizontal', point.y)) {
      drawBoundingBoxes();
    }
  }
});

if (previewWrap) {
  previewWrap.addEventListener('mousemove', (e) => {
    if (isPreviewPanning) {
      const dx = e.clientX - panStartX;
      const dy = e.clientY - panStartY;
      previewPanX = panOriginX + dx;
      previewPanY = panOriginY + dy;
      clampPreviewPan();
      applyPreviewTransform();
      drawBoundingBoxes();
      return;
    }
  });

  previewWrap.addEventListener('mouseleave', () => {
    stopPreviewPan();
  });

  previewWrap.addEventListener('mousedown', (e) => {
    if (e.button !== 0) return;
    if (flattenMode || activeGuideTool !== 'none' || !pageList.length) return;
    isPreviewPanning = true;
    panStartX = e.clientX;
    panStartY = e.clientY;
    panOriginX = previewPanX;
    panOriginY = previewPanY;
    previewWrap.classList.add('panning');
    e.preventDefault();
  });

}

window.addEventListener('mousemove', (e) => {
  if (horizontalGuideDragState && previewRowsRuler) {
    const key = horizontalGuideDragState.pageKey;
    const state = getGuideStateForPage(key, true);
    const rect = previewRowsRuler.getBoundingClientRect();
    if (!rect.height) return;
    const deltaRatio = (e.clientY - horizontalGuideDragState.startY) / rect.height;
    const lines = (horizontalGuideDragState.startLines || []).slice();
    const idx = horizontalGuideDragState.index;
    if (idx < 0 || idx >= lines.length) return;
    const prevLimit = idx > 0 ? lines[idx - 1] + HORIZONTAL_LINE_MIN_GAP : HORIZONTAL_LINE_MIN_GAP;
    const nextLimit = idx < lines.length - 1 ? lines[idx + 1] - HORIZONTAL_LINE_MIN_GAP : (1 - HORIZONTAL_LINE_MIN_GAP);
    const next = clamp(lines[idx] + deltaRatio, prevLimit, nextLimit);
    lines[idx] = next;
    state.horizontal = lines;
    updateGuideSectionsInfo();
    drawBoundingBoxes();
    renderPreviewRowsRuler();
    return;
  }
  if (!columnResizeState || !previewColumnsRuler) return;
  const activeWidth = getPreviewColumnsRulerActiveWidth();
  if (!activeWidth) return;
  const deltaRatio = (e.clientX - columnResizeState.startX) / activeWidth;
  const base = (columnResizeState.startLayout || []).map((col) => ({ ...col }));
  setColumnLayoutForPage(columnResizeState.pageKey, base);
  applyColumnResize(columnResizeState.pageKey, columnResizeState.index, deltaRatio, { redraw: true, invalidate: false });
});

window.addEventListener('mouseup', () => {
  if (horizontalGuideDragState) {
    const key = horizontalGuideDragState.pageKey;
    const before = horizontalGuideDragState.guideBefore || cloneGuideStateSnapshot(getGuideStateForPage(key, true));
    const after = cloneGuideStateSnapshot(getGuideStateForPage(key, true));
    if (!guideStatesEqual(before, after)) {
      pushGuideUndoSnapshot(key, before);
      markHorizontalGuideTouched(key);
      invalidateGuideDerivedData(key);
      updateGuideSectionsInfo();
      updateGuideToolButtons();
      markActivePageDirty();
      drawBoundingBoxes();
    }
    horizontalGuideDragState = null;
    if (previewRowsRuler) {
      previewRowsRuler.classList.remove('is-resizing');
    }
    renderPreviewRowsRuler();
  }
  if (columnResizeState) {
    const key = columnResizeState.pageKey;
    const before = columnResizeState.guideBefore || cloneGuideStateSnapshot(getGuideStateForPage(key, true));
    const after = cloneGuideStateSnapshot(getGuideStateForPage(key, true));
    if (!guideStatesEqual(before, after)) {
      pushGuideUndoSnapshot(key, before);
      invalidateGuideDerivedData(key);
      updateGuideSectionsInfo();
      updateGuideToolButtons();
      markActivePageDirty();
      drawBoundingBoxes();
    }
    columnResizeState = null;
    if (previewColumnsRuler) {
      previewColumnsRuler.classList.remove('is-resizing');
    }
    renderPreviewColumnsRuler();
  }
  stopPreviewPan();
});

if (resetViewBtn) {
  resetViewBtn.addEventListener('click', () => {
    resetPreviewTransform();
  });
}

if (zoomInBtn) {
  zoomInBtn.addEventListener('click', () => {
    stepPreviewZoom(PREVIEW_ZOOM_STEP);
  });
}

if (zoomOutBtn) {
  zoomOutBtn.addEventListener('click', () => {
    stepPreviewZoom(-PREVIEW_ZOOM_STEP);
  });
}

if (addHorizontalGuideBtn) {
  addHorizontalGuideBtn.addEventListener('click', () => {
    if (!pageList.length) return;
    setActiveGuideTool('horizontal');
  });
}

if (guideUndoBtn) {
  guideUndoBtn.addEventListener('click', () => {
    undoGuideLinesForCurrentPage();
  });
}

if (guideRedoBtn) {
  guideRedoBtn.addEventListener('click', () => {
    redoGuideLinesForCurrentPage();
  });
}

if (clearGuideLinesBtn) {
  clearGuideLinesBtn.addEventListener('click', () => {
    clearGuideLinesForCurrentPage();
  });
}

if (runSectionOcrBtn) {
  runSectionOcrBtn.addEventListener('click', async () => {
    await runSectionOcrForCurrentPage();
  });
}

if (imageToolButtons.length) {
  imageToolButtons.forEach((btn) => {
    btn.addEventListener('click', async () => {
      const tool = btn.dataset ? String(btn.dataset.imageTool || '').trim() : '';
      if (!tool) return;
      await applyImageToolForCurrentPage(tool);
    });
  });
}

if (previewColumnsRuler) {
  previewColumnsRuler.addEventListener('dragstart', (e) => {
    if (isTextOnlyToolsMode()) {
      e.preventDefault();
      return;
    }
    const item = e.target.closest('.preview-col-item');
    if (!item || !item.dataset || !item.dataset.colKey) return;
    if (columnResizeState) {
      e.preventDefault();
      return;
    }
    columnDragState.sourceKey = item.dataset.colKey;
    columnDragState.targetKey = item.dataset.colKey;
    columnSwapSelectKey = '';
    if (e.dataTransfer) {
      e.dataTransfer.effectAllowed = 'move';
      e.dataTransfer.setData('text/plain', columnDragState.sourceKey);
    }
    updateRulerDragClasses();
  });

  previewColumnsRuler.addEventListener('dragover', (e) => {
    if (isTextOnlyToolsMode()) return;
    if (!columnDragState.sourceKey) return;
    const item = e.target.closest('.preview-col-item');
    if (!item || !item.dataset || !item.dataset.colKey) return;
    e.preventDefault();
    columnDragState.targetKey = item.dataset.colKey;
    updateRulerDragClasses();
  });

  previewColumnsRuler.addEventListener('drop', (e) => {
    if (isTextOnlyToolsMode()) return;
    if (!columnDragState.sourceKey) return;
    const item = e.target.closest('.preview-col-item');
    const pageKey = currentPageKey();
    if (pageKey && item && item.dataset && item.dataset.colKey) {
      reorderColumnLayout(pageKey, columnDragState.sourceKey, item.dataset.colKey);
    }
    columnDragState = { sourceKey: '', targetKey: '' };
    updateRulerDragClasses();
  });

  previewColumnsRuler.addEventListener('dragend', () => {
    columnDragState = { sourceKey: '', targetKey: '' };
    updateRulerDragClasses();
  });

  previewColumnsRuler.addEventListener('mousedown', (e) => {
    if (isTextOnlyToolsMode()) return;
    const handle = e.target.closest('.preview-col-resizer');
    if (!handle || !handle.dataset) return;
    const pageKey = currentPageKey();
    if (!pageKey) return;
    const index = Number.parseInt(handle.dataset.resizeIndex || '', 10);
    if (!Number.isFinite(index)) return;

    const guideSnapshot = cloneGuideStateSnapshot(getGuideStateForPage(pageKey, true));
    columnResizeState = {
      pageKey,
      index,
      startX: e.clientX,
      startLayout: getColumnLayoutForPage(pageKey, true).map((col) => ({ ...col })),
      guideBefore: guideSnapshot,
    };
    previewColumnsRuler.classList.add('is-resizing');
    e.preventDefault();
    e.stopPropagation();
  });

  previewColumnsRuler.addEventListener('click', (e) => {
    if (isTextOnlyToolsMode()) return;
    if (e.target.closest('.preview-col-resizer')) return;
    const item = e.target.closest('.preview-col-item');
    if (!item || !item.dataset || !item.dataset.colKey) return;
    const pageKey = currentPageKey();
    if (!pageKey) return;

    const key = item.dataset.colKey;
    if (!columnSwapSelectKey) {
      columnSwapSelectKey = key;
      updateRulerDragClasses();
      return;
    }
    if (columnSwapSelectKey === key) {
      columnSwapSelectKey = '';
      updateRulerDragClasses();
      return;
    }
    reorderColumnLayout(pageKey, columnSwapSelectKey, key);
    columnSwapSelectKey = '';
    updateRulerDragClasses();
  });
}

if (previewRowsRuler) {
  previewRowsRuler.addEventListener('mousedown', (e) => {
    if (isTextOnlyToolsMode()) return;
    const handle = e.target.closest('.preview-row-handle');
    if (!handle || !handle.dataset) return;
    const pageKey = currentPageKey();
    if (!pageKey) return;
    const index = Number.parseInt(handle.dataset.rowIndex || '', 10);
    if (!Number.isFinite(index)) return;

    const state = getGuideStateForPage(pageKey, true);
    horizontalGuideDragState = {
      pageKey,
      index,
      startY: e.clientY,
      startLines: (state.horizontal || []).slice(),
      guideBefore: cloneGuideStateSnapshot(state),
    };
    previewRowsRuler.classList.add('is-resizing');
    e.preventDefault();
    e.stopPropagation();
  });

  previewRowsRuler.addEventListener('click', (e) => {
    if (isTextOnlyToolsMode()) return;
    if (e.target.closest('.preview-row-handle')) return;
    if (activeGuideTool !== 'horizontal') return;
    const pageKey = currentPageKey();
    if (!pageKey) return;
    const rect = previewRowsRuler.getBoundingClientRect();
    if (!rect.height) return;
    const y = clamp((e.clientY - rect.top) / rect.height, 0, 1);
    if (addGuideLineForCurrentPage('horizontal', y)) {
      drawBoundingBoxes();
    }
  });
}

if (flattenModeBtn) {
  flattenModeBtn.addEventListener('click', () => {
    if (!pageList.length || flattenBusy) return;
    flattenMode = !flattenMode;
    if (flattenMode) {
      activeGuideTool = 'none';
      updateGuideToolButtons();
    }
    if (flattenMode) {
      resetPreviewTransform();
    }
    flattenPoints = [];
    updateFlattenButtons();
    drawBoundingBoxes();
  });
}

if (applyFlattenBtn) {
  applyFlattenBtn.addEventListener('click', async () => {
    if (!currentJobId || !pageList.length || flattenBusy) return;
    if (flattenPoints.length !== 4) {
      alert('Select exactly 4 corner points first.');
      return;
    }
    try {
      flattenBusy = true;
      updateFlattenButtons();
      const pageFile = pageList[currentPageIndex];
      const pageKey = pageFile.replace('.png', '');

      const res = await fetchAuthed(`/jobs/${currentJobId}/pages/${pageKey}/flatten`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ points: flattenPoints }),
      });
      if (!res.ok) {
        const err = await safeParseJson(res);
        throw new Error((err && err.detail) || 'Flatten failed');
      }

      await refreshCurrentPageData(pageKey);
      flattenPoints = [];
      flattenMode = false;
      updateFlattenButtons();
      renderCurrentPage();
    } catch (err) {
      alert(err.message || 'Flatten failed');
    } finally {
      flattenBusy = false;
      updateFlattenButtons();
    }
  });
}

if (resetFlattenBtn) {
  resetFlattenBtn.addEventListener('click', async () => {
    if (!currentJobId || !pageList.length || flattenBusy) return;
    try {
      flattenBusy = true;
      updateFlattenButtons();
      const pageFile = pageList[currentPageIndex];
      const pageKey = pageFile.replace('.png', '');

      const res = await fetchAuthed(`/jobs/${currentJobId}/pages/${pageKey}/flatten/reset`, {
        method: 'POST',
      });
      if (!res.ok) {
        const err = await safeParseJson(res);
        throw new Error((err && err.detail) || 'Reset failed');
      }

      await refreshCurrentPageData(pageKey);
      flattenPoints = [];
      flattenMode = false;
      updateFlattenButtons();
      renderCurrentPage();
    } catch (err) {
      alert(err.message || 'Reset failed');
    } finally {
      flattenBusy = false;
      updateFlattenButtons();
    }
  });
}

function handleFileSelection(file) {
  if (!file.name.toLowerCase().endsWith('.pdf')) {
    alert('Please upload a PDF file.');
    return;
  }

  selectedFile = file;
  fileName.textContent = file.name;
  fileSize.textContent = formatFileSize(file.size);
  fileInfo.style.display = 'flex';
  resetResults();
  createDraftJob();
}

async function createDraftJob() {
  if (!selectedFile) return;

  shouldAutoScrollToResults = false;
  hasSeenInFlightStatus = false;
  ocrStarted = false;
  finishSave.textContent = 'Start OCR';
  startElapsedTimer();
  updateProgressUI(4, 'Uploading file...', 'uploading');

  try {
    const formData = new FormData();
    formData.append('file', selectedFile);
    formData.append('mode', getSelectedParseMode());

    const res = await fetchAuthed('/jobs', { method: 'POST', body: formData });
    if (!res.ok) {
      const error = await safeParseJson(res);
      throw new Error((error && error.detail) || 'Failed to upload file');
    }

    const data = await res.json();
    currentJobId = data.job_id;
    ocrStarted = true;
    shouldAutoScrollToResults = true;
    hasSeenInFlightStatus = false;
    finishSave.textContent = 'Running OCR...';
    await pollJobUntilDone();
    stopElapsedTimer();
    finishSave.textContent = 'OCR Done';
  } catch (err) {
    stopElapsedTimer();
    ocrStarted = false;
    finishSave.textContent = 'Start OCR';
    updateProgressUI(0, err.message || 'Upload failed', 'failed');
    alert(err.message || 'Upload failed.');
  }
}

async function pollDraftUntilReady() {
  if (!currentJobId) return;

  while (true) {
    const res = await fetchAuthed(`/jobs/${currentJobId}`);
    if (!res.ok) {
      throw new Error('Failed to read draft status');
    }

    const status = await res.json();
    const step = status.step || status.status || 'draft_queued';
    const progress = Number.isFinite(status.progress) ? status.progress : inferProgress(status.status, step);
    const currentOcrModel = status.ocr_backend || null;
    const currentMode = status.parse_mode || getSelectedParseMode();

    if (status.status === 'failed') {
      throw new Error(status.message || 'Draft preparation failed');
    }

    updateProgressUI(progress, stepToLabel(step), step, currentOcrModel, currentMode, status);

    if (status.status === 'draft' && step === 'ready_for_edit') {
      stopElapsedTimer();
      await loadDraftPages();
      updateProgressUI(100, 'Edit pages then click Start OCR', 'ready_for_edit', currentOcrModel, currentMode, status);
      return;
    }

    await sleep(900);
  }
}

async function loadDraftPages() {
  const cleanedRes = await fetchAuthed(`/jobs/${currentJobId}/cleaned`);
  if (!cleanedRes.ok) throw new Error('Failed to load draft pages');

  const cleanedData = await cleanedRes.json();
  pageList = cleanedData.pages || [];
  currentPageIndex = 0;
  pageImageVersion = {};
  pageList.forEach((fileName) => {
    pageImageVersion[fileName.replace('.png', '')] = 0;
  });
  clearPreviewBlobCache();
  renderRows([]);
  renderCurrentPage();
}

async function startProcessingFromDraft() {
  if (!currentJobId || ocrStarted) return;
  try {
    ocrStarted = true;
    shouldAutoScrollToResults = true;
    hasSeenInFlightStatus = false;
    finishSave.textContent = 'Running OCR...';
    startElapsedTimer();

    const res = await fetchAuthed(`/jobs/${currentJobId}/start`, { method: 'POST' });
    if (!res.ok) {
      const error = await safeParseJson(res);
      throw new Error((error && error.detail) || 'Failed to start OCR');
    }
    await pollJobUntilDone();
    stopElapsedTimer();
    finishSave.textContent = 'OCR Done';
  } catch (err) {
    stopElapsedTimer();
    ocrStarted = false;
    finishSave.textContent = 'Start OCR';
    updateProgressUI(0, err.message || 'Failed to start OCR', 'failed');
    alert(err.message || 'Failed to start OCR');
  }
}

async function pollJobUntilDone() {
  if (!currentJobId) return;
  resetStatusStallTracker();

  while (true) {
    const res = await fetchAuthed(`/jobs/${currentJobId}`);
    if (!res.ok) throw new Error('Failed to read job status');

    const status = await res.json();
    if (isStatusStalled(status)) {
      updateProgressUI(100, 'Processing appears stalled', 'failed', status.ocr_backend, status.parse_mode, status);
      throw new Error('processing_stale_timeout');
    }
    const step = status.step || status.status || 'processing';
    const progress = Number.isFinite(status.progress) ? status.progress : inferProgress(status.status, step);
    const currentOcrModel = status.ocr_backend || null;
    const currentMode = status.parse_mode || getSelectedParseMode();
    if (status.status === 'queued' || status.status === 'processing') {
      hasSeenInFlightStatus = true;
    }

    if (status.status === 'done') {
      updateProgressUI(100, 'Results ready', step, currentOcrModel, currentMode, status);
      await loadResults();
      if (shouldAutoScrollToResults && hasSeenInFlightStatus) {
        shouldAutoScrollToResults = false;
        hasSeenInFlightStatus = false;
        scrollToResults();
      }
      return;
    }

    if (status.status === 'failed') {
      updateProgressUI(progress, status.message || 'OCR job failed', step, currentOcrModel, currentMode, status);
      throw new Error(status.message || 'OCR job failed');
    }

    updateProgressUI(progress, stepToLabel(step), step, currentOcrModel, currentMode, status);
    await sleep(1200);
  }
}

async function loadResults() {
  const cleanedRes = await fetchAuthed(`/jobs/${currentJobId}/cleaned`);
  if (!cleanedRes.ok) throw new Error('Failed to read processed pages');

  const cleanedData = await cleanedRes.json();
  pageList = cleanedData.pages || [];

  rowsByPage = {};
  boundsByPage = {};
  pageRowToGlobal = {};
  identityBoundsByPage = {};
  guideLinesByPage = {};
  guideHistoryByPage = {};
  columnLayoutByPage = {};
  columnDragState = { sourceKey: '', targetKey: '' };
  columnResizeState = null;
  columnSwapSelectKey = '';
  horizontalGuideDragState = null;
  horizontalGuideTouchedByPage = {};
  activeGuideTool = 'none';
  sectionOcrResultsByPage = {};
  imageToolInFlight = false;
  setSectionOcrBusy(false);
  parsedRows = [];
  activeRowKey = null;
  currentPageIndex = 0;
  rowKeyCounter = 1;
  pageImageVersion = {};
  clearPreviewBlobCache();

  if (!pageList.length) {
    renderRows([]);
    renderCurrentPage();
    return;
  }

  let globalCounter = 1;
  let parsedAll = null;
  let boundsAll = null;

  try {
    const [parsedAllRes, boundsAllRes] = await Promise.all([
      fetchAuthed(`/jobs/${currentJobId}/parsed`),
      fetchAuthed(`/jobs/${currentJobId}/bounds`)
    ]);
    if (parsedAllRes.ok) parsedAll = await parsedAllRes.json();
    if (boundsAllRes.ok) boundsAll = await boundsAllRes.json();
  } catch (_) {
    parsedAll = null;
    boundsAll = null;
  }

  for (let i = 0; i < pageList.length; i += 1) {
    const pageKey = pageList[i].replace('.png', '');
    let rows = parsedAll && Array.isArray(parsedAll[pageKey]) ? parsedAll[pageKey] : null;
    let bounds = boundsAll && Array.isArray(boundsAll[pageKey]) ? boundsAll[pageKey] : null;

    if (!rows) {
      const parsedRes = await fetchAuthed(`/jobs/${currentJobId}/parsed/${pageKey}`);
      if (!parsedRes.ok) {
        throw new Error(`Failed to read parsed rows for ${pageKey}`);
      }
      rows = await parsedRes.json();
    }
    if (!bounds) {
      const boundsRes = await fetchAuthed(`/jobs/${currentJobId}/rows/${pageKey}/bounds`);
      bounds = boundsRes.ok ? await boundsRes.json() : [];
    }

    rowsByPage[pageKey] = Array.isArray(rows) ? rows : [];
    boundsByPage[pageKey] = Array.isArray(bounds) ? bounds : [];

    rowsByPage[pageKey].forEach((row) => {
      const globalId = String(globalCounter).padStart(3, '0');
      globalCounter += 1;

      const normalizedRow = {
        ...row,
        row_key: createRowKey(),
        global_row_id: globalId,
        page: pageKey,
        page_row_id: row.row_id
      };
      normalizeRowDisplayValues(normalizedRow);
      parsedRows.push(normalizedRow);

      pageRowToGlobal[`${pageKey}|${row.row_id}`] = globalId;
    });
  }

  rebuildPageRowMap();
  renderRows(parsedRows);
  flattenMode = false;
  flattenPoints = [];
  updateFlattenButtons();
  updateGuideToolButtons();
  renderCurrentPage();
  await loadAccountSummary();
  drawBoundingBoxes();
}

async function loadAccountSummary() {
  if (!accountNameSummary || !accountNumberSummary || !currentJobId) return;
  accountNameSummary.textContent = '-';
  accountNumberSummary.textContent = '-';
  try {
    let job = null;
    const identityRes = await fetchAuthed(`/jobs/${currentJobId}/account-identity`);
    if (identityRes.ok) {
      job = await identityRes.json();
    } else {
      const res = await fetchAuthed(`/jobs/${currentJobId}/diagnostics`);
      if (!res.ok) return;
      const diagnostics = await res.json();
      job = diagnostics && diagnostics.job ? diagnostics.job : {};
    }
    if (!job) return;
    accountNameSummary.textContent = (job.account_name || '-').toString();
    accountNumberSummary.textContent = (job.account_number || '-').toString();
    identityBoundsByPage = {};
    const nameBox = job.account_name_bbox;
    const numberBox = job.account_number_bbox;
    if (nameBox && nameBox.page) {
      identityBoundsByPage[nameBox.page] = identityBoundsByPage[nameBox.page] || [];
      identityBoundsByPage[nameBox.page].push({ ...nameBox, kind: 'account_name' });
    }
    if (numberBox && numberBox.page) {
      identityBoundsByPage[numberBox.page] = identityBoundsByPage[numberBox.page] || [];
      identityBoundsByPage[numberBox.page].push({ ...numberBox, kind: 'account_number' });
    }
  } catch (_) {
    accountNameSummary.textContent = '-';
    accountNumberSummary.textContent = '-';
    identityBoundsByPage = {};
  }
}

function renderRows(rows) {
  tableBody.innerHTML = '';

  if (!rows.length) {
    tableBody.innerHTML = '<div class="table-empty">No transactions extracted</div>';
    totalTransactions.textContent = '0';
    totalDebitTransactions.textContent = '0';
    totalCreditTransactions.textContent = '0';
    endingBalance.textContent = '-';
    syncTablePanelHeightToPreview();
    return;
  }

  rows.forEach((row) => {
    const el = document.createElement('div');
    el.className = 'table-row';
    el.dataset.rowKey = row.row_key;
    el.dataset.page = row.page;
    el.dataset.pageRowId = row.page_row_id || '';
    el.innerHTML = '';

    const rowNoCell = document.createElement('div');
    rowNoCell.className = 'table-cell';
    rowNoCell.textContent = row.global_row_id || '';
    el.appendChild(rowNoCell);

    el.appendChild(makeEditableCell(row, 'date'));
    el.appendChild(makeEditableCell(row, 'description'));
    el.appendChild(makeEditableCell(row, 'debit', true));
    el.appendChild(makeEditableCell(row, 'credit', true));
    el.appendChild(makeEditableCell(row, 'balance', true));

    const actionsCell = document.createElement('div');
    actionsCell.className = 'table-cell table-actions';
    actionsCell.appendChild(makeRowActionsMenu(row.row_key));
    el.appendChild(actionsCell);

    el.addEventListener('click', () => {
      selectRow(row.row_key);
    });

    tableBody.appendChild(el);
  });

  updateSummaryFromRows(rows);
  syncTablePanelHeightToPreview();
}

function selectRow(rowKey) {
  activeRowKey = rowKey;
  const row = parsedRows.find((r) => r.row_key === rowKey);
  if (!row) return;

  const pagePos = pageList.findIndex((p) => p.replace('.png', '') === row.page);
  if (pagePos >= 0) {
    currentPageIndex = pagePos;
  }

  highlightSelectedTableRow();
  renderCurrentPage();
}

function highlightSelectedTableRow() {
  document.querySelectorAll('.table-row').forEach((rowEl) => {
    const isActive = rowEl.dataset.rowKey === activeRowKey;
    rowEl.classList.toggle('selected', isActive);
  });
}

function renderCurrentPage() {
  renderActivePageSavedMark();
  if (!pageList.length) {
    pageIndicator.textContent = '0/0';
    prevPageBtn.disabled = true;
    nextPageBtn.disabled = true;
    syncPageSelect();
    setPreviewEmptyState();
    return;
  }

  const pageFile = pageList[currentPageIndex];
  const pageKey = pageFile.replace('.png', '');
  if (!isTextOnlyToolsMode()) {
    ensureColumnLayoutForPage(pageKey);
    maybeAutoSeedHorizontalGuides(pageKey, { redraw: false, invalidate: false });
  }
  pageIndicator.textContent = `${currentPageIndex + 1}/${pageList.length}`;
  prevPageBtn.disabled = currentPageIndex === 0;
  nextPageBtn.disabled = currentPageIndex >= pageList.length - 1;
  syncPageSelect();

  loadPreviewImageForPage(pageKey);

  const activeRow = parsedRows.find((r) => r.row_key === activeRowKey);
  if (activeRow && activeRow.page !== pageKey) {
    highlightSelectedTableRow();
  }
  prefetchPreviewNeighbors();
  updateFlattenButtons();
  renderSectionOcrResultForPage(pageKey);
  updateGuideToolButtons();
  updatePreviewInteractionMode();
}

function maybeFocusActiveRowInPreview() {
  // Disabled: selecting a table row should highlight only, without zoom/pan.
}

function focusPreviewOnActiveRow() {
  const pageFile = pageList[currentPageIndex];
  if (!pageFile || !previewWrap || !previewImage.naturalWidth) return false;

  const pageKey = pageFile.replace('.png', '');
  const activeRow = parsedRows.find((r) => r.row_key === activeRowKey);
  if (!activeRow || activeRow.page !== pageKey || !activeRow.global_row_id) return false;

  const activeBox = resolveActiveRowBox(pageKey, activeRow);
  if (!activeBox) return false;

  const fillZoom = getFillPreviewZoom();
  const minFocusZoom = Math.min(PREVIEW_ZOOM_MAX, fillZoom + 0.35);
  if (previewZoom < minFocusZoom) {
    previewZoom = minFocusZoom;
  }

  const rect = getRenderedImageRect(previewImage);
  const wrapW = previewWrap.clientWidth || 1;
  const wrapH = previewWrap.clientHeight || 1;
  if (rect.width <= 0 || rect.height <= 0) return false;

  const wrapRect = previewWrap.getBoundingClientRect();
  const baseLeft = rect.left - wrapRect.left;
  const baseTop = rect.top - wrapRect.top;

  const rowCx = ((Number(activeBox.x1) + Number(activeBox.x2)) / 2) * rect.width;
  const rowCy = ((Number(activeBox.y1) + Number(activeBox.y2)) / 2) * rect.height;
  const targetLocalX = wrapW / 2;
  const targetLocalY = wrapH * 0.42;

  previewPanX = targetLocalX - baseLeft - (rect.width / 2) - ((rowCx - (rect.width / 2)) * previewZoom);
  previewPanY = targetLocalY - baseTop - (rect.height / 2) - ((rowCy - (rect.height / 2)) * previewZoom);

  clampPreviewPan();
  applyPreviewTransform();
  drawBoundingBoxes();
  return true;
}

function drawBoundingBoxes() {
  const ctx = previewCanvas.getContext('2d');
  const pageFile = pageList[currentPageIndex];
  if (!ctx || !pageFile || !previewImage.naturalWidth) {
    clearCanvas();
    return;
  }

  const previewWrap = previewCanvas.parentElement;
  if (!previewWrap) {
    clearCanvas();
    return;
  }

  const wrapRect = previewWrap.getBoundingClientRect();
  const rect = getRenderedImageRect(previewImage);
  const dpr = window.devicePixelRatio || 1;
  const baseLeft = rect.left - wrapRect.left;
  const baseTop = rect.top - wrapRect.top;
  const centerX = baseLeft + (rect.width / 2) + previewPanX;
  const centerY = baseTop + (rect.height / 2) + previewPanY;
  const drawW = Math.max(1, Math.round(rect.width * previewZoom));
  const drawH = Math.max(1, Math.round(rect.height * previewZoom));
  const canvasLeft = Math.round(centerX - (drawW / 2));
  const canvasTop = Math.round(centerY - (drawH / 2));

  previewCanvas.width = Math.round(drawW * dpr);
  previewCanvas.height = Math.round(drawH * dpr);
  previewCanvas.style.width = `${drawW}px`;
  previewCanvas.style.height = `${drawH}px`;
  previewCanvas.style.left = `${canvasLeft}px`;
  previewCanvas.style.top = `${canvasTop}px`;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  syncPreviewColumnsRulerGeometry(canvasLeft, drawW);

  ctx.clearRect(0, 0, previewCanvas.width, previewCanvas.height);

  // Show only active row highlight in preview (no full bbox set, no row labels).
  const pageKey = pageFile.replace('.png', '');
  const activeRow = parsedRows.find((r) => r.row_key === activeRowKey);
  if (activeRow && activeRow.page === pageKey && activeRow.global_row_id) {
    const activeBox = resolveActiveRowBox(pageKey, activeRow);
    if (activeBox) {
      const x1 = activeBox.x1 * drawW;
      const y1 = activeBox.y1 * drawH;
      const x2 = activeBox.x2 * drawW;
      const y2 = activeBox.y2 * drawH;
      const width = Math.max(1, x2 - x1);
      const height = Math.max(1, y2 - y1);

      ctx.fillStyle = 'rgba(34, 197, 94, 0.18)';
      ctx.fillRect(x1, y1, width, height);
      ctx.strokeStyle = 'rgba(22, 163, 74, 0.9)';
      ctx.lineWidth = 1;
      ctx.strokeRect(x1, y1, width, height);
    }
  }

  drawGuideLinesAndSections(ctx, pageKey, drawW, drawH);

  if (flattenMode) {
    drawFlattenOverlay(ctx);
  }

  previewOverlayDataUrl = '';
}

function getRenderedImageRect(imgEl) {
  const container = previewWrap || imgEl.parentElement;
  const rect = container ? container.getBoundingClientRect() : imgEl.getBoundingClientRect();
  const boxW = container ? container.clientWidth : rect.width;
  const boxH = container ? container.clientHeight : rect.height;
  const naturalW = imgEl.naturalWidth || boxW;
  const naturalH = imgEl.naturalHeight || boxH;
  if (!boxW || !boxH || !naturalW || !naturalH) {
    return rect;
  }

  const boxAspect = boxW / boxH;
  const imageAspect = naturalW / naturalH;
  let renderW = boxW;
  let renderH = boxH;
  let offsetX = 0;
  let offsetY = 0;

  if (imageAspect > boxAspect) {
    renderW = boxW;
    renderH = boxW / imageAspect;
    offsetY = (boxH - renderH) / 2;
  } else {
    renderH = boxH;
    renderW = boxH * imageAspect;
    offsetX = (boxW - renderW) / 2;
  }

  return {
    left: rect.left + offsetX,
    top: rect.top + offsetY,
    width: renderW,
    height: renderH,
    right: rect.left + offsetX + renderW,
    bottom: rect.top + offsetY + renderH,
  };
}

function syncPageSelect() {
  if (!pageSelect) return;
  const needsRebuild = pageSelect.options.length !== (pageList.length + 1);
  if (needsRebuild) {
    pageSelect.innerHTML = '<option value="">Page</option>';
    pageList.forEach((_, idx) => {
      const opt = document.createElement('option');
      opt.value = String(idx);
      opt.textContent = `Page ${idx + 1}`;
      pageSelect.appendChild(opt);
    });
  }
  pageSelect.disabled = pageList.length === 0;
  pageSelect.value = pageList.length ? String(currentPageIndex) : '';
}

function applyPreviewTransform() {
  const transform = `translate(${Math.round(previewPanX)}px, ${Math.round(previewPanY)}px) scale(${previewZoom.toFixed(3)})`;
  previewImage.style.transform = transform;
  previewCanvas.style.transform = 'none';
  if (zoomLevel) {
    zoomLevel.textContent = `${Math.round(previewZoom * 100)}%`;
  }
  if (previewWrap) {
    previewWrap.classList.toggle(
      'pannable',
      previewZoom > getFillPreviewZoom() + 0.001 && !flattenMode && activeGuideTool === 'none'
    );
    if (!isPreviewPanning) {
      previewWrap.classList.remove('panning');
    }
  }
}

function resetPreviewTransform() {
  const fillZoom = getFillPreviewZoom();
  previewZoom = Math.max(PREVIEW_ZOOM_MIN, Math.min(PREVIEW_ZOOM_MAX, fillZoom));
  previewPanX = 0;
  previewPanY = 0;
  stopPreviewPan();
  applyPreviewTransform();
  if (pageList.length && previewImage.naturalWidth) {
    drawBoundingBoxes();
  }
}

function clampPreviewPan() {
  const fillZoom = getFillPreviewZoom();
  if (!previewWrap || !previewImage.naturalWidth || previewZoom < fillZoom - 0.001) {
    previewPanX = 0;
    previewPanY = 0;
    return;
  }
  const rect = getRenderedImageRect(previewImage);
  const maxPanX = Math.max(0, ((rect.width * previewZoom) - rect.width) / 2);
  const maxPanY = Math.max(0, ((rect.height * previewZoom) - rect.height) / 2);
  previewPanX = clamp(previewPanX, -maxPanX, maxPanX);
  previewPanY = clamp(previewPanY, -maxPanY, maxPanY);
}

function stopPreviewPan() {
  if (!isPreviewPanning) return;
  isPreviewPanning = false;
  if (previewWrap) {
    previewWrap.classList.remove('panning');
  }
}

function getFillPreviewZoom() {
  if (!previewWrap || !previewImage.naturalWidth) return 1;
  const rect = getRenderedImageRect(previewImage);
  const wrapW = previewWrap.clientWidth || 1;
  const wrapH = previewWrap.clientHeight || 1;
  if (rect.width <= 0 || rect.height <= 0) return 1;
  // Contain behavior: ensure the full page remains visible in the panel.
  const fillZoom = Math.min(wrapW / rect.width, wrapH / rect.height);
  return Math.max(PREVIEW_ZOOM_MIN, Math.min(PREVIEW_ZOOM_MAX, fillZoom));
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function getDefaultTopPanY(zoom) {
  if (!previewWrap || !previewImage.naturalWidth) return 0;
  const rect = getRenderedImageRect(previewImage);
  const maxPanY = Math.max(0, ((rect.height * zoom) - rect.height) / 2);
  return maxPanY > 0 ? maxPanY : 0;
}

function stepPreviewZoom(delta) {
  if (flattenMode || !pageList.length || !previewImage.naturalWidth) return;
  const fillZoom = getFillPreviewZoom();
  const nextZoom = clamp(previewZoom + delta, PREVIEW_ZOOM_MIN, PREVIEW_ZOOM_MAX);
  if (Math.abs(nextZoom - previewZoom) < 0.001) return;
  previewZoom = nextZoom;
  if (previewZoom < fillZoom - 0.001) {
    previewPanX = 0;
    previewPanY = 0;
  }
  clampPreviewPan();
  applyPreviewTransform();
  drawBoundingBoxes();
}

function clearCanvas() {
  const ctx = previewCanvas.getContext('2d');
  if (!ctx) return;
  ctx.clearRect(0, 0, previewCanvas.width, previewCanvas.height);
  previewOverlayDataUrl = '';
}

function setPreviewEmptyState() {
  previewImage.removeAttribute('src');
  previewImage.removeAttribute('data-src');
  previewImage.removeAttribute('data-loaded-src');
  previewImage.removeAttribute('data-requested-src');
  previewImage.style.display = 'none';
  clearPreviewAspectRatio();
  resetPreviewTransform();
  previewCanvas.style.left = '0px';
  previewCanvas.style.top = '0px';
  pageIndicator.textContent = '0/0';
  clearCanvas();
  updateFlattenButtons();
  renderSectionOcrResultForPage('');
  updateGuideSectionsInfo();
  renderPreviewColumnsRuler();
  renderPreviewRowsRuler();
  updatePreviewInteractionMode();
}

function drawFlattenOverlay(ctx) {
  const pointsPx = flattenPoints.map((p) => ({
    x: p.x * previewCanvas.width,
    y: p.y * previewCanvas.height,
  }));

  if (pointsPx.length > 1) {
    ctx.strokeStyle = '#2e4bff';
    ctx.lineWidth = 1.6;
    ctx.beginPath();
    ctx.moveTo(pointsPx[0].x, pointsPx[0].y);
    for (let i = 1; i < pointsPx.length; i += 1) {
      ctx.lineTo(pointsPx[i].x, pointsPx[i].y);
    }
    if (pointsPx.length === 4) {
      ctx.closePath();
    }
    ctx.stroke();
  }

  pointsPx.forEach((pt, idx) => {
    ctx.fillStyle = '#2e4bff';
    ctx.beginPath();
    ctx.arc(pt.x, pt.y, 4, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = '#1f2937';
    ctx.font = '600 10px "Manrope", sans-serif';
    ctx.fillText(String(idx + 1), pt.x + 6, pt.y - 6);
  });
}

function updateFlattenButtons() {
  const textOnlyMode = isTextOnlyToolsMode();
  if (flattenModeBtn) {
    flattenModeBtn.textContent = flattenMode ? 'Cancel' : 'Flatten';
    flattenModeBtn.disabled = flattenBusy || !pageList.length || textOnlyMode;
  }
  if (applyFlattenBtn) {
    applyFlattenBtn.disabled = flattenBusy || !flattenMode || flattenPoints.length !== 4 || textOnlyMode;
  }
  if (resetFlattenBtn) {
    resetFlattenBtn.disabled = flattenBusy || !pageList.length || textOnlyMode;
  }
  if (flattenMode) {
    stopPreviewPan();
  }
  applyPreviewTransform();
  updatePreviewInteractionMode();
}

async function refreshCurrentPageData(pageKey) {
  const [parsedRes, boundsRes] = await Promise.all([
    fetchAuthed(`/jobs/${currentJobId}/parsed/${pageKey}`),
    fetchAuthed(`/jobs/${currentJobId}/rows/${pageKey}/bounds`)
  ]);

  pageImageVersion[pageKey] = (pageImageVersion[pageKey] || 0) + 1;
  clearPreviewCacheForPage(pageKey);

  if (!parsedRes.ok) {
    boundsByPage[pageKey] = [];
    renderCurrentPage();
    return;
  }

  const rows = await parsedRes.json();
  const bounds = boundsRes.ok ? await boundsRes.json() : [];

  const existingManualRows = parsedRows.filter((r) => r.page === pageKey && !r.global_row_id);
  const retainedOtherPages = parsedRows.filter((r) => r.page !== pageKey);

  const refreshedRows = [];
  rows.forEach((row) => {
    const mapped = {
      ...row,
      row_key: createRowKey(),
      global_row_id: row.row_id || '',
      page: pageKey,
      page_row_id: row.row_id || '',
    };
    normalizeRowDisplayValues(mapped);
    refreshedRows.push(mapped);
  });

  parsedRows = [...retainedOtherPages, ...refreshedRows, ...existingManualRows];
  boundsByPage[pageKey] = Array.isArray(bounds) ? bounds : [];
  maybeAutoSeedHorizontalGuides(pageKey, { redraw: false, invalidate: false });
  rebuildPageRowMap();
  renderRows(parsedRows);
}

function scrollToResults() {
  if (!resultsSection) return;
  resultsSection.scrollIntoView({
    behavior: 'smooth',
    block: 'start'
  });
}

function computeSummary(rows) {
  return {
    total_transactions: rows.length,
    debit_transactions: countAmountTransactions(rows, 'debit'),
    credit_transactions: countAmountTransactions(rows, 'credit'),
    adb: computeAverageDailyBalanceNumber(rows),
    monthly: computeMonthlySummary(rows).map((item) => ({
      month: item.monthLabel,
      debit: item.debit,
      credit: item.credit,
      avg_debit: item.avgDebit,
      avg_credit: item.avgCredit,
      adb: item.adb,
    })),
  };
}

function computeAverageDailyBalanceNumber(rows) {
  const daily = buildDailyBalances(rows);
  if (!daily.length) return null;
  let weightedTotal = 0;
  let totalDays = 0;
  for (let i = 0; i < daily.length; i += 1) {
    const current = daily[i];
    const nextDate = i < daily.length - 1 ? daily[i + 1].date : addDaysUTC(current.date, 1);
    const days = Math.max(1, diffDaysUTC(current.date, nextDate));
    weightedTotal += current.balance * days;
    totalDays += days;
  }
  if (!totalDays) return null;
  return weightedTotal / totalDays;
}

function computeAverageDailyBalance(rows) {
  const adb = computeAverageDailyBalanceNumber(rows);
  if (!Number.isFinite(adb)) return '-';
  return formatMoney(adb, null, null, true);
}

function buildDailyBalances(rows) {
  const datedBalances = [];

  rows.forEach((row, idx) => {
    const balance = normalizeAmount(row.balance || '');
    const date = parseStatementDate(row.date || '');
    if (!Number.isFinite(balance) || !date) return;
    datedBalances.push({ idx, date, balance });
  });

  if (!datedBalances.length) return [];

  datedBalances.sort((a, b) => {
    const diff = a.date.getTime() - b.date.getTime();
    if (diff !== 0) return diff;
    return a.idx - b.idx;
  });

  const daily = [];
  datedBalances.forEach((entry) => {
    const key = toDateKey(entry.date);
    if (daily.length && daily[daily.length - 1].key === key) {
      daily[daily.length - 1].balance = entry.balance;
      return;
    }
    daily.push({ key, date: entry.date, balance: entry.balance });
  });
  return daily;
}

function computeMonthlySummary(rows) {
  const transactions = [];
  rows.forEach((row, idx) => {
    const date = parseStatementDate(row.date || '');
    if (!date) return;
    const debit = normalizeSummaryAmount(row, 'debit');
    const credit = normalizeSummaryAmount(row, 'credit');
    transactions.push({
      idx,
      date,
      debit,
      credit,
    });
  });
  if (!transactions.length) return [];

  transactions.sort((a, b) => {
    const diff = a.date.getTime() - b.date.getTime();
    if (diff !== 0) return diff;
    return a.idx - b.idx;
  });

  const monthMap = new Map();
  const firstDate = transactions[0].date;
  const lastDate = transactions[transactions.length - 1].date;
  seedMonthBuckets(monthMap, firstDate, lastDate);

  transactions.forEach((tx) => {
    const bucket = monthMap.get(monthKey(tx.date));
    if (!bucket) return;
    if (Number.isFinite(tx.debit)) {
      const amount = Math.abs(tx.debit);
      if (amount > 0) {
        bucket.debit += amount;
        bucket.debitCount += 1;
      }
    }
    if (Number.isFinite(tx.credit)) {
      const amount = Math.abs(tx.credit);
      if (amount > 0) {
        bucket.credit += amount;
        bucket.creditCount += 1;
      }
    }
  });

  const daily = buildDailyBalances(rows);
  for (let i = 0; i < daily.length; i += 1) {
    const current = daily[i];
    const nextDate = i < daily.length - 1 ? daily[i + 1].date : addDaysUTC(current.date, 1);
    allocateMonthlyBalanceRange(monthMap, current.date, nextDate, current.balance);
  }

  return Array.from(monthMap.values())
    .filter((bucket) => bucket.days > 0 || bucket.debit > 0 || bucket.credit > 0)
    .map((bucket) => ({
      monthLabel: bucket.label,
      debit: bucket.debit,
      credit: bucket.credit,
      avgDebit: bucket.debitCount > 0 ? (bucket.debit / bucket.debitCount) : 0,
      avgCredit: bucket.creditCount > 0 ? (bucket.credit / bucket.creditCount) : 0,
      adb: bucket.days > 0 ? (bucket.weightedBalance / bucket.days) : 0,
    }));
}

function formatMoney(parsedValue, rawA, rawB, absolute = false) {
  if (!Number.isFinite(parsedValue)) {
    const fallback = normalizeAmount((rawA || rawB || '').toString().trim());
    if (!Number.isFinite(fallback)) return '-';
    const fallbackValue = absolute ? Math.abs(fallback) : fallback;
    return formatPesoValue(fallbackValue, absolute);
  }
  const value = absolute ? Math.abs(parsedValue) : parsedValue;
  return formatPesoValue(value, absolute);
}

function formatPesoValue(value, absolute = false) {
  const formatter = new Intl.NumberFormat('en-PH', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });
  const sign = absolute ? '' : (value < 0 ? '-' : '+');
  return `${sign}₱${formatter.format(Math.abs(value))}`;
}

function normalizeAmount(value) {
  if (value === null || value === undefined) return NaN;
  const raw = String(value).trim();
  if (!raw) return NaN;

  const hasParenNegative = /\(\s*[\d,.\-]+\s*\)/.test(raw);
  const numericTokens = raw.match(/-?\d[\d,]*(?:\.\d+)?/g) || [];
  if (!numericTokens.length) return NaN;

  // Prefer explicit money-like tokens (decimal or comma-grouped) and use the last one.
  const moneyLike = numericTokens.filter((token) => /\.\d{1,}$/.test(token) || /,\d{3}/.test(token));
  const candidates = (moneyLike.length ? moneyLike : numericTokens).filter((token) => {
    const compact = token.replace(/[^0-9]/g, '');
    const hasDecimal = token.includes('.');
    // Ignore likely identifiers (e.g., long account/reference numbers) when not decimal amounts.
    if (!hasDecimal && compact.length >= 11) return false;
    return compact.length > 0;
  });
  if (!candidates.length) return NaN;

  const chosen = candidates[candidates.length - 1].replace(/,/g, '');
  const parsed = Number.parseFloat(chosen);
  if (!Number.isFinite(parsed)) return NaN;
  if (hasParenNegative && parsed > 0) return -parsed;
  return parsed;
}

function parseStatementDate(value) {
  const normalized = String(value || '').trim().toUpperCase().replace(/\s+/g, ' ');
  if (!normalized) return null;

  const dateOnly = normalized.split(',')[0].trim();
  const monthMap = {
    JAN: 1, FEB: 2, MAR: 3, APR: 4, MAY: 5, JUN: 6,
    JUL: 7, AUG: 8, SEP: 9, OCT: 10, NOV: 11, DEC: 12
  };

  let m = dateOnly.match(/^(\d{4})\/(\d{1,2})\/(\d{1,2})$/);
  if (m) return makeUtcDate(Number(m[1]), Number(m[2]), Number(m[3]));

  m = dateOnly.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (m) return makeUtcDate(Number(m[1]), Number(m[2]), Number(m[3]));

  m = dateOnly.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (m) return makeUtcDate(normalizeYear(m[3]), Number(m[1]), Number(m[2]));

  m = dateOnly.match(/^(\d{1,2})-(\d{1,2})-(\d{2,4})$/);
  if (m) return makeUtcDate(normalizeYear(m[3]), Number(m[2]), Number(m[1]));

  m = dateOnly.match(/^(\d{1,2})\s+([A-Z]{3,9})\s+(\d{2,4})$/);
  if (m) {
    const mon = monthMap[m[2].slice(0, 3)];
    if (!mon) return null;
    return makeUtcDate(normalizeYear(m[3]), mon, Number(m[1]));
  }

  return null;
}

function normalizeYear(rawYear) {
  const n = Number(rawYear);
  if (!Number.isFinite(n)) return NaN;
  if (n >= 100) return n;
  return n <= 79 ? 2000 + n : 1900 + n;
}

function makeUtcDate(year, month, day) {
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  const d = new Date(Date.UTC(year, month - 1, day));
  if (d.getUTCFullYear() !== year || d.getUTCMonth() !== month - 1 || d.getUTCDate() !== day) {
    return null;
  }
  return d;
}

function diffDaysUTC(start, end) {
  const msPerDay = 24 * 60 * 60 * 1000;
  return Math.round((end.getTime() - start.getTime()) / msPerDay);
}

function addDaysUTC(date, days) {
  return new Date(date.getTime() + (days * 24 * 60 * 60 * 1000));
}

function toDateKey(date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, '0');
  const d = String(date.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function monthKey(date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, '0');
  return `${y}-${m}`;
}

function monthLabel(date) {
  return date.toLocaleDateString('en-US', {
    month: 'short',
    year: 'numeric',
    timeZone: 'UTC',
  });
}

function seedMonthBuckets(monthMap, firstDate, lastDate) {
  let cursor = new Date(Date.UTC(firstDate.getUTCFullYear(), firstDate.getUTCMonth(), 1));
  const monthCap = new Date(Date.UTC(lastDate.getUTCFullYear(), lastDate.getUTCMonth(), 1));
  while (cursor.getTime() <= monthCap.getTime()) {
    const key = monthKey(cursor);
    monthMap.set(key, {
      label: monthLabel(cursor),
      debit: 0,
      credit: 0,
      debitCount: 0,
      creditCount: 0,
      weightedBalance: 0,
      days: 0,
    });
    cursor = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth() + 1, 1));
  }
}

function allocateMonthlyBalanceRange(monthMap, startDate, endDate, balance) {
  if (!Number.isFinite(balance)) return;
  let cursor = new Date(startDate.getTime());

  while (cursor.getTime() < endDate.getTime()) {
    const monthStart = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth(), 1));
    const nextMonthStart = new Date(Date.UTC(monthStart.getUTCFullYear(), monthStart.getUTCMonth() + 1, 1));
    const segmentEnd = endDate.getTime() < nextMonthStart.getTime() ? endDate : nextMonthStart;
    const days = Math.max(1, diffDaysUTC(cursor, segmentEnd));
    const bucket = monthMap.get(monthKey(cursor));
    if (bucket) {
      bucket.weightedBalance += balance * days;
      bucket.days += days;
    }
    cursor = segmentEnd;
  }
}

function makeEditableCell(row, field, isAmount = false) {
  const cell = document.createElement('div');
  cell.className = `table-cell ${isAmount ? 'amount-input-cell' : ''}`;
  cell.dataset.rowKey = row.row_key;
  cell.dataset.field = field;

  const input = document.createElement('input');
  input.type = 'text';
  input.className = `table-input table-input-${field}`;
  input.value = getDisplayValue(row, field);
  input.dataset.rowKey = row.row_key;
  input.dataset.field = field;
  const isExistingOcrRow = Boolean(row.global_row_id);
  const shouldHidePlaceholder = isExistingOcrRow && (field === 'debit' || field === 'credit');
  input.placeholder = shouldHidePlaceholder ? '' : field.toUpperCase();

  input.addEventListener('click', (e) => {
    e.stopPropagation();
  });

  input.addEventListener('input', () => {
    row[field] = input.value.trim();
    markActivePageDirty();
    updateSummaryFromRows(parsedRows);
  });

  input.addEventListener('focus', () => {
    document.querySelectorAll('.table-cell.active-cell').forEach((el) => el.classList.remove('active-cell'));
    cell.classList.add('active-cell');
    if (activeRowKey !== row.row_key) {
      selectRow(row.row_key);
    }
  });

  input.addEventListener('blur', () => {
    cell.classList.remove('active-cell');
  });

  input.addEventListener('keydown', (e) => {
    handleTableInputKeydown(e, row.row_key, field);
  });

  input.addEventListener('blur', () => {
    const normalized = normalizeFieldValue(field, input.value);
    row[field] = normalized;
    input.value = normalized;
    markActivePageDirty();
    updateSummaryFromRows(parsedRows);
  });

  input.addEventListener('change', () => {
    const normalized = normalizeFieldValue(field, input.value);
    row[field] = normalized;
    markActivePageDirty();
    updateSummaryFromRows(parsedRows);
  });

  cell.appendChild(input);
  return cell;
}

function makeRowActionsMenu(rowKey) {
  const wrap = document.createElement('div');
  wrap.className = 'row-actions-inline';
  wrap.appendChild(makeRowActionButton('fa-solid fa-plus', 'Insert row', 'insert', () => {
    insertRowAfter(rowKey);
  }));
  wrap.appendChild(makeRowActionButton('fa-solid fa-trash', 'Delete row', 'delete', () => {
    deleteRowByKey(rowKey);
  }));
  return wrap;
}

function closeAllRowMenus() {
  // No-op for icon-based row actions.
}

function makeRowActionButton(iconClass, ariaLabel, kind, onClick) {
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = `row-action row-action-${kind}`;
  btn.setAttribute('aria-label', ariaLabel);
  btn.title = ariaLabel;
  btn.innerHTML = `<i class="${iconClass}" aria-hidden="true"></i>`;
  btn.addEventListener('click', (e) => {
    e.stopPropagation();
    onClick();
  });
  return btn;
}

function insertRowAfter(rowKey) {
  const idx = parsedRows.findIndex((r) => r.row_key === rowKey);
  if (idx < 0) return;

  const base = parsedRows[idx] || {};
  const newRow = {
    row_key: createRowKey(),
    global_row_id: '',
    row_id: '',
    date: '',
    description: '',
    debit: '',
    credit: '',
    balance: '',
    page: base.page || currentPageKey(),
    page_row_id: '',
  };
  parsedRows.splice(idx + 1, 0, newRow);
  markActivePageDirty();
  rebuildPageRowMap();
  renderRows(parsedRows);
  highlightSelectedTableRow();
  renderCurrentPage();
}

function deleteRowByKey(rowKey) {
  const idx = parsedRows.findIndex((r) => r.row_key === rowKey);
  if (idx < 0) return;
  parsedRows.splice(idx, 1);
  markActivePageDirty();
  rebuildPageRowMap();

  if (!parsedRows.length) {
    activeRowKey = null;
  } else if (activeRowKey === rowKey) {
    const next = parsedRows[Math.min(idx, parsedRows.length - 1)];
    activeRowKey = next ? next.row_key : null;
  }

  renderRows(parsedRows);
  highlightSelectedTableRow();
  renderCurrentPage();
}

function rebuildPageRowMap() {
  pageRowToGlobal = {};
  identityBoundsByPage = {};
  let ocrCounter = 1;
  parsedRows.forEach((row) => {
    if (!row.page) {
      row.page = currentPageKey();
    }
    if (!row.row_key) {
      row.row_key = createRowKey();
    }
    if (row.page_row_id) {
      row.global_row_id = String(ocrCounter).padStart(3, '0');
      ocrCounter += 1;
    } else {
      row.global_row_id = '';
    }
    if (row.page && row.page_row_id) {
      pageRowToGlobal[`${row.page}|${row.page_row_id}`] = row.global_row_id;
    }
  });
}

function handleTableInputKeydown(e, rowKey, field) {
  if (!rowKey || !field) return;
  const col = TABLE_EDIT_FIELDS.indexOf(field);
  if (col < 0) return;

  if (e.key === 'Enter') {
    e.preventDefault();
    focusTableCellByOffset(rowKey, col, 1, 0);
    return;
  }
  if (e.key === 'ArrowDown') {
    e.preventDefault();
    focusTableCellByOffset(rowKey, col, 1, 0);
    return;
  }
  if (e.key === 'ArrowUp') {
    e.preventDefault();
    focusTableCellByOffset(rowKey, col, -1, 0);
    return;
  }
  if (e.key === 'ArrowRight' && e.altKey) {
    e.preventDefault();
    focusTableCellByOffset(rowKey, col, 0, 1);
    return;
  }
  if (e.key === 'ArrowLeft' && e.altKey) {
    e.preventDefault();
    focusTableCellByOffset(rowKey, col, 0, -1);
  }
}

function focusTableCellByOffset(rowKey, colIdx, rowDelta, colDelta) {
  const rowIndex = parsedRows.findIndex((r) => r.row_key === rowKey);
  if (rowIndex < 0) return;
  const nextRow = Math.max(0, Math.min(parsedRows.length - 1, rowIndex + rowDelta));
  const nextCol = Math.max(0, Math.min(TABLE_EDIT_FIELDS.length - 1, colIdx + colDelta));
  const targetRowKey = parsedRows[nextRow].row_key;
  const targetField = TABLE_EDIT_FIELDS[nextCol];
  const selector = `.table-input[data-row-key="${targetRowKey}"][data-field="${targetField}"]`;
  const target = document.querySelector(selector);
  if (!target) return;
  target.focus();
  if (typeof target.select === 'function') target.select();
}

function createRowKey() {
  const next = rowKeyCounter;
  rowKeyCounter += 1;
  return `row_${next}`;
}

function currentPageKey() {
  if (!pageList.length) return '';
  return pageList[Math.max(0, Math.min(currentPageIndex, pageList.length - 1))].replace('.png', '');
}

function updateSummaryFromRows(rows) {
  if (authRole === 'credit_evaluator' && evaluatorSelectedSubmission) {
    return;
  }
  totalTransactions.textContent = String(rows.length);
  totalDebitTransactions.textContent = String(countAmountTransactions(rows, 'debit'));
  totalCreditTransactions.textContent = String(countAmountTransactions(rows, 'credit'));
  endingBalance.textContent = rows.length ? computeAverageDailyBalance(rows) : '-';
  renderMonthlySummary(rows);
}

function renderMonthlySummary(rows) {
  const monthlySummaryBody = document.getElementById('monthlySummaryBody');
  const monthlySummaryWrap = document.querySelector('.monthly-summary-wrap');
  if (!monthlySummaryBody) return;
  const monthly = computeMonthlySummary(rows);
  if (!monthly.length) {
    if (monthlySummaryWrap) monthlySummaryWrap.classList.add('is-empty');
    monthlySummaryBody.innerHTML = '<tr><td class="monthly-empty" colspan="6">No monthly data</td></tr>';
    return;
  }
  if (monthlySummaryWrap) monthlySummaryWrap.classList.remove('is-empty');
  monthlySummaryBody.innerHTML = monthly.map((item) => (
    `<tr>
      <td>${escapeHtml(item.monthLabel)}</td>
      <td>${escapeHtml(formatPesoValue(Math.abs(item.debit), true))}</td>
      <td>${escapeHtml(formatPesoValue(Math.abs(item.credit), true))}</td>
      <td>${escapeHtml(formatPesoValue(Math.abs(item.avgDebit), true))}</td>
      <td>${escapeHtml(formatPesoValue(Math.abs(item.avgCredit), true))}</td>
      <td>${escapeHtml(formatPesoValue(item.adb, true))}</td>
    </tr>`
  )).join('');
}

function countAmountTransactions(rows, field) {
  return rows.reduce((count, row) => {
    const amount = normalizeSummaryAmount(row, field);
    return Number.isFinite(amount) && Math.abs(amount) > 0 ? count + 1 : count;
  }, 0);
}

function normalizeSummaryAmount(row, field) {
  const amount = normalizeAmount(row && row[field]);
  if (!Number.isFinite(amount)) return NaN;
  const absAmount = Math.abs(amount);

  // Guard against reference/account-like values leaking into amount columns.
  if (absAmount >= 1_000_000_000) return NaN;

  const balance = normalizeAmount(row && row.balance);
  if (Number.isFinite(balance) && Math.abs(balance) > 0) {
    if (absAmount > Math.abs(balance) * 50) return NaN;
  }

  return amount;
}

function getDisplayValue(row, field) {
  return normalizeFieldValue(field, row[field] || '');
}

function normalizeRowDisplayValues(row) {
  row.date = normalizeFieldValue('date', row.date || '');
  row.description = normalizeFieldValue('description', row.description || '');
  row.debit = normalizeFieldValue('debit', row.debit || '');
  row.credit = normalizeFieldValue('credit', row.credit || '');
  row.balance = normalizeFieldValue('balance', row.balance || '');
}

function normalizeFieldValue(field, value) {
  const raw = String(value || '').trim();
  if (!raw) return '';

  if (field === 'date') {
    const parsed = parseStatementDate(raw);
    if (!parsed) return raw;
    const mm = String(parsed.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(parsed.getUTCDate()).padStart(2, '0');
    const yyyy = String(parsed.getUTCFullYear());
    return `${mm}/${dd}/${yyyy}`;
  }

  if (field === 'debit' || field === 'credit' || field === 'balance') {
    const parsed = normalizeAmount(raw);
    if (!Number.isFinite(parsed)) return raw;
    return new Intl.NumberFormat('en-US', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(parsed);
  }

  return raw;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function updateProgressUI(progress, labelText, step, ocrModel = null, parseMode = null, status = null) {
  if (parseMode != null) {
    setActiveParseMode(parseMode);
  }
  const clamped = Math.max(0, Math.min(100, Math.round(progress)));
  const statusKey = String((status && status.status) || step || '').toLowerCase();
  const watchSig = `${statusKey}|${String(step || '')}|${clamped}`;
  if (watchSig !== progressWatchSignature) {
    progressWatchSignature = watchSig;
    progressWatchUpdatedAt = Date.now();
    progressWatchHandled = false;
  }
  progressWatchStatus = statusKey || progressWatchStatus;
  progressFill.style.width = `${clamped}%`;
  progressPercent.textContent = `${clamped}%`;
  progressLabel.textContent = labelText;
  progressStep.textContent = `Step: ${stepToLabel(step)}`;
  if (progressModel) {
    const modeLabel = (parseMode || 'text').toString().toUpperCase();
    const showModel = modeLabel === 'OCR' || String(step || '').toLowerCase() === 'section_ocr';
    progressModel.textContent = `Mode: ${modeLabel}${showModel ? ` | OCR Model: ${ocrModel || '-'}` : ''}`;
  }
  if (analyzerStatus && progressAnalyzerRow && analyzerModel && analyzerProfile && analyzerReason) {
    const modelText = status && status.profile_analyzer_model ? status.profile_analyzer_model : '-';
    const resultText = status && status.profile_analyzer_result ? status.profile_analyzer_result : 'idle';
    const reasonText = status && status.profile_analyzer_reason ? status.profile_analyzer_reason : '-';
    const profileText = status && status.profile_selected_after_analyzer ? status.profile_selected_after_analyzer : '-';
    const triggered = Boolean(status && status.profile_analyzer_triggered);
    analyzerStatus.textContent = `AI Analyzer: ${triggered ? capitalizeWord(resultText) : 'Idle'}`;
    analyzerModel.textContent = `Model: ${modelText}`;
    analyzerProfile.textContent = `Profile: ${profileText}`;
    analyzerReason.textContent = `Reason: ${reasonText}`;
    progressAnalyzerRow.classList.toggle('is-hidden', !triggered);
  }
  if (progressElapsed && !elapsedStartMs) {
    progressElapsed.textContent = 'Elapsed: 00:00';
  }
}

function applyAnalyzerMetaToProgress(meta, ocrModel = 'tesseract', parseMode = 'ocr') {
  if (!meta || typeof meta !== 'object') return;
  const reason = String(meta.reason || '').toLowerCase();
  const showTriggered = Boolean(meta.triggered)
    || reason === 'matched_existing_profile'
    || reason === 'no_ocr_profiles_sampled'
    || reason === 'no_text_or_ocr_profiles_sampled';
  const resultText = String(meta.result || '').trim()
    || (reason === 'matched_existing_profile' ? 'matched' : 'skipped');
  let label = 'Profile check completed';
  if (reason === 'matched_existing_profile') {
    const matched = meta.profile_name ? ` (${meta.profile_name})` : '';
    label = `Profile matched${matched}`;
  } else if (resultText.toLowerCase() === 'applied') {
    label = 'AI profile created and applied';
  } else if (resultText.toLowerCase() === 'rejected') {
    label = 'AI profile proposal rejected by validation';
  } else if (resultText.toLowerCase() === 'failed') {
    label = `AI profile analyzer failed${meta.reason ? ` (${meta.reason})` : ''}`;
  } else if (reason === 'no_ocr_profiles_sampled') {
    label = 'No OCR text sampled for profile analysis';
  }
  const statusLike = {
    profile_analyzer_triggered: showTriggered,
    profile_analyzer_provider: meta.provider || '-',
    profile_analyzer_model: meta.model || '-',
    profile_analyzer_result: resultText || 'idle',
    profile_analyzer_reason: meta.reason || '-',
    profile_selected_after_analyzer: meta.profile_name || null,
  };
  updateProgressUI(
    100,
    label,
    'profile_analyzer',
    ocrModel,
    parseMode,
    statusLike
  );
}

function startElapsedTimer() {
  elapsedStartMs = Date.now();
  renderElapsedTime();
  if (elapsedTimer) {
    clearInterval(elapsedTimer);
  }
  elapsedTimer = setInterval(renderElapsedTime, 1000);
}

function stopElapsedTimer() {
  if (elapsedTimer) {
    clearInterval(elapsedTimer);
    elapsedTimer = null;
  }
  renderElapsedTime();
}

function resetElapsedTimer() {
  elapsedStartMs = 0;
  if (elapsedTimer) {
    clearInterval(elapsedTimer);
    elapsedTimer = null;
  }
  if (progressElapsed) {
    progressElapsed.textContent = 'Elapsed: 00:00';
  }
}

function renderElapsedTime() {
  if (!progressElapsed) return;
  if (!elapsedStartMs) {
    progressElapsed.textContent = 'Elapsed: 00:00';
    checkProgressWatchdog();
    return;
  }
  const totalSecs = Math.max(0, Math.floor((Date.now() - elapsedStartMs) / 1000));
  const mins = String(Math.floor(totalSecs / 60)).padStart(2, '0');
  const secs = String(totalSecs % 60).padStart(2, '0');
  progressElapsed.textContent = `Elapsed: ${mins}:${secs}`;
  checkProgressWatchdog();
}

function resetProgressUI() {
  updateProgressUI(0, 'Waiting for upload', 'queued', null);
}

function inferProgress(status, step) {
  if (step === 'draft_queued') return 2;
  if (step === 'draft_pdf_to_images') return 35;
  if (step === 'draft_image_cleaning') return 80;
  if (step === 'ready_for_edit' || status === 'draft') return 100;
  if (status === 'queued' || step === 'queued') return 0;
  if (status === 'for_review' || step === 'for_review') return 0;
  if (status === 'summary_generated') return 100;
  if (status === 'done') return 100;
  if (step === 'pdf_to_images') return 15;
  if (step === 'image_cleaning') return 40;
  if (step === 'text_extraction') return 65;
  if (step === 'page_ocr') return 80;
  if (step === 'page_text') return 80;
  if (step === 'parsing' || step === 'saving_results') return 92;
  return 10;
}

function stepToLabel(step) {
  const key = String(step || '').toLowerCase();
  if (key === 'queued') return 'Queued';
  if (key === 'for_review') return 'For Review';
  if (key === 'draft_queued') return 'Draft queued';
  if (key === 'draft_pdf_to_images') return 'Preparing preview pages';
  if (key === 'draft_image_cleaning') return 'Cleaning draft pages';
  if (key === 'ready_for_edit') return 'Ready for edit';
  if (key === 'uploading') return 'Uploading';
  if (key === 'pdf_to_images') return 'Converting PDF to images';
  if (key === 'image_cleaning') return 'Cleaning page images';
  if (key === 'text_extraction') return 'Extracting PDF text';
  if (key === 'account_identity_ai') return 'Extracting account identity';
  if (key === 'profile_analyzer') return 'Analyzing unknown bank structure';
  if (key === 'page_ocr') return 'Running OCR per page';
  if (key === 'section_ocr') return 'OCR in selected sections';
  if (key === 'page_text') return 'Parsing text per page';
  if (key === 'saving_results') return 'Saving results';
  if (key === 'parsing') return 'Parsing extracted rows';
  if (key === 'completed' || key === 'done') return 'Completed';
  if (key === 'summary_generated') return 'Summary Generated';
  if (key === 'failed') return 'Failed';
  return 'Processing';
}

function capitalizeWord(value) {
  const text = String(value || '').trim();
  if (!text) return '';
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function formatSubmissionStatus(status) {
  const raw = String(status || '').trim();
  if (!raw) return '-';
  return raw
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function resetResults() {
  clearActivePageAutosaveTimer();
  if (sectionOcrProgressTimer) {
    clearInterval(sectionOcrProgressTimer);
    sectionOcrProgressTimer = null;
  }
  sectionOcrProgressValue = 0;
  totalTransactions.textContent = '0';
  totalDebitTransactions.textContent = '0';
  totalCreditTransactions.textContent = '0';
  endingBalance.textContent = '-';
  if (accountNameSummary) accountNameSummary.textContent = '-';
  if (accountNumberSummary) accountNumberSummary.textContent = '-';
  tableBody.innerHTML = '';
  pageList = [];
  parsedRows = [];
  rowsByPage = {};
  boundsByPage = {};
  pageRowToGlobal = {};
  guideLinesByPage = {};
  guideHistoryByPage = {};
  columnLayoutByPage = {};
  columnDragState = { sourceKey: '', targetKey: '' };
  columnResizeState = null;
  columnSwapSelectKey = '';
  horizontalGuideDragState = null;
  horizontalGuideTouchedByPage = {};
  activeGuideTool = 'none';
  sectionOcrResultsByPage = {};
  imageToolInFlight = false;
  activeParseMode = 'text';
  ocrToolsUnlocked = false;
  setSectionOcrBusy(false);
  activeRowKey = null;
  currentPageIndex = 0;
  rowKeyCounter = 1;
  pageImageVersion = {};
  clearPreviewBlobCache();
  evaluatorManifest = [];
  evaluatorReviewProgress = { total_pages: 0, parsed_pages: 0, reviewed_pages: 0, percent: 0 };
  evaluatorCanExport = false;
  activePageKey = '';
  activePageReviewStatus = 'pending';
  activePageUpdatedAt = null;
  activePageDirty = false;
  activePageDirtyRevision = 0;
  activePageSaveInFlight = false;
  activePageSavePromise = null;
  clearEvaluatorManifestTimer();
  renderActivePageSavedMark();
  flattenMode = false;
  flattenPoints = [];
  flattenBusy = false;
  ocrStarted = false;
  finishSave.textContent = 'Start OCR';
  resetElapsedTimer();
  shouldAutoScrollToResults = false;
  hasSeenInFlightStatus = false;
  updateSummaryFromRows([]);
  setSummaryLocked(authRole === 'credit_evaluator');
  updateFlattenButtons();
  updateGuideToolButtons();
  renderCurrentPage();
}

function buildPreviewSrc(pageKey) {
  return `/jobs/${currentJobId}/preview/${pageKey}?v=${pageImageVersion[pageKey] || 0}`;
}

function clearPreviewBlobCache() {
  prefetchedPreviewSrcs.clear();
  for (const url of previewBlobUrlCache.values()) {
    URL.revokeObjectURL(url);
  }
  previewBlobUrlCache.clear();
}

function clearPreviewCacheForPage(pageKey) {
  const marker = `/preview/${pageKey}?`;
  for (const [src, objectUrl] of previewBlobUrlCache.entries()) {
    if (!src.includes(marker)) continue;
    URL.revokeObjectURL(objectUrl);
    previewBlobUrlCache.delete(src);
    prefetchedPreviewSrcs.delete(src);
  }
}

async function getPreviewBlobUrl(src) {
  const cached = previewBlobUrlCache.get(src);
  if (cached) return cached;
  const res = await fetchAuthed(src);
  if (!res.ok) {
    throw new Error(`Preview load failed (${res.status})`);
  }
  const blob = await res.blob();
  const objectUrl = URL.createObjectURL(blob);
  previewBlobUrlCache.set(src, objectUrl);
  return objectUrl;
}

async function loadPreviewImageForPage(pageKey) {
  if (!currentJobId) return;
  const src = buildPreviewSrc(pageKey);
  if (previewImage.dataset.loadedSrc === src && previewImage.src) {
    previewImage.style.display = 'block';
    setPreviewAspectRatioFromImage();
    drawBoundingBoxes();
    applyPreviewTransform();
    return;
  }
  resetPreviewTransform();
  previewImage.style.display = 'none';
  previewImage.dataset.requestedSrc = src;
  try {
    const objectUrl = await getPreviewBlobUrl(src);
    if (previewImage.dataset.requestedSrc !== src) return;
    previewImage.dataset.loadedSrc = src;
    previewImage.src = objectUrl;
  } catch (err) {
    if (previewImage.dataset.requestedSrc === src) {
      setPreviewEmptyState();
    }
    console.warn(err.message || 'Failed to load preview image');
  }
}

function prefetchPreviewPageByIndex(idx) {
  if (!currentJobId || idx < 0 || idx >= pageList.length) return;
  const pageKey = pageList[idx].replace('.png', '');
  const src = buildPreviewSrc(pageKey);
  if (prefetchedPreviewSrcs.has(src)) return;
  prefetchedPreviewSrcs.add(src);
  getPreviewBlobUrl(src).catch(() => {
    prefetchedPreviewSrcs.delete(src);
  });
}

function prefetchPreviewNeighbors() {
  prefetchPreviewPageByIndex(currentPageIndex + 1);
  prefetchPreviewPageByIndex(currentPageIndex - 1);
  prefetchPreviewPageByIndex(currentPageIndex + 2);
}

function formatFileSize(bytes) {
  if (!bytes) return '0 Bytes';
  const units = ['Bytes', 'KB', 'MB', 'GB'];
  const idx = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, idx)).toFixed(1)} ${units[idx]}`;
}

async function safeParseJson(res) {
  try {
    return await res.json();
  } catch (e) {
    return null;
  }
}

async function markSummaryGenerated() {
  if (!evaluatorSelectedSubmission || !evaluatorSelectedSubmission.id) return;
  const res = await fetchAuthed(`/evaluator/submissions/${evaluatorSelectedSubmission.id}/mark-summary-generated`, {
    method: 'POST',
  });
  const body = await safeParseJson(res);
  if (!res.ok) {
    throw new Error((body && body.detail) || 'Failed to mark summary generated');
  }
  evaluatorSelectedSubmission = body;
  await loadEvaluatorSubmissions();
}

async function getEvaluatorExportPayload() {
  if (!(authRole === 'credit_evaluator' && evaluatorSelectedSubmission && evaluatorSelectedSubmission.id)) {
    return null;
  }
  const submissionId = evaluatorSelectedSubmission.id;
  const reviewRes = await fetchAuthed(`/evaluator/submissions/${submissionId}/review-status`);
  const reviewBody = await safeParseJson(reviewRes);
  if (!reviewRes.ok) {
    throw new Error((reviewBody && reviewBody.detail) || 'Failed to check review status');
  }
  if (!reviewBody.can_export) {
    throw new Error('Review all pages to enable export.');
  }

  const manifestRes = await fetchAuthed(`/evaluator/submissions/${submissionId}/pages`);
  const manifestBody = await safeParseJson(manifestRes);
  if (!manifestRes.ok) {
    throw new Error((manifestBody && manifestBody.detail) || 'Failed to load page manifest');
  }
  const pages = Array.isArray(manifestBody.pages) ? manifestBody.pages : [];
  const allRows = [];
  for (const page of pages) {
    const pageKey = page && page.page_key ? page.page_key : '';
    if (!pageKey) continue;
    const pageRes = await fetchAuthed(`/evaluator/submissions/${submissionId}/pages/${pageKey}`);
    const pageBody = await safeParseJson(pageRes);
    if (!pageRes.ok) continue;
    const rows = Array.isArray(pageBody.rows) ? pageBody.rows : [];
    rows.forEach((row, idx) => {
      allRows.push({
        row_key: createRowKey(),
        global_row_id: String(row.row_id || idx + 1).padStart(3, '0'),
        row_id: String(row.row_id || idx + 1).padStart(3, '0'),
        date: row.date || '',
        description: row.description || '',
        debit: row.debit != null ? String(row.debit) : '',
        credit: row.credit != null ? String(row.credit) : '',
        balance: row.balance != null ? String(row.balance) : '',
        page: pageKey,
        page_row_id: String(row.row_id || idx + 1).padStart(3, '0'),
      });
    });
  }

  const subRes = await fetchAuthed(`/evaluator/submissions/${submissionId}`);
  const subBody = await safeParseJson(subRes);
  const summary = subRes.ok && subBody ? (subBody.summary || null) : null;
  return { rows: allRows, summary };
}

async function exportToPdf() {
  let rowsForExport = parsedRows.slice();
  let summaryForExport = null;

  if (authRole === 'credit_evaluator' && evaluatorSelectedSubmission && evaluatorSelectedSubmission.id) {
    try {
      const payload = await getEvaluatorExportPayload();
      rowsForExport = payload && Array.isArray(payload.rows) ? payload.rows : [];
      summaryForExport = payload ? payload.summary : null;
    } catch (err) {
      alert((err && err.message) || 'Export blocked');
      return;
    }
  }

  if (!rowsForExport.length) {
    alert('No extracted rows to export yet.');
    return;
  }

  if (!window.jspdf || typeof window.jspdf.jsPDF !== 'function') {
    alert('PDF library is not loaded.');
    return;
  }

  const doc = new window.jspdf.jsPDF({
    orientation: 'portrait',
    unit: 'pt',
    format: 'a4'
  });

  const marginLeft = 40;
  let y = 42;

  doc.setFont('helvetica', 'bold');
  doc.setFontSize(14);
  doc.text('Account Summary', marginLeft, y);
  y += 16;

  doc.setFont('helvetica', 'normal');
  doc.setFontSize(9);
  const generatedAt = new Date();
  doc.text(
    `Generated: ${generatedAt.toLocaleDateString()} ${generatedAt.toLocaleTimeString()}`,
    marginLeft,
    y
  );
  y += 14;

  const computedSummary = summaryForExport || computeSummary(rowsForExport);
  const summaryRows = [
    ['Account Name', pdfSafeText((accountNameSummary && accountNameSummary.textContent ? accountNameSummary.textContent : '-').trim())],
    ['Account Number', pdfSafeText((accountNumberSummary && accountNumberSummary.textContent ? accountNumberSummary.textContent : '-').trim())],
    ['Total Transactions', pdfSafeText(String(computedSummary.total_transactions || rowsForExport.length))],
    ['Debit Transactions', pdfSafeText(String(computedSummary.debit_transactions || countAmountTransactions(rowsForExport, 'debit')))],
    ['Credit Transactions', pdfSafeText(String(computedSummary.credit_transactions || countAmountTransactions(rowsForExport, 'credit')))],
    ['Average Daily Balance (ADB)', formatPdfMoneyPlain(computedSummary.adb)]
  ];

  if (typeof doc.autoTable === 'function') {
    doc.autoTable({
      startY: y,
      theme: 'grid',
      margin: { left: marginLeft, right: 40 },
      head: [[{ content: 'Account Summary', colSpan: 2, styles: { halign: 'left' } }]],
      body: summaryRows,
      styles: { font: 'helvetica', fontSize: 9, cellPadding: 5 },
      headStyles: { fillColor: [245, 246, 250], textColor: [33, 37, 41], fontStyle: 'bold' },
      columnStyles: { 0: { cellWidth: 180 } }
    });
    y = doc.lastAutoTable.finalY + 16;
  } else {
    summaryRows.forEach(([label, value]) => {
      doc.text(`${label}: ${value}`, marginLeft, y);
      y += 12;
    });
    y += 8;
  }

  const monthly = Array.isArray(computedSummary.monthly) && computedSummary.monthly.length
    ? computedSummary.monthly.map((item) => ({
      monthLabel: item.month || item.monthLabel,
      debit: Number(item.debit || 0),
      credit: Number(item.credit || 0),
      avgDebit: Number(item.avg_debit || item.avgDebit || 0),
      avgCredit: Number(item.avg_credit || item.avgCredit || 0),
      adb: Number(item.adb || 0),
    }))
    : computeMonthlySummary(rowsForExport);
  doc.setFont('helvetica', 'bold');
  doc.setFontSize(11);
  doc.text('Monthly Summary', marginLeft, y);
  y += 8;

  const monthlyRows = monthly.length
    ? monthly.map((item) => ([
      pdfSafeText(item.monthLabel),
      formatPdfMoney(item.debit, true),
      formatPdfMoney(item.credit, true),
      formatPdfMoney(item.avgDebit, true),
      formatPdfMoney(item.avgCredit, true),
      formatPdfMoney(item.adb, true),
    ]))
    : [['No monthly data', '-', '-', '-', '-', '-']];

  if (typeof doc.autoTable === 'function') {
    doc.autoTable({
      startY: y,
      theme: 'grid',
      margin: { left: marginLeft, right: 40 },
      head: [['Month', 'Debit', 'Credit', 'Avg Debit', 'Avg Credit', 'ADB']],
      body: monthlyRows,
      styles: { font: 'helvetica', fontSize: 8.5, cellPadding: 4 },
      headStyles: { fillColor: [245, 246, 250], textColor: [33, 37, 41], fontStyle: 'bold' },
      columnStyles: {
        0: { cellWidth: 108 },
        1: { cellWidth: 88, halign: 'right' },
        2: { cellWidth: 88, halign: 'right' },
        3: { cellWidth: 95, halign: 'right' },
        4: { cellWidth: 95, halign: 'right' },
        5: { cellWidth: 95, halign: 'right' }
      }
    });
    y = doc.lastAutoTable.finalY + 16;
  } else {
    monthlyRows.forEach(([month, debit, credit, avgDebit, avgCredit, adb]) => {
      doc.text(`${month}: ${debit} / ${credit} / ${avgDebit} / ${avgCredit} / ${adb}`, marginLeft, y);
      y += 12;
    });
    y += 8;
  }

  doc.setFont('helvetica', 'bold');
  doc.setFontSize(12);
  doc.text('Transactions', marginLeft, y);
  y += 10;

  const tableRows = rowsForExport.map((row, idx) => ([
    String(row.global_row_id || row.row_id || idx + 1),
    pdfSafeText(getDisplayValue(row, 'date') || ''),
    pdfSafeText(getDisplayValue(row, 'description') || ''),
    formatPdfMoney(getDisplayValue(row, 'debit') || ''),
    formatPdfMoney(getDisplayValue(row, 'credit') || ''),
    formatPdfMoney(getDisplayValue(row, 'balance') || '')
  ]));

  if (typeof doc.autoTable === 'function') {
    doc.autoTable({
      startY: y,
      theme: 'grid',
      margin: { left: marginLeft, right: 40 },
      head: [['#', 'Date', 'Description', 'Debit', 'Credit', 'Balance']],
      body: tableRows,
      styles: { font: 'helvetica', fontSize: 8.5, cellPadding: 4, overflow: 'linebreak' },
      headStyles: { fillColor: [245, 246, 250], textColor: [33, 37, 41], fontStyle: 'bold' },
      columnStyles: {
        0: { cellWidth: 34, halign: 'center' },
        1: { cellWidth: 70 },
        2: { cellWidth: 170 },
        3: { cellWidth: 78, halign: 'right' },
        4: { cellWidth: 78, halign: 'right' },
        5: { cellWidth: 88, halign: 'right' }
      }
    });
  } else {
    const fallbackLines = tableRows.map((cols) => cols.join(' | '));
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(8);
    fallbackLines.forEach((line) => {
      if (y > 780) {
        doc.addPage();
        y = 42;
      }
      doc.text(line, marginLeft, y);
      y += 10;
    });
  }

  doc.save(buildPdfFileName());
  if (authRole === 'credit_evaluator' && evaluatorSelectedSubmission && evaluatorSelectedSubmission.id) {
    try {
      await markSummaryGenerated();
    } catch (_err) {
      // Keep export non-blocking even if status update fails.
    }
  }
}

async function exportToExcel() {
  let rowsForExport = parsedRows.slice();
  let summaryForExport = null;
  if (authRole === 'credit_evaluator' && evaluatorSelectedSubmission && evaluatorSelectedSubmission.id) {
    try {
      const payload = await getEvaluatorExportPayload();
      rowsForExport = payload && Array.isArray(payload.rows) ? payload.rows : [];
      summaryForExport = payload ? payload.summary : null;
    } catch (err) {
      alert((err && err.message) || 'Export blocked');
      return;
    }
  }

  if (!rowsForExport.length) {
    alert('No extracted rows to export yet.');
    return;
  }
  if (!window.XLSX || !window.XLSX.utils || typeof window.XLSX.writeFile !== 'function') {
    alert('Excel export library is not loaded.');
    return;
  }

  const workbook = window.XLSX.utils.book_new();

  const computedSummary = summaryForExport || computeSummary(rowsForExport);
  const summaryRows = [
    ['Account Name', (accountNameSummary && accountNameSummary.textContent ? accountNameSummary.textContent : '-').trim()],
    ['Account Number', (accountNumberSummary && accountNumberSummary.textContent ? accountNumberSummary.textContent : '-').trim()],
    ['Total Transactions', String(computedSummary.total_transactions || rowsForExport.length)],
    ['Debit Transactions', String(computedSummary.debit_transactions || countAmountTransactions(rowsForExport, 'debit'))],
    ['Credit Transactions', String(computedSummary.credit_transactions || countAmountTransactions(rowsForExport, 'credit'))],
    ['Average Daily Balance (ADB)', formatPesoValue(Number(computedSummary.adb || 0), true)],
  ];
  const summarySheet = window.XLSX.utils.aoa_to_sheet([
    ['Account Summary', ''],
    ...summaryRows,
  ]);
  window.XLSX.utils.book_append_sheet(workbook, summarySheet, 'Summary');

  const monthly = Array.isArray(computedSummary.monthly) && computedSummary.monthly.length
    ? computedSummary.monthly.map((item) => ({
      monthLabel: item.month || item.monthLabel,
      debit: Number(item.debit || 0),
      credit: Number(item.credit || 0),
      avgDebit: Number(item.avg_debit || item.avgDebit || 0),
      avgCredit: Number(item.avg_credit || item.avgCredit || 0),
      adb: Number(item.adb || 0),
    }))
    : computeMonthlySummary(rowsForExport);
  const monthlyRows = monthly.length
    ? monthly.map((item) => ([
      item.monthLabel,
      formatPesoValue(Math.abs(item.debit), true),
      formatPesoValue(Math.abs(item.credit), true),
      formatPesoValue(Math.abs(item.avgDebit), true),
      formatPesoValue(Math.abs(item.avgCredit), true),
      formatPesoValue(item.adb, true),
    ]))
    : [['No monthly data', '-', '-', '-', '-', '-']];
  const monthlySheet = window.XLSX.utils.aoa_to_sheet([
    ['Month', 'Debit', 'Credit', 'Avg Debit', 'Avg Credit', 'ADB'],
    ...monthlyRows,
  ]);
  window.XLSX.utils.book_append_sheet(workbook, monthlySheet, 'Monthly Summary');

  const txRows = rowsForExport.map((row, idx) => ([
    String(row.global_row_id || row.row_id || idx + 1),
    getDisplayValue(row, 'date') || '',
    getDisplayValue(row, 'description') || '',
    getDisplayValue(row, 'debit') || '',
    getDisplayValue(row, 'credit') || '',
    getDisplayValue(row, 'balance') || '',
  ]));
  const txSheet = window.XLSX.utils.aoa_to_sheet([
    ['#', 'Date', 'Description', 'Debit', 'Credit', 'Balance'],
    ...txRows,
  ]);
  window.XLSX.utils.book_append_sheet(workbook, txSheet, 'Transactions');

  const stamp = currentJobId ? String(currentJobId).slice(0, 8) : 'export';
  window.XLSX.writeFile(workbook, `statement_${stamp}.xlsx`);
  if (authRole === 'credit_evaluator' && evaluatorSelectedSubmission && evaluatorSelectedSubmission.id) {
    try {
      await markSummaryGenerated();
    } catch (_err) {
      // Keep export non-blocking even if status update fails.
    }
  }
}

function buildPdfFileName() {
  const raw = selectedFile && selectedFile.name ? selectedFile.name : 'statement';
  const base = raw.replace(/\.pdf$/i, '').replace(/[^a-zA-Z0-9-_]+/g, '_').replace(/^_+|_+$/g, '');
  return `${base || 'statement'}_export.pdf`;
}

function formatPdfMoney(value, absolute = false) {
  const n = typeof value === 'number' ? value : normalizeAmount(String(value || ''));
  if (!Number.isFinite(n)) return pdfSafeText(String(value || '-').trim() || '-');
  const formatted = new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Math.abs(n));
  const sign = absolute ? '' : (n < 0 ? '-' : '');
  return `${sign}${formatted}`;
}

function formatPdfMoneyPlain(value, absolute = false) {
  const n = typeof value === 'number' ? value : normalizeAmount(String(value || ''));
  if (!Number.isFinite(n)) return pdfSafeText(String(value || '-').trim() || '-');
  const formatted = new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Math.abs(n));
  const sign = absolute ? '' : (n < 0 ? '-' : '');
  return `${sign}${formatted}`;
}

function pdfSafeText(value) {
  return String(value || '')
    .replace(/₱/g, 'PHP ')
    .replace(/[^\x20-\x7E\n\r\t]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function openFilePicker() {
  if (typeof fileInput.showPicker === 'function') {
    fileInput.showPicker();
    return;
  }
  fileInput.click();
}

resetProgressUI();
resetResults();
updateFlattenButtons();

if ('scrollRestoration' in history) {
  history.scrollRestoration = 'manual';
}
window.scrollTo(0, 0);
