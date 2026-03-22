/**
 * Bank Statement Analyzer - Main Application JavaScript
 * Clean, modular vanilla JS implementation
 */

// ============================================
// State Management
// ============================================
const AppState = {
  currentPage: 'uploads',
  user: null,
  jobs: [],
  transactions: [],
  selectedJob: null,
  selectedTransaction: null,
  isLoading: false,
  sidebarOpen: true
};

// ============================================
// DOM Elements Cache
// ============================================
const DOM = {
  sidebar: document.getElementById('sidebar'),
  sidebarToggle: document.getElementById('sidebarToggle'),
  mainContent: document.getElementById('mainContent'),
  pageTitle: document.getElementById('pageTitle'),
  modalOverlay: document.getElementById('modalOverlay'),
  modal: document.getElementById('modal'),
  modalTitle: document.getElementById('modalTitle'),
  modalBody: document.getElementById('modalBody'),
  modalFooter: document.getElementById('modalFooter'),
  modalClose: document.getElementById('modalClose'),
  logoutBtn: document.getElementById('logoutBtn'),
  adminLink: document.getElementById('adminLink')
};

// ============================================
// Page Templates
// ============================================
const Pages = {
  uploads: () => `
    <div class="content-container">
      <div class="page-header">
        <h1 class="page-header-title">Upload Bank Statements</h1>
        <p class="page-header-description">Upload PDF bank statements to process and extract transaction data.</p>
      </div>

      <div class="card">
        <div class="card-body">
          <div class="dropzone" id="dropzone">
            <svg class="dropzone-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"></path>
            </svg>
            <div class="dropzone-title">Drop PDF files here</div>
            <div class="dropzone-description">or click to browse files from your computer</div>
            <button class="btn btn-primary">Choose Files</button>
            <input type="file" id="fileInput" accept=".pdf" multiple hidden>
          </div>
        </div>
      </div>

      <div class="card mt-6">
        <div class="card-header">
          <div>
            <h2 class="card-title">Recent Uploads</h2>
            <p class="card-description">Files uploaded in the last 30 days</p>
          </div>
          <div class="input-group" style="width: 280px;">
            <svg class="input-group-icon" width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path>
            </svg>
            <input type="text" class="form-input" placeholder="Search uploads..." id="uploadSearch">
          </div>
        </div>
        <div class="card-body p-0">
          <div class="table-container">
            <table class="table">
              <thead>
                <tr>
                  <th style="width: 40%">File Name</th>
                  <th>Size</th>
                  <th>Uploaded</th>
                  <th>Status</th>
                  <th class="table-cell-actions">Actions</th>
                </tr>
              </thead>
              <tbody id="uploadsTableBody">
                <!-- Populated by JS -->
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  `,

  jobs: () => `
    <div class="content-container">
      <div class="page-header">
        <div>
          <h1 class="page-header-title">Processing Jobs</h1>
          <p class="page-header-description">Monitor and manage OCR processing jobs.</p>
        </div>
      </div>

      <!-- Stats -->
      <div class="stats-grid mb-6">
        <div class="stat-card">
          <div class="stat-label">Total Jobs</div>
          <div class="stat-value">24</div>
          <div class="stat-change stat-change-positive">+3 this week</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Processing</div>
          <div class="stat-value">2</div>
          <div class="stat-change">Active now</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Completed</div>
          <div class="stat-value">18</div>
          <div class="stat-change stat-change-positive">75% success rate</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Failed</div>
          <div class="stat-value">4</div>
          <div class="stat-change stat-change-negative">Needs attention</div>
        </div>
      </div>

      <!-- Jobs Table -->
      <div class="card">
        <div class="card-header">
          <div class="tabs">
            <div class="tabs-list" style="border-bottom: none; padding: 0;">
              <button class="tab active" data-tab="all">All Jobs</button>
              <button class="tab" data-tab="processing">Processing</button>
              <button class="tab" data-tab="completed">Completed</button>
              <button class="tab" data-tab="failed">Failed</button>
            </div>
          </div>
          <div class="flex gap-3">
            <div class="input-group" style="width: 240px;">
              <svg class="input-group-icon" width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path>
              </svg>
              <input type="text" class="form-input" placeholder="Search jobs..." id="jobSearch">
            </div>
            <button class="btn btn-secondary">
              <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z"></path>
              </svg>
              Filter
            </button>
          </div>
        </div>
        <div class="card-body p-0">
          <div class="table-container">
            <table class="table">
              <thead>
                <tr>
                  <th style="width: 25%">Job ID</th>
                  <th>File</th>
                  <th>Created</th>
                  <th>Progress</th>
                  <th>Status</th>
                  <th class="table-cell-actions">Actions</th>
                </tr>
              </thead>
              <tbody id="jobsTableBody">
                <!-- Populated by JS -->
              </tbody>
            </table>
          </div>
        </div>
        <div class="card-footer">
          <span class="text-sm text-secondary">Showing 1-10 of 24 jobs</span>
          <div class="flex gap-2">
            <button class="btn btn-secondary btn-sm" disabled>Previous</button>
            <button class="btn btn-secondary btn-sm">Next</button>
          </div>
        </div>
      </div>
    </div>
  `,

  transactions: () => `
    <div class="content-container">
      <div class="page-header">
        <div>
          <h1 class="page-header-title">Transactions</h1>
          <p class="page-header-description">View and edit extracted transaction data.</p>
        </div>
      </div>

      <div class="split-view">
        <!-- Transactions Table -->
        <div class="card">
          <div class="card-header">
            <div>
              <h2 class="card-title">Extracted Transactions</h2>
              <p class="card-description">Job: #job-2024-001</p>
            </div>
            <div class="flex gap-3">
              <button class="btn btn-secondary btn-sm" id="reverseOrderBtn">
                <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16V4m0 0L3 8m4-4l4 4m6 0v12m0 0l4-4m-4 4l-4-4"></path>
                </svg>
                Reverse
              </button>
              <button class="btn btn-primary">
                <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path>
                </svg>
                Export
              </button>
            </div>
          </div>
          <div class="card-body p-0">
            <div class="table-container" style="max-height: 600px;">
              <table class="table" id="transactionsTable">
                <thead>
                  <tr>
                    <th style="width: 60px">#</th>
                    <th>Date</th>
                    <th style="width: 35%">Description</th>
                    <th class="table-cell-numeric">Debit</th>
                    <th class="table-cell-numeric">Credit</th>
                    <th class="table-cell-numeric">Balance</th>
                  </tr>
                </thead>
                <tbody id="transactionsTableBody">
                  <!-- Populated by JS -->
                </tbody>
              </table>
            </div>
          </div>
          <div class="card-footer">
            <div class="flex gap-4 text-sm">
              <span class="flex items-center gap-2">
                <span class="badge badge-error"></span> Errors
              </span>
              <span class="flex items-center gap-2">
                <span class="badge badge-warning"></span> Modified
              </span>
              <span class="flex items-center gap-2">
                <span class="badge badge-success"></span> Added
              </span>
            </div>
            <span class="text-sm text-secondary">156 rows total</span>
          </div>
        </div>

        <!-- Preview Panel -->
        <div class="card">
          <div class="card-header">
            <div>
              <h2 class="card-title">Document Preview</h2>
              <p class="card-description">Page 1 of 5</p>
            </div>
            <div class="flex gap-2">
              <button class="btn btn-ghost btn-icon" title="Zoom out">
                <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20 12H4"></path>
                </svg>
              </button>
              <span class="text-sm text-secondary">100%</span>
              <button class="btn btn-ghost btn-icon" title="Zoom in">
                <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"></path>
                </svg>
              </button>
            </div>
          </div>
          <div class="card-body">
            <div class="preview-panel" id="previewPanel">
              <div class="preview-empty">
                <svg width="48" height="48" fill="none" stroke="currentColor" viewBox="0 0 24 24" style="margin-bottom: 16px; opacity: 0.5;">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"></path>
                </svg>
                <p>Select a transaction to view</p>
              </div>
            </div>
          </div>
          <div class="card-footer">
            <div class="flex gap-2">
              <button class="btn btn-ghost btn-icon" title="First page">
                <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 19l-7-7 7-7m8 14l-7-7 7-7"></path>
                </svg>
              </button>
              <button class="btn btn-ghost btn-icon" title="Previous page">
                <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"></path>
                </svg>
              </button>
              <input type="text" class="form-input" value="1" style="width: 60px; text-align: center;">
              <span class="text-sm text-secondary">of 5</span>
              <button class="btn btn-ghost btn-icon" title="Next page">
                <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                </svg>
              </button>
              <button class="btn btn-ghost btn-icon" title="Last page">
                <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 5l7 7-7 7M5 5l7 7-7 7"></path>
                </svg>
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  `,

  processing: () => `
    <div class="content-container">
      <div class="page-header">
        <div>
          <h1 class="page-header-title">Processing Workspace</h1>
          <p class="page-header-description">Monitor OCR processing and review results.</p>
        </div>
        <div class="flex gap-3">
          <button class="btn btn-secondary" disabled>
            <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path>
            </svg>
            Export PDF
          </button>
          <button class="btn btn-secondary" disabled>
            <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path>
            </svg>
            Export Excel
          </button>
          <button class="btn btn-primary" id="startJobBtn">
            <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"></path>
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path>
            </svg>
            Start Job
          </button>
        </div>
      </div>

      <!-- Job Status Card -->
      <div class="card mb-6">
        <div class="card-body">
          <div class="grid-cols-4" style="display: grid; gap: 24px;">
            <div>
              <div class="text-xs font-semibold uppercase text-tertiary mb-2">Job ID</div>
              <div class="text-sm font-medium">#job-2024-001</div>
            </div>
            <div>
              <div class="text-xs font-semibold uppercase text-tertiary mb-2">Status</div>
              <span class="badge badge-primary badge-processing">Processing</span>
            </div>
            <div>
              <div class="text-xs font-semibold uppercase text-tertiary mb-2">Step</div>
              <div class="text-sm font-medium">OCR Analysis</div>
            </div>
            <div>
              <div class="text-xs font-semibold uppercase text-tertiary mb-2">Progress</div>
              <div class="flex items-center gap-3">
                <div class="progress flex-1">
                  <div class="progress-bar" style="width: 65%"></div>
                </div>
                <span class="text-sm font-medium">65%</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div class="split-view">
        <!-- Preview -->
        <div class="card">
          <div class="card-header">
            <h2 class="card-title">Document Preview</h2>
          </div>
          <div class="card-body">
            <div class="preview-panel" style="min-height: 500px;">
              <div class="preview-empty">
                <svg width="64" height="64" fill="none" stroke="currentColor" viewBox="0 0 24 24" style="margin-bottom: 16px; opacity: 0.3;">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path>
                </svg>
                <p>Upload a file to start processing</p>
              </div>
            </div>
          </div>
        </div>

        <!-- Results -->
        <div class="card">
          <div class="card-header">
            <h2 class="card-title">Processing Results</h2>
          </div>
          <div class="card-body">
            <div class="tabs">
              <div class="tabs-list" style="padding: 0; margin-bottom: 16px;">
                <button class="tab active" data-tab="parsed">Parsed Rows</button>
                <button class="tab" data-tab="disbalance">Disbalance</button>
                <button class="tab" data-tab="flagged">Flagged</button>
              </div>
              <div class="tab-content active" data-tab-content="parsed">
                <div class="table-container" style="max-height: 400px;">
                  <table class="table">
                    <thead>
                      <tr>
                        <th>Row</th>
                        <th>Date</th>
                        <th>Description</th>
                        <th class="table-cell-numeric">Debit</th>
                        <th class="table-cell-numeric">Credit</th>
                        <th class="table-cell-numeric">Balance</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr>
                        <td>1</td>
                        <td>2024-01-15</td>
                        <td>Opening Balance</td>
                        <td class="table-cell-numeric">-</td>
                        <td class="table-cell-numeric">-</td>
                        <td class="table-cell-numeric">₱50,000.00</td>
                      </tr>
                      <tr>
                        <td>2</td>
                        <td>2024-01-16</td>
                        <td>Grocery Purchase</td>
                        <td class="table-cell-numeric">₱2,500.00</td>
                        <td class="table-cell-numeric">-</td>
                        <td class="table-cell-numeric">₱47,500.00</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
              <div class="tab-content" data-tab-content="disbalance">
                <div class="table-empty">
                  <div class="table-empty-icon">
                    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                    </svg>
                  </div>
                  <div class="table-empty-title">No Disbalance Found</div>
                  <div class="table-empty-description">All transactions are balanced correctly.</div>
                </div>
              </div>
              <div class="tab-content" data-tab-content="flagged">
                <div class="table-empty">
                  <div class="table-empty-icon">
                    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                    </svg>
                  </div>
                  <div class="table-empty-title">No Flagged Transactions</div>
                  <div class="table-empty-description">No suspicious transactions detected.</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  `,

  crm: () => `
    <div class="content-container">
      <div class="page-header">
        <div>
          <h1 class="page-header-title">CRM Bank Statements</h1>
          <p class="page-header-description">Files from EspoCRM leads and accounts.</p>
        </div>
        <button class="btn btn-primary" id="refreshCrmBtn">
          <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path>
          </svg>
          Refresh Files
        </button>
      </div>

      <div class="card">
        <div class="card-header">
          <div class="tabs">
            <div class="tabs-list" style="border-bottom: none; padding: 0;">
              <button class="tab active" data-tab="not_started">Not Started</button>
              <button class="tab" data-tab="queued">Queued</button>
              <button class="tab" data-tab="completed">Completed</button>
            </div>
          </div>
          <div class="input-group" style="width: 280px;">
            <svg class="input-group-icon" width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path>
            </svg>
            <input type="text" class="form-input" placeholder="Search CRM files..." id="crmSearch">
          </div>
        </div>
        <div class="card-body p-0">
          <div class="table-container">
            <table class="table">
              <thead>
                <tr>
                  <th>Account</th>
                  <th>Type</th>
                  <th>Date</th>
                  <th>File Name</th>
                  <th>Status</th>
                  <th class="table-cell-actions">Actions</th>
                </tr>
              </thead>
              <tbody id="crmTableBody">
                <!-- Populated by JS -->
              </tbody>
            </table>
          </div>
        </div>
        <div class="card-footer">
          <span class="text-sm text-secondary">Showing 1-10 of 45 files</span>
          <div class="flex gap-2">
            <button class="btn btn-secondary btn-sm" disabled>Previous</button>
            <button class="btn btn-secondary btn-sm">Next</button>
          </div>
        </div>
      </div>
    </div>
  `
};

// ============================================
// Mock Data
// ============================================
const MockData = {
  uploads: [
    { id: 1, name: 'statement_jan_2024.pdf', size: '2.4 MB', date: '2024-01-15 10:30', status: 'completed' },
    { id: 2, name: 'bdo_statement_feb.pdf', size: '1.8 MB', date: '2024-02-01 14:22', status: 'processing' },
    { id: 3, name: 'metrobank_q4_2023.pdf', size: '3.2 MB', date: '2024-01-28 09:15', status: 'failed' },
    { id: 4, name: 'unionbank_dec.pdf', size: '1.5 MB', date: '2024-01-10 16:45', status: 'completed' }
  ],

  jobs: [
    { id: 'job-2024-004', file: 'statement_jan_2024.pdf', created: '2024-01-15 10:30', progress: 100, status: 'completed' },
    { id: 'job-2024-003', file: 'bdo_statement_feb.pdf', created: '2024-02-01 14:22', progress: 65, status: 'processing' },
    { id: 'job-2024-002', file: 'metrobank_q4_2023.pdf', created: '2024-01-28 09:15', progress: 0, status: 'failed' },
    { id: 'job-2024-001', file: 'unionbank_dec.pdf', created: '2024-01-10 16:45', progress: 100, status: 'completed' }
  ],

  transactions: [
    { id: 1, date: '2024-01-15', description: 'Opening Balance', debit: null, credit: null, balance: 50000.00, status: 'normal' },
    { id: 2, date: '2024-01-16', description: 'Grocery Purchase - SM Supermarket', debit: 2500.00, credit: null, balance: 47500.00, status: 'normal' },
    { id: 3, date: '2024-01-17', description: 'Salary Credit', debit: null, credit: 45000.00, balance: 92500.00, status: 'modified' },
    { id: 4, date: '2024-01-18', description: 'Electric Bill Payment', debit: 3500.00, credit: null, balance: 89000.00, status: 'normal' },
    { id: 5, date: '2024-01-19', description: 'Online Transfer - Suspicious', debit: 25000.00, credit: null, balance: 64000.00, status: 'error' },
    { id: 6, date: '2024-01-20', description: 'ATM Withdrawal', debit: 5000.00, credit: null, balance: 59000.00, status: 'normal' },
    { id: 7, date: '2024-01-21', description: 'Refund - Lazada', debit: null, credit: 1200.00, balance: 60200.00, status: 'added' },
    { id: 8, date: '2024-01-22', description: 'Gas Station', debit: 2500.00, credit: null, balance: 57700.00, status: 'normal' }
  ],

  crmFiles: [
    { id: 1, account: 'ABC Corporation', type: 'Lead', date: '2024-01-15', file: 'bank_stmt_jan.pdf', status: 'not_started' },
    { id: 2, account: 'XYZ Trading', type: 'Account', date: '2024-01-14', file: 'bdo_feb_2024.pdf', status: 'queued' },
    { id: 3, account: 'Smith Enterprises', type: 'Lead', date: '2024-01-13', file: 'metrobank_stmt.pdf', status: 'completed' },
    { id: 4, account: 'Global Tech', type: 'Account', date: '2024-01-12', file: 'unionbank_dec.pdf', status: 'not_started' }
  ]
};

// ============================================
// Helper Functions
// ============================================
const formatCurrency = (amount) => {
  if (amount === null || amount === undefined) return '-';
  return '₱' + amount.toLocaleString('en-PH', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
};

const getStatusBadge = (status) => {
  const statusMap = {
    completed: 'badge-success',
    processing: 'badge-primary badge-processing',
    failed: 'badge-error',
    queued: 'badge-info',
    not_started: 'badge-default',
    normal: 'badge-default',
    modified: 'badge-warning',
    error: 'badge-error',
    added: 'badge-success'
  };
  return statusMap[status] || 'badge-default';
};

// ============================================
// Render Functions
// ============================================
const Render = {
  uploads: () => {
    const tbody = document.getElementById('uploadsTableBody');
    if (!tbody) return;

    if (MockData.uploads.length === 0) {
      tbody.innerHTML = `
        <tr>
          <td colspan="5" class="table-empty">
            <div class="table-empty-icon"></div>
            <div class="table-empty-title">No uploads yet</div>
            <div class="table-empty-description">Upload a PDF file to get started.</div>
          </td>
        </tr>
      `;
      return;
    }

    tbody.innerHTML = MockData.uploads.map(upload => `
      <tr>
        <td>
          <div class="flex items-center gap-3">
            <svg width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24" style="color: var(--primary-500);">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path>
            </svg>
            <span class="font-medium">${upload.name}</span>
          </div>
        </td>
        <td class="text-secondary">${upload.size}</td>
        <td class="text-secondary">${upload.date}</td>
        <td><span class="badge ${getStatusBadge(upload.status)}">${upload.status}</span></td>
        <td class="table-cell-actions">
          <button class="btn btn-ghost btn-sm" onclick="Actions.viewUpload(${upload.id})">View</button>
          <button class="btn btn-ghost btn-sm" onclick="Actions.deleteUpload(${upload.id})">Delete</button>
        </td>
      </tr>
    `).join('');
  },

  jobs: () => {
    const tbody = document.getElementById('jobsTableBody');
    if (!tbody) return;

    tbody.innerHTML = MockData.jobs.map(job => `
      <tr class="cursor-pointer" onclick="Actions.selectJob('${job.id}')">
        <td>
          <span class="font-mono text-sm">${job.id}</span>
        </td>
        <td>${job.file}</td>
        <td class="text-secondary">${job.created}</td>
        <td>
          <div class="flex items-center gap-3">
            <div class="progress flex-1" style="max-width: 100px;">
              <div class="progress-bar ${job.status === 'failed' ? 'progress-bar-error' : ''}" style="width: ${job.progress}%"></div>
            </div>
            <span class="text-sm text-secondary">${job.progress}%</span>
          </div>
        </td>
        <td><span class="badge ${getStatusBadge(job.status)}">${job.status}</span></td>
        <td class="table-cell-actions">
          <button class="btn btn-ghost btn-sm" onclick="event.stopPropagation(); Actions.viewJob('${job.id}')">View</button>
          ${job.status === 'failed' ? `<button class="btn btn-ghost btn-sm" onclick="event.stopPropagation(); Actions.retryJob('${job.id}')">Retry</button>` : ''}
        </td>
      </tr>
    `).join('');
  },

  transactions: () => {
    const tbody = document.getElementById('transactionsTableBody');
    if (!tbody) return;

    tbody.innerHTML = MockData.transactions.map((tx, index) => `
      <tr class="clickable ${tx.status !== 'normal' ? `table-row-${tx.status === 'modified' ? 'warning' : tx.status}` : ''}" 
          data-id="${tx.id}" 
          onclick="Actions.selectTransaction(${tx.id})">
        <td class="font-mono">${index + 1}</td>
        <td>${tx.date}</td>
        <td class="table-cell-truncate" title="${tx.description}">${tx.description}</td>
        <td class="table-cell-numeric ${tx.debit ? 'text-error' : ''}">${formatCurrency(tx.debit)}</td>
        <td class="table-cell-numeric ${tx.credit ? 'text-success' : ''}">${formatCurrency(tx.credit)}</td>
        <td class="table-cell-numeric font-medium">${formatCurrency(tx.balance)}</td>
      </tr>
    `).join('');
  },

  crm: () => {
    const tbody = document.getElementById('crmTableBody');
    if (!tbody) return;

    tbody.innerHTML = MockData.crmFiles.map(file => `
      <tr>
        <td>
          <div class="font-medium">${file.account}</div>
        </td>
        <td><span class="badge badge-default">${file.type}</span></td>
        <td class="text-secondary">${file.date}</td>
        <td>${file.file}</td>
        <td><span class="badge ${getStatusBadge(file.status)}">${file.status.replace('_', ' ')}</span></td>
        <td class="table-cell-actions">
          ${file.status === 'not_started' ? `
            <button class="btn btn-primary btn-sm" onclick="Actions.processCrmFile(${file.id})">Process</button>
          ` : `
            <button class="btn btn-ghost btn-sm" onclick="Actions.viewCrmFile(${file.id})">View</button>
          `}
        </td>
      </tr>
    `).join('');
  }
};

// ============================================
// Actions
// ============================================
const Actions = {
  navigate: (page) => {
    AppState.currentPage = page;
    DOM.pageTitle.textContent = page.charAt(0).toUpperCase() + page.slice(1);

    // Update sidebar active state
    document.querySelectorAll('.sidebar-nav-link').forEach(link => {
      link.classList.remove('active');
      if (link.dataset.page === page) {
        link.classList.add('active');
      }
    });

    // Render page content
    DOM.mainContent.innerHTML = Pages[page] ? Pages[page]() : Pages.uploads();

    // Initialize page-specific features
    setTimeout(() => {
      if (Render[page]) Render[page]();
      initPageFeatures(page);
    }, 0);
  },

  viewUpload: (id) => {
    Modal.open('Upload Details', `
      <div class="grid-cols-2" style="display: grid; gap: 16px;">
        <div>
          <div class="text-sm text-secondary mb-1">File Name</div>
          <div class="font-medium">statement_jan_2024.pdf</div>
        </div>
        <div>
          <div class="text-sm text-secondary mb-1">Size</div>
          <div class="font-medium">2.4 MB</div>
        </div>
        <div>
          <div class="text-sm text-secondary mb-1">Uploaded</div>
          <div class="font-medium">2024-01-15 10:30</div>
        </div>
        <div>
          <div class="text-sm text-secondary mb-1">Status</div>
          <span class="badge badge-success">Completed</span>
        </div>
      </div>
    `, `
      <button class="btn btn-ghost" onclick="Modal.close()">Close</button>
      <button class="btn btn-primary" onclick="Actions.navigate('processing'); Modal.close();">View Results</button>
    `);
  },

  deleteUpload: (id) => {
    if (confirm('Are you sure you want to delete this upload?')) {
      MockData.uploads = MockData.uploads.filter(u => u.id !== id);
      Render.uploads();
    }
  },

  selectJob: (jobId) => {
    AppState.selectedJob = jobId;
    document.querySelectorAll('#jobsTableBody tr').forEach(row => {
      row.classList.remove('table-row-selected');
    });
    event.currentTarget.classList.add('table-row-selected');
  },

  viewJob: (jobId) => {
    Actions.navigate('processing');
  },

  retryJob: (jobId) => {
    const job = MockData.jobs.find(j => j.id === jobId);
    if (job) {
      job.status = 'processing';
      job.progress = 0;
      Render.jobs();

      // Simulate progress
      const interval = setInterval(() => {
        job.progress += 10;
        if (job.progress >= 100) {
          job.status = 'completed';
          clearInterval(interval);
        }
        Render.jobs();
      }, 500);
    }
  },

  selectTransaction: (id) => {
    AppState.selectedTransaction = id;
    document.querySelectorAll('#transactionsTableBody tr').forEach(row => {
      row.classList.remove('table-row-selected');
    });
    event.currentTarget.classList.add('table-row-selected');

    // Show preview (simulated)
    const previewPanel = document.getElementById('previewPanel');
    if (previewPanel) {
      previewPanel.innerHTML = `
        <div style="padding: 24px; text-align: center;">
          <svg width="64" height="64" fill="none" stroke="currentColor" viewBox="0 0 24 24" style="margin-bottom: 16px; opacity: 0.5;">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path>
          </svg>
          <p>Preview for transaction #${id}</p>
          <p class="text-sm text-secondary">Bounding box would be highlighted here</p>
        </div>
      `;
    }
  },

  processCrmFile: (id) => {
    const file = MockData.crmFiles.find(f => f.id === id);
    if (file) {
      file.status = 'queued';
      Render.crm();
    }
  },

  viewCrmFile: (id) => {
    Actions.navigate('processing');
  }
};

// ============================================
// Modal System
// ============================================
const Modal = {
  open: (title, body, footer = '') => {
    DOM.modalTitle.textContent = title;
    DOM.modalBody.innerHTML = body;
    DOM.modalFooter.innerHTML = footer;
    DOM.modalOverlay.classList.add('active');
    document.body.style.overflow = 'hidden';
  },

  close: () => {
    DOM.modalOverlay.classList.remove('active');
    document.body.style.overflow = '';
  }
};

// ============================================
// Event Handlers
// ============================================
function initGlobalEvents() {
  // Sidebar toggle
  DOM.sidebarToggle.addEventListener('click', () => {
    DOM.sidebar.classList.toggle('open');
    AppState.sidebarOpen = !AppState.sidebarOpen;
  });

  // Navigation
  document.querySelectorAll('.sidebar-nav-link').forEach(link => {
    link.addEventListener('click', (e) => {
      e.preventDefault();
      const page = link.dataset.page;
      if (page) Actions.navigate(page);
    });
  });

  // Modal close
  DOM.modalClose.addEventListener('click', Modal.close);
  DOM.modalOverlay.addEventListener('click', (e) => {
    if (e.target === DOM.modalOverlay) Modal.close();
  });

  // Logout
  DOM.logoutBtn.addEventListener('click', () => {
    if (confirm('Are you sure you want to logout?')) {
      window.location.href = '/login';
    }
  });

  // Keyboard shortcuts
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') Modal.close();
  });
}

function initPageFeatures(page) {
  // Upload page features
  if (page === 'uploads') {
    const dropzone = document.getElementById('dropzone');
    const fileInput = document.getElementById('fileInput');

    if (dropzone && fileInput) {
      dropzone.addEventListener('click', () => fileInput.click());
      dropzone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropzone.classList.add('dragover');
      });
      dropzone.addEventListener('dragleave', () => {
        dropzone.classList.remove('dragover');
      });
      dropzone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropzone.classList.remove('dragover');
        // Handle file drop
        console.log('Files dropped:', e.dataTransfer.files);
      });
      fileInput.addEventListener('change', (e) => {
        console.log('Files selected:', e.target.files);
      });
    }
  }

  // Jobs page features
  if (page === 'jobs') {
    // Tab switching
    document.querySelectorAll('.tab').forEach(tab => {
      tab.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
      });
    });
  }

  // Transactions page features
  if (page === 'transactions') {
    const reverseBtn = document.getElementById('reverseOrderBtn');
    if (reverseBtn) {
      reverseBtn.addEventListener('click', () => {
        MockData.transactions.reverse();
        Render.transactions();
      });
    }
  }

  // Processing page features
  if (page === 'processing') {
    const startBtn = document.getElementById('startJobBtn');
    if (startBtn) {
      startBtn.addEventListener('click', () => {
        startBtn.disabled = true;
        startBtn.innerHTML = `
          <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24" style="animation: spin 1s linear infinite;">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path>
          </svg>
          Starting...
        `;
        setTimeout(() => {
          startBtn.disabled = false;
          startBtn.innerHTML = `
            <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"></path>
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path>
            </svg>
            Start Job
          `;
        }, 2000);
      });
    }

    // Tab switching
    document.querySelectorAll('.tab').forEach(tab => {
      tab.addEventListener('click', () => {
        const tabName = tab.dataset.tab;
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        document.querySelectorAll('.tab-content').forEach(content => {
          content.classList.remove('active');
        });
        const content = document.querySelector(`[data-tab-content="${tabName}"]`);
        if (content) content.classList.add('active');
      });
    });
  }

  // CRM page features
  if (page === 'crm') {
    const refreshBtn = document.getElementById('refreshCrmBtn');
    if (refreshBtn) {
      refreshBtn.addEventListener('click', () => {
        refreshBtn.disabled = true;
        refreshBtn.innerHTML = `
          <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24" style="animation: spin 1s linear infinite;">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path>
          </svg>
          Refreshing...
        `;
        setTimeout(() => {
          refreshBtn.disabled = false;
          refreshBtn.innerHTML = `
            <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path>
            </svg>
            Refresh Files
          `;
        }, 1500);
      });
    }

    // Tab switching
    document.querySelectorAll('.tab').forEach(tab => {
      tab.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
      });
    });
  }
}

// ============================================
// Initialization
// ============================================
function init() {
  initGlobalEvents();
  Actions.navigate('uploads');
}

// Start the app when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}

// Add spin animation for loading states
const style = document.createElement('style');
style.textContent = `
  @keyframes spin {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
  }
`;
document.head.appendChild(style);
