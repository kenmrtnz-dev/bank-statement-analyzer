/**
 * Admin Panel JavaScript
 * Clean, modular implementation for admin functionality
 */

// ============================================
// State Management
// ============================================
const AdminState = {
  currentTab: 'accounts',
  users: [],
  jobs: [],
  transactions: [],
  flagCodes: [],
  settings: {},
  isLoading: false
};

// ============================================
// Mock Data
// ============================================
const AdminMockData = {
  users: [
    { id: 1, username: 'evaluator1', role: 'evaluator', created: '2024-01-15', lastLogin: '2024-03-20' },
    { id: 2, username: 'evaluator2', role: 'evaluator', created: '2024-02-01', lastLogin: '2024-03-19' },
    { id: 3, username: 'admin', role: 'admin', created: '2024-01-01', lastLogin: '2024-03-21' }
  ],

  jobs: [
    { id: 'job-001', user: 'evaluator1', file: 'statement.pdf', status: 'completed', created: '2024-03-20 10:30' },
    { id: 'job-002', user: 'evaluator2', file: 'bdo_feb.pdf', status: 'processing', created: '2024-03-20 11:15' },
    { id: 'job-003', user: 'evaluator1', file: 'metrobank.pdf', status: 'failed', created: '2024-03-19 14:22' }
  ],

  transactions: [
    { id: 1, jobId: 'job-001', date: '2024-01-15', description: 'Opening Balance', amount: 50000, status: 'valid' },
    { id: 2, jobId: 'job-001', date: '2024-01-16', description: 'Grocery Purchase', amount: -2500, status: 'valid' },
    { id: 3, jobId: 'job-002', date: '2024-01-17', description: 'Suspicious Transfer', amount: -25000, status: 'flagged' }
  ],

  flagCodes: [
    { id: 1, code: 'SUSPICIOUS', description: 'Suspicious transaction pattern', severity: 'high', active: true },
    { id: 2, code: 'LARGE_AMOUNT', description: 'Unusually large amount', severity: 'medium', active: true },
    { id: 3, code: 'ODD_HOURS', description: 'Transaction outside business hours', severity: 'low', active: false }
  ],

  settings: {
    uploadTesting: false,
    autoProcess: true,
    retentionDays: 30,
    maxFileSize: 50
  }
};

// ============================================
// DOM Elements
// ============================================
const AdminDOM = {
  // Navigation
  navButtons: document.querySelectorAll('.admin-nav-btn'),
  tabPanels: document.querySelectorAll('.admin-tab-panel'),
  activeTabLabel: document.getElementById('activeTabLabel'),
  
  // User info
  sessionUserLabel: document.getElementById('sessionUserLabel'),
  sessionRoleLabel: document.getElementById('sessionRoleLabel'),
  sidebarUploadTestingStatus: document.getElementById('sidebarUploadTestingStatus'),
  
  // Logout
  adminLogoutBtn: document.getElementById('adminLogoutBtn')
};

// ============================================
// Render Functions
// ============================================
const AdminRender = {
  accounts: () => {
    const tbody = document.getElementById('usersTableBody');
    if (!tbody) return;

    tbody.innerHTML = AdminMockData.users.map(user => `
      <tr>
        <td>
          <div class="flex items-center gap-3">
            <div class="user-avatar-sm">${user.username.charAt(0).toUpperCase()}</div>
            <span class="font-medium">${user.username}</span>
          </div>
        </td>
        <td><span class="badge ${user.role === 'admin' ? 'badge-primary' : 'badge-default'}">${user.role}</span></td>
        <td class="text-secondary">${user.created}</td>
        <td class="text-secondary">${user.lastLogin}</td>
        <td class="table-cell-actions">
          <button class="btn btn-ghost btn-sm" onclick="AdminActions.editUser(${user.id})">Edit</button>
          ${user.role !== 'admin' ? `<button class="btn btn-ghost btn-sm text-error" onclick="AdminActions.deleteUser(${user.id})">Delete</button>` : ''}
        </td>
      </tr>
    `).join('');
  },

  jobs: () => {
    const tbody = document.getElementById('adminJobsTableBody');
    if (!tbody) return;

    tbody.innerHTML = AdminMockData.jobs.map(job => `
      <tr>
        <td><span class="font-mono text-sm">${job.id}</span></td>
        <td>${job.user}</td>
        <td>${job.file}</td>
        <td class="text-secondary">${job.created}</td>
        <td><span class="badge ${AdminHelpers.getStatusBadge(job.status)}">${job.status}</span></td>
        <td class="table-cell-actions">
          <button class="btn btn-ghost btn-sm" onclick="AdminActions.viewJob('${job.id}')">View</button>
        </td>
      </tr>
    `).join('');
  },

  transactions: () => {
    const tbody = document.getElementById('adminTransactionsTableBody');
    if (!tbody) return;

    tbody.innerHTML = AdminMockData.transactions.map(tx => `
      <tr>
        <td><span class="font-mono text-sm">${tx.id}</span></td>
        <td><span class="font-mono text-sm">${tx.jobId}</span></td>
        <td>${tx.date}</td>
        <td>${tx.description}</td>
        <td class="table-cell-numeric ${tx.amount < 0 ? 'text-error' : 'text-success'}">${AdminHelpers.formatCurrency(tx.amount)}</td>
        <td><span class="badge ${AdminHelpers.getStatusBadge(tx.status)}">${tx.status}</span></td>
      </tr>
    `).join('');
  },

  flagCodes: () => {
    const tbody = document.getElementById('flagCodesTableBody');
    if (!tbody) return;

    tbody.innerHTML = AdminMockData.flagCodes.map(code => `
      <tr>
        <td><span class="font-mono text-sm font-medium">${code.code}</span></td>
        <td>${code.description}</td>
        <td><span class="badge ${AdminHelpers.getSeverityBadge(code.severity)}">${code.severity}</span></td>
        <td>
          <label class="toggle">
            <input type="checkbox" ${code.active ? 'checked' : ''} onchange="AdminActions.toggleFlagCode(${code.id})">
            <span class="toggle-slider"></span>
          </label>
        </td>
        <td class="table-cell-actions">
          <button class="btn btn-ghost btn-sm" onclick="AdminActions.editFlagCode(${code.id})">Edit</button>
        </td>
      </tr>
    `).join('');
  },

  settings: () => {
    const uploadTestingToggle = document.getElementById('uploadTestingToggle');
    const autoProcessToggle = document.getElementById('autoProcessToggle');
    const retentionInput = document.getElementById('retentionDays');
    const maxFileSizeInput = document.getElementById('maxFileSize');

    if (uploadTestingToggle) uploadTestingToggle.checked = AdminMockData.settings.uploadTesting;
    if (autoProcessToggle) autoProcessToggle.checked = AdminMockData.settings.autoProcess;
    if (retentionInput) retentionInput.value = AdminMockData.settings.retentionDays;
    if (maxFileSizeInput) maxFileSizeInput.value = AdminMockData.settings.maxFileSize;
  }
};

// ============================================
// Helper Functions
// ============================================
const AdminHelpers = {
  formatCurrency: (amount) => {
    const absAmount = Math.abs(amount);
    const formatted = '₱' + absAmount.toLocaleString('en-PH', { minimumFractionDigits: 2 });
    return amount < 0 ? `-${formatted}` : formatted;
  },

  getStatusBadge: (status) => {
    const map = {
      completed: 'badge-success',
      processing: 'badge-primary badge-processing',
      failed: 'badge-error',
      valid: 'badge-success',
      flagged: 'badge-warning'
    };
    return map[status] || 'badge-default';
  },

  getSeverityBadge: (severity) => {
    const map = {
      high: 'badge-error',
      medium: 'badge-warning',
      low: 'badge-info'
    };
    return map[severity] || 'badge-default';
  }
};

// ============================================
// Actions
// ============================================
const AdminActions = {
  switchTab: (tabName) => {
    AdminState.currentTab = tabName;

    // Update navigation
    document.querySelectorAll('.admin-nav-btn').forEach(btn => {
      btn.classList.remove('active');
      if (btn.dataset.tab === tabName) {
        btn.classList.add('active');
      }
    });

    // Update panels
    document.querySelectorAll('.admin-tab-panel').forEach(panel => {
      panel.classList.remove('active');
      if (panel.dataset.panel === tabName) {
        panel.classList.add('active');
      }
    });

    // Update breadcrumb
    const tabLabels = {
      accounts: 'Accounts',
      jobs: 'Jobs',
      transactions: 'Transactions',
      'flag-codes': 'Flag Codes',
      settings: 'Settings'
    };
    const label = document.getElementById('activeTabLabel');
    if (label) label.textContent = tabLabels[tabName] || tabName;

    // Render tab content
    if (AdminRender[tabName]) {
      AdminRender[tabName]();
    }
  },

  createUser: (e) => {
    e.preventDefault();
    const form = e.target;
    const formData = new FormData(form);
    
    const newUser = {
      id: AdminMockData.users.length + 1,
      username: formData.get('username'),
      role: formData.get('role'),
      created: new Date().toISOString().split('T')[0],
      lastLogin: 'Never'
    };

    AdminMockData.users.push(newUser);
    AdminRender.accounts();
    form.reset();
    
    // Show success message
    alert('User created successfully');
  },

  editUser: (id) => {
    const user = AdminMockData.users.find(u => u.id === id);
    if (!user) return;

    const newRole = confirm(`Change role for ${user.username}?\n\nClick OK for Admin, Cancel for Evaluator`);
    user.role = newRole ? 'admin' : 'evaluator';
    AdminRender.accounts();
  },

  deleteUser: (id) => {
    if (confirm('Are you sure you want to delete this user?')) {
      AdminMockData.users = AdminMockData.users.filter(u => u.id !== id);
      AdminRender.accounts();
    }
  },

  viewJob: (jobId) => {
    window.open(`/?job=${jobId}`, '_blank');
  },

  toggleFlagCode: (id) => {
    const code = AdminMockData.flagCodes.find(c => c.id === id);
    if (code) {
      code.active = !code.active;
      AdminRender.flagCodes();
    }
  },

  editFlagCode: (id) => {
    const code = AdminMockData.flagCodes.find(c => c.id === id);
    if (!code) return;

    const newDescription = prompt('Edit description:', code.description);
    if (newDescription !== null) {
      code.description = newDescription;
      AdminRender.flagCodes();
    }
  },

  saveSettings: (e) => {
    e.preventDefault();
    const form = e.target;
    const formData = new FormData(form);

    AdminMockData.settings.uploadTesting = formData.get('uploadTesting') === 'on';
    AdminMockData.settings.autoProcess = formData.get('autoProcess') === 'on';
    AdminMockData.settings.retentionDays = parseInt(formData.get('retentionDays'));
    AdminMockData.settings.maxFileSize = parseInt(formData.get('maxFileSize'));

    // Update sidebar status
    const statusEl = document.getElementById('sidebarUploadTestingStatus');
    if (statusEl) {
      statusEl.textContent = AdminMockData.settings.uploadTesting ? 'Enabled' : 'Disabled';
    }

    alert('Settings saved successfully');
  },

  logout: () => {
    if (confirm('Are you sure you want to logout?')) {
      window.location.href = '/login';
    }
  }
};

// ============================================
// Event Handlers
// ============================================
function initAdminEvents() {
  // Navigation
  document.querySelectorAll('.admin-nav-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      AdminActions.switchTab(btn.dataset.tab);
    });
  });

  // Create user form
  const createUserForm = document.getElementById('createUserForm');
  if (createUserForm) {
    createUserForm.addEventListener('submit', AdminActions.createUser);
  }

  // Settings form
  const settingsForm = document.getElementById('settingsForm');
  if (settingsForm) {
    settingsForm.addEventListener('submit', AdminActions.saveSettings);
  }

  // Logout
  const logoutBtn = document.getElementById('adminLogoutBtn');
  if (logoutBtn) {
    logoutBtn.addEventListener('click', AdminActions.logout);
  }
}

// ============================================
// Initialization
// ============================================
function initAdmin() {
  // Set user info
  const sessionUserLabel = document.getElementById('sessionUserLabel');
  const sessionRoleLabel = document.getElementById('sessionRoleLabel');
  
  if (sessionUserLabel) sessionUserLabel.textContent = 'admin';
  if (sessionRoleLabel) sessionRoleLabel.textContent = 'Admin';

  initAdminEvents();
  AdminActions.switchTab('accounts');
}

// Start when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initAdmin);
} else {
  initAdmin();
}
