(() => {
  const UPLOADS_CACHE_KEY = 'bsa_uploaded_jobs_v1';
  const msg = document.getElementById('adminMessage');
  const createForm = document.getElementById('createEvaluatorForm');
  const createFeedback = document.getElementById('createEvaluatorFeedback');
  const clearForm = document.getElementById('clearStoreForm');
  const logoutBtn = document.getElementById('adminLogoutBtn');
  const confirmModal = document.getElementById('confirmModal');
  const confirmCancelBtn = document.getElementById('confirmCancelBtn');
  const confirmClearBtn = document.getElementById('confirmClearBtn');

  function show(message, isError = false) {
    if (!msg) return;
    msg.textContent = message;
    msg.classList.remove('hidden');
    msg.style.color = isError ? '#a22e45' : '#1e6d43';
    msg.style.background = isError ? '#fdeef2' : '#e7f8ed';
    msg.style.borderColor = isError ? '#f5ccd8' : '#bde7cb';
  }

  function showCreateFeedback(message, isError = false) {
    if (!createFeedback) return;
    createFeedback.textContent = message;
    createFeedback.classList.remove('hidden');
    createFeedback.style.color = isError ? '#a22e45' : '#1f6d44';
    createFeedback.style.background = isError ? '#fdeef2' : '#edf9f2';
    createFeedback.style.borderColor = isError ? '#f5ccd8' : '#cae8d5';
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
    if (String(payload.role || '').toLowerCase() !== 'admin') {
      show('Admin access required.', true);
      if (createForm) createForm.style.display = 'none';
      if (clearForm) clearForm.style.display = 'none';
      return false;
    }
    return true;
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
      show(`Cleared ${payload.cleared_jobs} jobs and ${payload.cleared_exports} exports.`);
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

  requireAdmin().catch(() => show('Failed to verify admin session.', true));
})();
