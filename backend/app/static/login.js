(() => {
  const form = document.getElementById('loginForm');
  const error = document.getElementById('loginError');
  if (!form) return;

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    error?.classList.add('hidden');
    const formData = new FormData(form);
    try {
      const res = await fetch('/auth/login', { method: 'POST', body: formData });
      if (!res.ok) throw new Error('login_failed');
      const payload = await res.json();
      const role = String(payload.role || '').toLowerCase().trim();
      const nextPath = role === 'admin' ? '/admin' : role === 'evaluator' ? '/evaluator' : '/uploads';
      window.location.replace(nextPath);
    } catch (_err) {
      error?.classList.remove('hidden');
    }
  });
})();
