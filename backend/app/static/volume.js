(() => {
  const form = document.getElementById("volumeUploadForm");
  const setNameInput = document.getElementById("setName");
  const fileInput = document.getElementById("bulkFiles");
  const fileCountLabel = document.getElementById("fileCountLabel");
  const fileList = document.getElementById("fileList");
  const submitBtn = document.getElementById("volumeSubmitBtn");
  const successEl = document.getElementById("volumeSuccess");
  const errorEl = document.getElementById("volumeError");
  const refreshSetsBtn = document.getElementById("refreshSetsBtn");
  const setsEmptyState = document.getElementById("setsEmptyState");
  const setsTableWrap = document.getElementById("setsTableWrap");
  const setsTableBody = document.getElementById("setsTableBody");
  const initialSetsScript = document.getElementById("volumeInitialSets");

  if (!form || !setNameInput || !fileInput || !fileCountLabel || !fileList || !submitBtn || !successEl || !errorEl
    || !refreshSetsBtn || !setsEmptyState || !setsTableWrap || !setsTableBody || !initialSetsScript) {
    return;
  }

  function readInitialSets() {
    try {
      const payload = JSON.parse(initialSetsScript.textContent || "[]");
      return Array.isArray(payload) ? payload : [];
    } catch {
      return [];
    }
  }

  function formatTimestamp(value) {
    const date = new Date(Number(value || 0) * 1000);
    if (Number.isNaN(date.getTime())) return "-";
    return date.toLocaleString();
  }

  function formatBytes(value) {
    const bytes = Number(value || 0);
    if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
    const units = ["B", "KB", "MB", "GB"];
    let size = bytes;
    let unitIndex = 0;
    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex += 1;
    }
    return `${size.toFixed(size >= 10 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
  }

  function renderSets(items) {
    const rows = Array.isArray(items) ? items : [];
    setsTableBody.innerHTML = "";

    const hasRows = rows.length > 0;
    setsEmptyState.classList.toggle("hidden", hasRows);
    setsTableWrap.classList.toggle("hidden", !hasRows);

    if (!hasRows) {
      return;
    }

    rows.forEach((row) => {
      const tr = document.createElement("tr");

      const nameCell = document.createElement("td");
      nameCell.textContent = row.set_name || "-";
      tr.appendChild(nameCell);

      const fileCountCell = document.createElement("td");
      fileCountCell.textContent = String(row.file_count ?? 0);
      tr.appendChild(fileCountCell);

      const sizeCell = document.createElement("td");
      sizeCell.textContent = formatBytes(row.total_size);
      tr.appendChild(sizeCell);

      const updatedCell = document.createElement("td");
      updatedCell.textContent = formatTimestamp(row.updated_at);
      tr.appendChild(updatedCell);

      const actionCell = document.createElement("td");
      const downloadLink = document.createElement("a");
      downloadLink.href = `/volume/sets/${encodeURIComponent(row.set_name || "")}/download`;
      downloadLink.textContent = "Download Zip";
      downloadLink.className = "table-download-link";
      actionCell.appendChild(downloadLink);
      tr.appendChild(actionCell);

      setsTableBody.appendChild(tr);
    });
  }

  async function loadSets() {
    refreshSetsBtn.disabled = true;
    refreshSetsBtn.textContent = "Refreshing...";
    try {
      const response = await fetch("/volume/sets");
      if (response.status === 401) {
        window.location.href = "/login";
        return;
      }
      const body = await response.json().catch(() => null);
      if (!response.ok) {
        throw new Error(body && body.detail ? body.detail : "Unable to load sets");
      }
      if (!body || !Array.isArray(body.items)) {
        throw new Error("Unexpected response while loading sets");
      }
      renderSets(body.items);
    } catch (error) {
      errorEl.textContent = error instanceof Error ? error.message : "Unable to load sets";
      errorEl.classList.remove("hidden");
    } finally {
      refreshSetsBtn.disabled = false;
      refreshSetsBtn.textContent = "Refresh List";
    }
  }

  function setFilesSummary(files) {
    const selectedFiles = Array.from(files || []);
    fileList.innerHTML = "";
    if (selectedFiles.length === 0) {
      fileCountLabel.textContent = "No files selected";
      return;
    }

    fileCountLabel.textContent = `${selectedFiles.length} file${selectedFiles.length === 1 ? "" : "s"} selected`;
    selectedFiles.forEach((file) => {
      const item = document.createElement("li");
      item.textContent = file.name;
      fileList.appendChild(item);
    });
  }

  function hideMessages() {
    successEl.classList.add("hidden");
    errorEl.classList.add("hidden");
    successEl.textContent = "";
    errorEl.textContent = "";
  }

  fileInput.addEventListener("change", () => setFilesSummary(fileInput.files));

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    hideMessages();

    const setName = String(setNameInput.value || "").trim();
    const files = Array.from(fileInput.files || []);
    if (!setName) {
      errorEl.textContent = "Set name is required.";
      errorEl.classList.remove("hidden");
      setNameInput.focus();
      return;
    }
    if (files.length === 0) {
      errorEl.textContent = "Please choose one or more files.";
      errorEl.classList.remove("hidden");
      fileInput.focus();
      return;
    }

    submitBtn.disabled = true;
    submitBtn.textContent = "Uploading...";

    const payload = new FormData();
    payload.append("set_name", setName);
    files.forEach((file) => payload.append("files", file));

    try {
      const response = await fetch("/volume/upload", {
        method: "POST",
        body: payload,
      });
      if (response.status === 401) {
        window.location.href = "/login";
        return;
      }

      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(body.detail || "Upload failed");
      }

      successEl.textContent = `Saved ${body.saved_count} file(s) to ${body.saved_dir}.`;
      successEl.classList.remove("hidden");
      form.reset();
      setFilesSummary([]);
      await loadSets();
    } catch (error) {
      errorEl.textContent = error instanceof Error ? error.message : "Upload failed";
      errorEl.classList.remove("hidden");
    } finally {
      submitBtn.disabled = false;
      submitBtn.textContent = "Upload Files";
    }
  });

  refreshSetsBtn.addEventListener("click", () => {
    hideMessages();
    loadSets();
  });

  renderSets(readInitialSets());
  loadSets();
})();
