const payrollInput = document.getElementById("payrollFile");

const uploadBtn = document.getElementById("uploadBtn");
const generateBtn = document.getElementById("generateBtn");
const downloadBtn = document.getElementById("downloadBtn");
const downloadT13Btn = document.getElementById("downloadT13Btn");
const downloadLogBtn = document.getElementById("downloadLogBtn");
const previewFilter = document.getElementById("previewFilter");
const previewMeta = document.getElementById("previewMeta");
const appVersionNode = document.getElementById("appVersion");
const roleGroupsEditor = document.getElementById("roleGroupsEditor");
const roleGroupsBody = document.querySelector("#roleGroupsTable tbody");
const loadMoreBtn = document.getElementById("loadMoreBtn");

const statusNode = document.getElementById("status");
const previewBody = document.querySelector("#previewTable tbody");
let previewRows = [];
let previewTotal = 0;
let previewNextOffset = 0;
let previewPageSize = 150;
let roleGroupOverrides = {};
let availableRoleGroups = ["Кухня", "Зал", "Касса", "Бар", "Обслуживание"];

function setStatus(text) {
  statusNode.textContent = text;
}

function formatApiDetail(detail) {
  if (typeof detail === "string") {
    return detail;
  }
  if (detail && typeof detail === "object") {
    if (detail.message) {
      return detail.message;
    }
    return JSON.stringify(detail, null, 2);
  }
  return "Неизвестная ошибка";
}

function filterPreviewRows(rows, filterMode) {
  if (filterMode === "deficit") {
    return rows.filter((row) => row.deficit);
  }
  if (filterMode === "cross") {
    return rows.filter((row) => row.cross_restaurant);
  }
  return rows;
}

function renderPreview(rows, totalRowsCount = rows.length) {
  previewBody.innerHTML = "";
  previewMeta.textContent = `Показано: ${rows.length} из ${totalRowsCount}`;

  if (!rows || rows.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 8;
    td.textContent = "Нет данных для отображения.";
    tr.appendChild(td);
    previewBody.appendChild(tr);
    return;
  }

  for (const row of rows) {
    const tr = document.createElement("tr");
    if (row.deficit) {
      tr.classList.add("row-deficit");
    } else if (row.cross_restaurant) {
      tr.classList.add("row-cross");
    }

    let statusText = row.status || "ОК";
    if (row.deficit) {
      statusText = "ДЕФИЦИТ";
    } else if (row.cross_restaurant) {
      statusText = "Межресторанная замена";
    }

    const values = [
      row.day,
      row.restaurant,
      row.role,
      row.shift_label,
      row.hours,
      row.employee,
      row.employee_home_restaurant || "-",
      statusText,
    ];

    for (const value of values) {
      const td = document.createElement("td");
      td.textContent = value ?? "";
      tr.appendChild(td);
    }

    previewBody.appendChild(tr);
  }
}

function renderFilteredPreview() {
  const rows = filterPreviewRows(previewRows, previewFilter.value);
  renderPreview(rows, previewRows.length);
}

function updateLoadMoreButton() {
  const hasMore = previewNextOffset < previewTotal;
  loadMoreBtn.disabled = !hasMore;
  loadMoreBtn.textContent = hasMore
    ? `Загрузить еще (${previewRows.length}/${previewTotal})`
    : `Все загружено (${previewRows.length}/${previewTotal})`;
}

function renderRoleGroupsEditor(defaults) {
  roleGroupsBody.innerHTML = "";
  roleGroupOverrides = { ...defaults };

  const roles = Object.keys(defaults).sort((a, b) => a.localeCompare(b, "ru"));
  if (!roles.length) {
    roleGroupsEditor.classList.add("hidden");
    return;
  }

  for (const role of roles) {
    const tr = document.createElement("tr");

    const roleTd = document.createElement("td");
    roleTd.textContent = role;
    tr.appendChild(roleTd);

    const groupTd = document.createElement("td");
    const select = document.createElement("select");
    select.dataset.role = role;

    for (const groupName of availableRoleGroups) {
      const option = document.createElement("option");
      option.value = groupName;
      option.textContent = groupName;
      if (defaults[role] === groupName) {
        option.selected = true;
      }
      select.appendChild(option);
    }

    select.addEventListener("change", (event) => {
      const selected = event.target.value;
      roleGroupOverrides[role] = selected;
    });

    groupTd.appendChild(select);
    tr.appendChild(groupTd);
    roleGroupsBody.appendChild(tr);
  }

  roleGroupsEditor.classList.remove("hidden");
}

uploadBtn.addEventListener("click", async () => {
  const payrollFile = payrollInput.files[0];

  if (!payrollFile) {
    setStatus("Выберите файл расчетных листков перед загрузкой.");
    return;
  }

  setStatus("Загрузка и разбор файлов...");
  generateBtn.disabled = true;
  downloadBtn.disabled = true;
  downloadT13Btn.disabled = true;

  const formData = new FormData();
  formData.append("payroll_file", payrollFile);

  try {
    const response = await fetch("/upload", {
      method: "POST",
      body: formData,
    });

    const data = await response.json();
    if (!response.ok) {
      throw new Error(formatApiDetail(data.detail) || "Ошибка загрузки файлов");
    }

    const warnings = (data.warnings || []).length
      ? `\nПредупреждения:\n- ${data.warnings.join("\n- ")}`
      : "";

    if (Array.isArray(data.summary?.available_role_groups) && data.summary.available_role_groups.length) {
      availableRoleGroups = data.summary.available_role_groups;
    }
    renderRoleGroupsEditor(data.role_group_defaults || {});

    setStatus(
      `${data.message}\n` +
        `Сотрудников: ${data.summary.employee_count}, ресторанов: ${data.summary.restaurants}, ролей: ${data.summary.roles}, дней: ${data.summary.days_in_payroll ?? data.summary.days_in_template}, выходных: ${data.summary.weekend_days_in_payroll ?? data.summary.weekend_days_in_template ?? 0}.` +
        warnings
    );

    generateBtn.disabled = false;
    previewRows = [];
    previewTotal = 0;
    previewNextOffset = 0;
    previewPageSize = 150;
    previewFilter.value = "all";
    renderFilteredPreview();
    updateLoadMoreButton();
  } catch (error) {
    setStatus(`Ошибка: ${error.message}`);
  }
});

generateBtn.addEventListener("click", async () => {
  setStatus("Генерация графика...");
  generateBtn.disabled = true;
  downloadBtn.disabled = true;
  downloadT13Btn.disabled = true;

  try {
    const response = await fetch("/generate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        role_group_overrides: roleGroupOverrides,
      }),
    });

    const data = await response.json();
    if (!response.ok) {
      throw new Error(formatApiDetail(data.detail) || "Ошибка генерации");
    }

    const warnings = (data.warnings || []).length
      ? `\nПредупреждения:\n- ${data.warnings.join("\n- ")}`
      : "";

    setStatus(
      `${data.message}\n` +
        `Назначений: ${data.assignments_count}, сотрудников: ${data.employees_count}, дней: ${data.days_count}, нарушений лимитов: ${data.violations_count}, дефицитных обязательных смен: ${data.deficit_count}, межресторанных замен: ${data.cross_restaurant_count}.` +
        warnings
    );

    previewRows = data.preview || [];
    previewTotal = Number.isFinite(data.preview_total) ? data.preview_total : previewRows.length;
    previewPageSize = Number.isFinite(data.preview_page_size) ? data.preview_page_size : 150;
    previewNextOffset = Number.isFinite(data.preview_next_offset)
      ? data.preview_next_offset
      : previewRows.length;
    renderFilteredPreview();
    updateLoadMoreButton();
    downloadBtn.disabled = false;
    downloadT13Btn.disabled = false;
  } catch (error) {
    setStatus(`Ошибка: ${error.message}`);
  } finally {
    generateBtn.disabled = false;
  }
});

downloadBtn.addEventListener("click", () => {
  window.location.href = "/download";
});

downloadT13Btn.addEventListener("click", () => {
  window.location.href = "/download_t13";
});

downloadLogBtn.addEventListener("click", () => {
  window.location.href = "/download_log";
});

loadMoreBtn.addEventListener("click", async () => {
  if (previewNextOffset >= previewTotal) {
    updateLoadMoreButton();
    return;
  }

  loadMoreBtn.disabled = true;
  const previousText = loadMoreBtn.textContent;
  loadMoreBtn.textContent = "Загрузка...";

  try {
    const params = new URLSearchParams({
      offset: String(previewNextOffset),
      limit: String(previewPageSize),
    });
    const response = await fetch(`/preview?${params.toString()}`);
    const data = await response.json();
    if (!response.ok) {
      throw new Error(formatApiDetail(data.detail) || "Ошибка подгрузки записей");
    }

    const rows = data.rows || [];
    previewRows = previewRows.concat(rows);
    previewTotal = Number.isFinite(data.total) ? data.total : previewTotal;
    previewNextOffset = Number.isFinite(data.next_offset)
      ? data.next_offset
      : previewRows.length;

    renderFilteredPreview();
    updateLoadMoreButton();
  } catch (error) {
    setStatus(`Ошибка: ${error.message}`);
    loadMoreBtn.textContent = previousText;
    updateLoadMoreButton();
  }
});

previewFilter.addEventListener("change", renderFilteredPreview);

async function loadVersion() {
  try {
    const response = await fetch(`/version?t=${Date.now()}`);
    if (!response.ok) {
      return;
    }
    const data = await response.json();
    appVersionNode.textContent = `Версия: ${data.version}`;
  } catch (_error) {
    // no-op: версия не критична для работы интерфейса
  }
}

loadVersion();
