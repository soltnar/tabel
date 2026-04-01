const payrollInput = document.getElementById("payrollFile");
const timesheetInput = document.getElementById("timesheetFile");

const uploadBtn = document.getElementById("uploadBtn");
const generateBtn = document.getElementById("generateBtn");
const downloadBtn = document.getElementById("downloadBtn");
const downloadT13Btn = document.getElementById("downloadT13Btn");
const downloadLogBtn = document.getElementById("downloadLogBtn");
const previewFilter = document.getElementById("previewFilter");
const previewMeta = document.getElementById("previewMeta");
const appVersionNode = document.getElementById("appVersion");

const statusNode = document.getElementById("status");
const previewBody = document.querySelector("#previewTable tbody");
let previewRows = [];

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
    td.colSpan = 9;
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

    let statusText = "ОК";
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
      `${row.start}-${row.end}`,
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

uploadBtn.addEventListener("click", async () => {
  const payrollFile = payrollInput.files[0];
  const timesheetFile = timesheetInput.files[0];

  if (!payrollFile || !timesheetFile) {
    setStatus("Выберите 2 файла (расчетные листки и табель) перед загрузкой.");
    return;
  }

  setStatus("Загрузка и разбор файлов...");
  generateBtn.disabled = true;
  downloadBtn.disabled = true;
  downloadT13Btn.disabled = true;

  const formData = new FormData();
  formData.append("payroll_file", payrollFile);
  formData.append("timesheet_file", timesheetFile);

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

    setStatus(
      `${data.message}\n` +
        `Сотрудников: ${data.summary.employee_count}, ресторанов: ${data.summary.restaurants}, ролей: ${data.summary.roles}, дней: ${data.summary.days_in_template}, выходных в шаблоне: ${data.summary.weekend_days_in_template ?? 0}.` +
        warnings
    );

    generateBtn.disabled = false;
    previewRows = [];
    previewFilter.value = "all";
    renderFilteredPreview();
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
    renderFilteredPreview();
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

previewFilter.addEventListener("change", renderFilteredPreview);

async function loadVersion() {
  try {
    const response = await fetch("/version");
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
