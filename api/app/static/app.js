const uploadForm = document.getElementById("uploadForm");
const csvFileInput = document.getElementById("csvFile");
const dropzone = document.getElementById("dropzone");
const submitButton = document.getElementById("submitButton");
const statusBox = document.getElementById("status");
const resultBox = document.getElementById("result");
const resultBadge = document.getElementById("resultBadge");
const resultContent = document.getElementById("resultContent");
const fileName = document.getElementById("fileName");
const fileMeta = document.getElementById("fileMeta");
const fileMessage = document.getElementById("fileMessage");

let fallbackFile = null;

function formatFileSize(sizeBytes) {
  if (!Number.isFinite(sizeBytes) || sizeBytes < 1024) {
    return `${sizeBytes || 0} B`;
  }

  const units = ["KB", "MB", "GB"];
  let value = sizeBytes / 1024;
  let unitIndex = 0;

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }

  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unitIndex]}`;
}

function getSelectedFile() {
  return csvFileInput.files[0] || fallbackFile;
}

function showFieldMessage(message) {
  if (!message) {
    fileMessage.textContent = "";
    fileMessage.className = "field-message hidden";
    dropzone.classList.remove("invalid");
    return;
  }

  fileMessage.textContent = message;
  fileMessage.className = "field-message error";
  dropzone.classList.add("invalid");
}

function updateFileSummary(file) {
  if (!file) {
    fileName.textContent = "No file selected";
    fileMeta.textContent = "Choose a Letterboxd CSV export to continue.";
    return;
  }

  fileName.textContent = file.name;
  fileMeta.textContent = `${formatFileSize(file.size)} file ready for upload`;
}

function validateFile(file) {
  if (!file) {
    return "Please select a CSV file first.";
  }

  if (!file.name.toLowerCase().endsWith(".csv")) {
    return "Only .csv files are supported.";
  }

  if (file.size === 0) {
    return "The selected file is empty.";
  }

  return "";
}

function setResultState(type, label) {
  resultBadge.textContent = label;
  resultBadge.className = `result-badge ${type}`;
}

function setUploadingState(isUploading) {
  submitButton.disabled = isUploading;
  submitButton.textContent = isUploading ? "Uploading..." : "Upload dataset";
}

function showStatus(message, type) {
  statusBox.textContent = message;
  statusBox.className = `status ${type}`;
  statusBox.classList.remove("hidden");
}

function showResult(data, type = "success") {
  resultContent.textContent = JSON.stringify(data, null, 2);
  resultBox.classList.remove("hidden");
  setResultState(type, type === "error" ? "Error" : "Success");
}

function clearResult() {
  resultBox.classList.add("hidden");
  resultContent.textContent = "";
  setResultState("info", "Ready");
}

function syncSelectedFile(fileList) {
  const file = fileList && fileList[0] ? fileList[0] : null;

  if (file && typeof DataTransfer !== "undefined") {
    const transfer = new DataTransfer();
    transfer.items.add(file);
    csvFileInput.files = transfer.files;
    fallbackFile = null;
  } else {
    fallbackFile = file;
  }

  const selectedFile = getSelectedFile();
  updateFileSummary(selectedFile);
  showFieldMessage(selectedFile ? validateFile(selectedFile) : "");
}

uploadForm.addEventListener("submit", async (event) => {
  event.preventDefault();

  const file = getSelectedFile();
  const validationMessage = validateFile(file);

  if (validationMessage) {
    showFieldMessage(validationMessage);
    clearResult();
    showStatus(validationMessage, "error");
    return;
  }

  const formData = new FormData();
  formData.append("file", file);

  showFieldMessage("");
  clearResult();
  setUploadingState(true);
  showStatus("Uploading file to the ingestion pipeline...", "loading");

  try {
    const response = await fetch("/ingest/letterboxd/upload", {
      method: "POST",
      body: formData,
    });

    const data = await response.json().catch(() => ({
      detail: "The API returned an unexpected response.",
    }));

    if (!response.ok) {
      showStatus(data.detail || "Upload failed.", "error");
      showResult(data, "error");
      return;
    }

    showStatus("Upload completed successfully.", "success");
    showResult(data, "success");
    uploadForm.reset();
    fallbackFile = null;
    updateFileSummary(null);
  } catch (error) {
    showStatus("Could not connect to the API.", "error");
    showResult({ error: error.message }, "error");
  } finally {
    setUploadingState(false);
  }
});

csvFileInput.addEventListener("change", () => {
  fallbackFile = null;
  const file = getSelectedFile();
  updateFileSummary(file);
  showFieldMessage(file ? validateFile(file) : "");
});

["dragenter", "dragover"].forEach((eventName) => {
  dropzone.addEventListener(eventName, (event) => {
    event.preventDefault();
    dropzone.classList.add("active");
  });
});

["dragleave", "dragend", "drop"].forEach((eventName) => {
  dropzone.addEventListener(eventName, (event) => {
    event.preventDefault();
    if (eventName !== "drop") {
      dropzone.classList.remove("active");
    }
  });
});

dropzone.addEventListener("drop", (event) => {
  dropzone.classList.remove("active");
  syncSelectedFile(event.dataTransfer.files);
});

updateFileSummary(null);
setResultState("info", "Ready");
