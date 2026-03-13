const uploadForm = document.getElementById("uploadForm");
const csvFileInput = document.getElementById("csvFile");
const statusBox = document.getElementById("status");
const resultBox = document.getElementById("result");
const resultContent = document.getElementById("resultContent");

function showStatus(message, type) {
  statusBox.textContent = message;
  statusBox.className = `status ${type}`;
}

function showResult(data) {
  resultContent.textContent = JSON.stringify(data, null, 2);
  resultBox.classList.remove("hidden");
}

uploadForm.addEventListener("submit", async (event) => {
  event.preventDefault();

  const file = csvFileInput.files[0];

  if (!file) {
    showStatus("Please select a CSV file first.", "error");
    return;
  }

  const formData = new FormData();
  formData.append("file", file);

  showStatus("Uploading file...", "success");
  resultBox.classList.add("hidden");

  try {
    const response = await fetch("/ingest/letterboxd/upload", {
      method: "POST",
      body: formData,
    });

    const data = await response.json();

    if (!response.ok) {
      showStatus(data.detail || "Upload failed.", "error");
      showResult(data);
      return;
    }

    showStatus("Upload completed successfully.", "success");
    showResult(data);
    uploadForm.reset();
  } catch (error) {
    showStatus("Could not connect to the API.", "error");
    showResult({ error: error.message });
  }
});