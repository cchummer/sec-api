function toggleContent(id) {
    const content = document.getElementById(id);
    if (!content) {
        console.error(`Element with ID ${id} not found`);
        return;
    }

    // Toggle visibility
    if (content.style.display === "none" || content.style.display === "") {
        content.style.display = "block";
    } else {
        content.style.display = "none";
    }
}

function loadFinDocuments(filingID) {
    fetch(`/documents/${filingID}`)
        .then(response => response.json())
        .then(data => {
            const container = document.getElementById(`documents-${filingID}`);
            container.innerHTML = `
                <h5>Documents:</h5>
                <ul>
                    ${data.textDocuments.map(doc => `<li>${doc.section_doc} - <button onclick="showAnalysis(${doc.text_document_id})">Analyze</button></li>`).join('')}
                    ${data.financialReports.map(rep => `<li>${rep.report_name} - <button onclick="showFinancialFacts(${rep.financial_report_id})">View Facts</button></li>`).join('')}
                </ul>
            `;
        });
}

document.addEventListener("DOMContentLoaded", () => {
    const accordions = document.querySelectorAll(".accordion");

    accordions.forEach(button => {
        button.addEventListener("click", () => {
            const panel = button.nextElementSibling;

            // Toggle visibility
            if (panel.style.maxHeight) {
                panel.style.maxHeight = null;
                button.classList.remove("active");
            } else {
                panel.style.maxHeight = panel.scrollHeight + "px";
                button.classList.add("active");
            }
        });
    });
});
document.addEventListener("DOMContentLoaded", function () {
    // Text section visibility toggle
    document.querySelectorAll(".show-text-btn").forEach(button => {
        button.addEventListener("click", function () {
            const sectionId = this.getAttribute("data-section-id");
            const textRow = document.getElementById(`text_${sectionId}`);
            
            if (textRow.style.display === "none") {
                textRow.style.display = "table-row";
                this.textContent = "Hide Text";
            } else {
                textRow.style.display = "none";
                this.textContent = "Show Text";
            }
        });
    });

    // Fetch section summary
    document.querySelectorAll(".section-summarize-btn").forEach(button => {
        button.addEventListener("click", function () {
            const filingId = this.getAttribute("data-filing-id");
            const sectionId = this.getAttribute("data-section-id");
            const summaryElement = document.getElementById(`summary_${sectionId}`);
            summaryElement.textContent = "Summarizing...";

            fetch(`/run_filing_section_summary/${filingId}/${sectionId}`)
                .then(response => response.json())
                .then(data => {
                    summaryElement.textContent = data.summary || "No summary available.";
                })
                .catch(error => {
                    console.error("Error fetching summary:", error);
                    summaryElement.textContent = "Error fetching summary.";
                });
        });
    });

    // Fetch document summary
    document.querySelectorAll(".doc-summarize-btn").forEach(button => {
        button.addEventListener("click", function () {
            const filingId = this.getAttribute("data-filing-id");
            const docId = this.getAttribute("data-doc-id");
            const summaryElement = document.getElementById(`summary_${docId}`);
            summaryElement.textContent = "Summarizing...";

            fetch(`/run_filing_document_summary/${filingId}/${docId}`)
                .then(response => response.json())
                .then(data => {
                    summaryElement.textContent = data.summary || "No summary available.";
                })
                .catch(error => {
                    console.error("Error fetching summary:", error);
                    summaryElement.textContent = "Error fetching summary.";
                });
        });
    });

    // Fetch section sentiment summary
    document.querySelectorAll(".section-sentiment-btn").forEach(button => {
        button.addEventListener("click", function () {
            const filingId = this.getAttribute("data-filing-id");
            const sectionId = this.getAttribute("data-section-id");
            const sentimentElement = document.getElementById(`sentiment_${sectionId}`);
            sentimentElement.innerHTML = "<li>Fetching sentiment summary...</li>";
            sentimentElement.style.display = "block";

            fetch(`/run_filing_section_sentiment_summary/${filingId}/${sectionId}`)
                .then(response => response.json())
                .then(data => {
                    sentimentElement.innerHTML = "";
                    if (Object.keys(data).length > 0) {
                        for (const [emotion, summary] of Object.entries(data)) {
                            const li = document.createElement("li");
                            li.innerHTML = `<strong>${emotion.charAt(0).toUpperCase() + emotion.slice(1)}:</strong> ${summary}`;
                            sentimentElement.appendChild(li);
                        }
                    } else {
                        sentimentElement.innerHTML = "<li>No sentiment data available.</li>";
                    }
                })
                .catch(error => {
                    console.error("Error fetching sentiment summary:", error);
                    sentimentElement.innerHTML = "<li>Error fetching sentiment summary.</li>";
                });
        });
    });

    // Perform industry level topic analysis
    document.querySelectorAll(".industry-topic-btn").forEach(button => {
        button.addEventListener("click", function () {
            const targetSic = this.getAttribute("data-target-sic");
            const summaryElement = document.getElementById(`topics_${targetSic}`);
            summaryElement.textContent = "Performing topic analysis...";

            fetch(`/run_industry_topic_analysis/${targetSic}`)
                .then(response => response.json())
                .then(data => {
                    summaryElement.textContent = data.summary || "No topic analysis available.";
                })
                .catch(error => {
                    console.error("Error performing analysis:", error);
                    summaryElement.textContent = "Error performing analysis.";
                });
        });
    });
});

