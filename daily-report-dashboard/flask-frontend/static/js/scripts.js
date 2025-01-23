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