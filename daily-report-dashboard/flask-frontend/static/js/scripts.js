function toggleContent(id) {
    const content = document.getElementById(id);
    if (content.style.display === "block") {
        content.style.display = "none";
    } else {
        content.style.display = "block";
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
