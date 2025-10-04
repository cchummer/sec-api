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

// Bar chart related functions
function parseDateNoTZ(dateStr) {
    const parts = dateStr.split('-'); // ["YYYY", "MM", "DD"]
    return new Date(parts[0], parts[1]-1, parts[2]); 
}
function formatDateLabel(dateStr, timeframe) {
    const date = parseDateNoTZ(dateStr); // Manually construct, no room for timezone manipulation (was previously causing issue, midnight UTC-> UTC-7{Phx time} == previous{wrong} date)
    switch(timeframe) {
        case "day":
            return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric" }).format(date);
        case "week":
            return "Week of " + new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric" }).format(date);
        case "month":
            return new Intl.DateTimeFormat("en-US", { month: "long", year: "numeric" }).format(date);
        case "quarter":
            const q = Math.floor(date.getMonth() / 3) + 1;
            return `Q${q} ${date.getFullYear()}`;
        case "year":
            return date.getFullYear().toString();
        default:
            return dateStr;
    }
}
function renderBarChart(canvasId, data, plotHorizontal=false, countLabel=null, highlightBar=null, labelFormatter=null, onBarClick=null) {
    
    const labels = data.map(d => 
        (typeof labelFormatter === "function") ? labelFormatter(d.label) : d.label
    );
    const counts = data.map(d => d.value);

    const backgroundColors = data.map(item => {
        if (!highlightBar) return 'rgba(54, 162, 235, 0.6)';
        return item.label === highlightBar ? 'rgba(26, 188, 156, 1)' : 'rgba(54, 162, 235, 0.6)';
    });

    const ctx = document.getElementById(canvasId).getContext('2d');
    const chart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: countLabel || 'Count',
                data: counts,
                backgroundColor: backgroundColors,
                borderColor: 'rgba(54, 162, 235, 1)',
                borderWidth: 1,
                borderRadius: 4
            }]
        },
        options: {
            maintainAspectRatio: false,
            indexAxis: plotHorizontal ? 'y' : 'x',
            scales: {
                x: {
                    beginAtZero: plotHorizontal // if horizontal, zero on x
                },
                y: {
                    beginAtZero: !plotHorizontal // if vertical, zero on y
                }
            },
            responsive: true,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const value = plotHorizontal ? context.parsed.x : context.parsed.y;
                            return `${value} filings`;
                        }

                    }
                }
            },
            onClick: function(evt) {
                if (!onBarClick) return; // no click handler
                const points = chart.getElementsAtEventForMode(evt, 'nearest', { intersect: true }, true);
                if (points.length) {
                    const index = points[0].index;
                    const clickedLabel = data[index].label;
                    onBarClick(clickedLabel);
                }
            }
        }
    });

    return chart;
}

// Industry Explorer related functions
// Toggle division (2-digit SIC codes - the top level)
function toggleDivision(divisionCode) {
    const content = document.getElementById('content-' + divisionCode);
    const icon = document.getElementById('icon-' + divisionCode);
    const header = document.querySelector(`[onclick="toggleDivision('${divisionCode}')"]`).closest('.division-header');
    
    if (!content || !icon || !header) {
        console.error('Division elements not found for code:', divisionCode);
        return;
    }
    
    if (content.style.display === 'none' || content.style.display === '') {
        // Expand division
        content.style.display = 'block';
        icon.classList.add('expanded');
        header.classList.add('active');
    } else {
        // Collapse division
        content.style.display = 'none';
        icon.classList.remove('expanded');
        header.classList.remove('active');
        
        // Also collapse all major groups within this division
        const majorGroups = content.querySelectorAll('.major-group-content');
        const majorIcons = content.querySelectorAll('.major-group .expand-icon');
        const majorHeaders = content.querySelectorAll('.major-group-header');
        
        majorGroups.forEach(mg => mg.style.display = 'none');
        majorIcons.forEach(icon => icon.classList.remove('expanded'));
        majorHeaders.forEach(header => header.classList.remove('active'));
    }
}
// Toggle major group (3-digit SIC codes - the middle level)
function toggleMajorGroup(majorCode) {
    const content = document.getElementById('major-content-' + majorCode);
    const icon = document.getElementById('major-icon-' + majorCode);
    const header = document.querySelector(`[onclick="toggleMajorGroup('${majorCode}')"]`).closest('.major-group-header');
    
    if (!content || !icon || !header) {
        console.error('Major group elements not found for code:', majorCode);
        return;
    }
    
    if (content.style.display === 'none' || content.style.display === '') {
        // Expand major group
        content.style.display = 'block';
        icon.classList.add('expanded');
        header.classList.add('active');
    } else {
        // Collapse major group
        content.style.display = 'none';
        icon.classList.remove('expanded');
        header.classList.remove('active');
    }
}

// Filter industries based on search input
function filterIndustries() {
    const searchTerm = document.getElementById('industrySearch').value.toLowerCase();
    const divisions = document.querySelectorAll('.division-group');
    const majorGroups = document.querySelectorAll('.major-group');
    const industries = document.querySelectorAll('.industry-item');
    const noResults = document.getElementById('no-results');
    
    let hasVisibleItems = false;

    if (searchTerm === '') {
        // Show all items if search is empty
        divisions.forEach(div => {
            div.style.display = 'block';
            // Reset to collapsed state
            const content = div.querySelector('.division-content');
            const icon = div.querySelector('.expand-icon');
            const header = div.querySelector('.division-header');
            if (content && icon && header) {
                content.style.display = 'none';
                icon.classList.remove('expanded');
                header.classList.remove('active');
            }
        });
        
        majorGroups.forEach(major => {
            major.style.display = 'block';
            // Reset to collapsed state
            const content = major.querySelector('.major-group-content');
            const icon = major.querySelector('.expand-icon');
            const header = major.querySelector('.major-group-header');
            if (content && icon && header) {
                content.style.display = 'none';
                icon.classList.remove('expanded');
                header.classList.remove('active');
            }
        });
        
        industries.forEach(ind => ind.style.display = 'block');
        noResults.style.display = 'none';
        hasVisibleItems = true;
    } else {
        // Filter based on search term
        industries.forEach(industry => {
            const searchText = industry.getAttribute('data-search-text');
            if (searchText && searchText.toLowerCase().includes(searchTerm)) {
                industry.style.display = 'block';
                hasVisibleItems = true;
                
                // Show and expand parent major group and division
                const majorGroup = industry.closest('.major-group');
                const division = industry.closest('.division-group');
                
                if (majorGroup) {
                    majorGroup.style.display = 'block';
                    // Expand major group
                    const majorContent = majorGroup.querySelector('.major-group-content');
                    const majorIcon = majorGroup.querySelector('.expand-icon');
                    const majorHeader = majorGroup.querySelector('.major-group-header');
                    if (majorContent && majorIcon && majorHeader) {
                        majorContent.style.display = 'block';
                        majorIcon.classList.add('expanded');
                        majorHeader.classList.add('active');
                    }
                }
                
                if (division) {
                    division.style.display = 'block';
                    // Expand division
                    const divContent = division.querySelector('.division-content');
                    const divIcon = division.querySelector('.expand-icon');
                    const divHeader = division.querySelector('.division-header');
                    if (divContent && divIcon && divHeader) {
                        divContent.style.display = 'block';
                        divIcon.classList.add('expanded');
                        divHeader.classList.add('active');
                    }
                }
            } else {
                industry.style.display = 'none';
            }
        });

        // Hide major groups with no visible industries
        majorGroups.forEach(majorGroup => {
            const visibleIndustries = majorGroup.querySelectorAll('.industry-item:not([style*="display: none"])');
            if (visibleIndustries.length === 0) {
                majorGroup.style.display = 'none';
            }
        });

        // Hide divisions with no visible major groups
        divisions.forEach(division => {
            const visibleMajorGroups = division.querySelectorAll('.major-group:not([style*="display: none"])');
            if (visibleMajorGroups.length === 0) {
                division.style.display = 'none';
            }
        });
    }

    noResults.style.display = hasVisibleItems ? 'none' : 'block';
}

// Treemap functions
// Create treemap data for industry divisions
function createIndustryDivsTreemapData(sicHierarchy) {
    const data = [];
    const indDivs = Object.keys(sicHierarchy);
    
    indDivs.forEach((divCode, index) => {
        const divData = sicHierarchy[divCode];
        
        data.push({
            category: divCode,
            value: divData.total_count,
            label: divData.name,
            divData: divData
        });

    });

    return data;
}
// Create treemap data for industry groups
function createMajorGroupsTreemapData(divData) {
    const data = [];
    
    if (!divData || !divData.groups) {
        return data;
    }
    
    Object.keys(divData.groups).forEach((groupCode, index) => {
        const groupData = divData.groups[groupCode];
        
        data.push({
            category: groupCode,
            value: groupData.total_count,
            label: groupData.name,
            groupData: groupData
        });
    });

    return data;
}

// Helper function to update the selected major group display
function updateSelectedDivision(divCode, divName) {
    const selectedDisplay = document.querySelector('.selected-industry-div');
    if (selectedDisplay) {
        selectedDisplay.textContent = `Selected: ${divCode} - ${divName}`;
        selectedDisplay.classList.remove('no-selection');
    }
}
// Initialize divisions treemap
function initDivisionsTreemap(sicHierarchy) {
    const ctx = document.getElementById('divisionsTreemap').getContext('2d');
    const data = createIndustryDivsTreemapData(sicHierarchy);

    if (divisionsTreemapChart) {
        divisionsTreemapChart.destroy();
    }

    console.log('Initializing chart object for industry division treemap. data:')
    console.log(data)
    divisionsTreemapChart = new Chart(ctx, {
        type: 'treemap',
        data: {
            datasets: [{
                label: 'Divisions',
                tree: data,
                key: 'value',
                borderWidth: 2,
                borderColor: '#fff',
                spacing: 1,
                backgroundColor: (ctx) => {
                    // Generate colors based on the data index for visual distinction
                    const colors = [
                        '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', 
                        '#FECA57', '#FF9FF3', '#54A0FF', '#5F27CD',
                        '#00D2D3', '#FF9F43', '#0ABDE3', '#10AC84'
                    ];
                    return colors[ctx.dataIndex % colors.length];
                },
                labels: {
                    display: true,
                    color: 'white',
                    font: {
                        size: 10,
                        weight: 'bold'
                    },
                    formatter: (ctx) => {
                        const item = ctx.raw._data;
                        if (!item) return '';
                        
                        const rect = ctx.raw;
                        const width = rect.w;
                        const height = rect.h;
                        
                        // Show division code for smaller tiles, code + name for larger ones
                        if (width < 60 || height < 30) {
                            return item.category;
                        }
                        
                        return [item.category, item.label];
                    }
                }
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    displayColors: false,
                    callbacks: {
                        title: (context) => {
                            const item = context[0].raw._data;
                            if (item) {
                                return `${item.category} - ${item.label}`;
                            }
                            return 'Division';
                        },
                        label: (context) => {
                            const item = context.raw._data;
                            if (item) {
                                return `${item.value} filings`;
                            }
                            return `${context.raw.v || context.raw.s || 'N/A'} filings`;
                        }
                    }
                }
            },
            onClick: (event, elements) => {
                if (elements.length > 0) {
                    const element = elements[0];
                    
                    // Try to get data from different possible locations
                    let itemData = element.element.$context?.raw?._data;
                    
                    if (!itemData) {
                        // Alternative way to access the original data
                        const dataIndex = element.index;
                        itemData = data[dataIndex];
                    }
                    
                    if (itemData) {
                        console.log('Clicked division:', itemData.category, itemData.divData);
                        
                        // Update the UI
                        updateSelectedDivision(itemData.category, itemData.label);
                        
                        // Initialize/update the divisions treemap
                        initMajorGroupsTreemap(itemData.divData);
                    }
                }
            }    
        }
    });
}
// Initialize industry major groups treemap
function initMajorGroupsTreemap(divisionData) {
    const ctx = document.getElementById('majorGroupsTreemap').getContext('2d');
    const data = createMajorGroupsTreemapData(divisionData);

    if (majorGroupsTreemapChart) {
        majorGroupsTreemapChart.destroy();
    }

    console.log('Initializing chart object for industry major groups treemap. data:')
    console.log(data)

    // If no data, show empty chart with message
    if (data.length === 0) {
        // Clear the canvas
        ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
        
        // Draw "No data" message
        ctx.fillStyle = '#999';
        ctx.font = '16px Arial';
        ctx.textAlign = 'center';
        ctx.fillText('Click a division to view its major groups', ctx.canvas.width / 2, ctx.canvas.height / 2);
        return;
    }
    
    majorGroupsTreemapChart = new Chart(ctx, {
        type: 'treemap',
        data: {
            datasets: [{
                label: 'Major Groups',
                tree: data,
                key: 'value',
                borderWidth: 2,
                borderColor: '#fff',
                spacing: 1,
                backgroundColor: (ctx) => {
                    // Use different colors for groups (blues and greens)
                    const colors = [
                        '#3498DB', '#2ECC71', '#1ABC9C', '#16A085',
                        '#27AE60', '#2980B9', '#8E44AD', '#9B59B6',
                        '#34495E', '#2C3E50', '#17A2B8', '#28A745'
                    ];
                    return colors[ctx.dataIndex % colors.length];
                },
                labels: {
                    display: true,
                    color: 'white',
                    font: {
                        size: 10,
                        weight: 'bold'
                    },
                    formatter: (ctx) => {
                        const item = ctx.raw._data;
                        if (!item) return '';
                        
                        const rect = ctx.raw;
                        const width = rect.w;
                        const height = rect.h;
                        
                        // Show code for smaller tiles, code + name for larger ones
                        if (width < 60 || height < 30) {
                            return item.category;
                        }
                        
                        return [item.category, item.label];
                    }
                }
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    displayColors: false,
                    callbacks: {
                        title: (context) => {
                            const item = context[0].raw._data;
                            if (item) {
                                return `${item.category} - ${item.label}`;
                            }
                            return 'Major Group';
                        },
                        label: (context) => {
                            const item = context.raw._data;
                            if (item) {
                                return `${item.value} filings`;
                            }
                            return `${context.raw.v || context.raw.s || 'N/A'} filings`;
                        }
                    }
                }
            },
            onClick: (event, elements) => {
                if (elements.length > 0) {
                    const element = elements[0];
                    
                    let itemData = element.element.$context?.raw?._data;
                    
                    if (!itemData) {
                        const dataIndex = element.index;
                        itemData = data[dataIndex];
                    }
                    
                    if (itemData) {
                        console.log('Clicked major group:', itemData.category, itemData.groupData);
                        jumpToMajorGroup(itemData.category);
                    }
                }
            }    
        }
    });
}
// Function to handle jumping to and expanding a major group in the industry explorer
function jumpToMajorGroup(majorGroupCode) {
    console.log('Jumping to major group:', majorGroupCode);
    
    // Find the major group header
    const majorGroupHeader = document.querySelector(`[onclick="toggleMajorGroup('${majorGroupCode}')"]`);
    
    if (!majorGroupHeader) {
        console.error('Major group header not found for code:', majorGroupCode);
        return;
    }
    
    // Find the parent division
    const divisionGroup = majorGroupHeader.closest('.division-group');
    if (!divisionGroup) {
        console.error('Parent division not found for major group:', majorGroupCode);
        return;
    }
    
    // Get division code from the division header
    const divisionHeader = divisionGroup.querySelector('.division-header-content[onclick*="toggleDivision"]');
    if (!divisionHeader) {
        console.error('Division header not found');
        return;
    }
    
    const divisionOnClick = divisionHeader.getAttribute('onclick');
    const divisionCodeMatch = divisionOnClick.match(/toggleDivision\('([^']+)'\)/);
    
    if (!divisionCodeMatch) {
        console.error('Could not extract division code');
        return;
    }
    
    const divisionCode = divisionCodeMatch[1];
    
    // First, ensure the parent division is expanded
    const divisionContent = document.getElementById('content-' + divisionCode);
    const divisionIcon = document.getElementById('icon-' + divisionCode);
    const divisionHeaderElement = divisionHeader.closest('.division-header');
    
    if (divisionContent && divisionContent.style.display !== 'block') {
        // Expand the division
        divisionContent.style.display = 'block';
        if (divisionIcon) divisionIcon.classList.add('expanded');
        if (divisionHeaderElement) divisionHeaderElement.classList.add('active');
    }
    
    // Now expand the major group
    const majorGroupContent = document.getElementById('major-content-' + majorGroupCode);
    const majorGroupIcon = document.getElementById('major-icon-' + majorGroupCode);
    const majorGroupHeaderElement = majorGroupHeader.closest('.major-group-header');
    
    if (majorGroupContent && majorGroupIcon && majorGroupHeaderElement) {
        // Expand the major group
        majorGroupContent.style.display = 'block';
        majorGroupIcon.classList.add('expanded');
        majorGroupHeaderElement.classList.add('active');
        
        // Smooth scroll to the major group with some offset for better visibility
        setTimeout(() => {
            majorGroupHeaderElement.scrollIntoView({
                behavior: 'smooth',
                block: 'center'
            });
            
            // Add a brief highlight effect
            majorGroupHeaderElement.style.boxShadow = '0 0 20px rgba(33, 150, 243, 0.8)';
            majorGroupHeaderElement.style.transition = 'box-shadow 0.3s ease';
            
            // Remove highlight after animation
            setTimeout(() => {
                majorGroupHeaderElement.style.boxShadow = '';
                setTimeout(() => {
                    majorGroupHeaderElement.style.transition = '';
                }, 300);
            }, 1500);
            
        }, 100); // Small delay to ensure DOM is updated
        
    } else {
        console.error('Major group content or icon not found for code:', majorGroupCode);
    }
}

// Debug function to test accordion functionality
function debugAccordion() {
    console.log('=== ACCORDION DEBUG INFO ===');
    
    // Check divisions
    const divisions = document.querySelectorAll('.division-group');
    console.log(`Found ${divisions.length} divisions`);
    
    divisions.forEach((div, index) => {
        const header = div.querySelector('.division-header-content[onclick*="toggleDivision"]');
        const content = div.querySelector('.division-content');
        const icon = div.querySelector('.expand-icon');
        
        console.log(`Division ${index + 1}:`, {
            hasHeader: !!header,
            hasContent: !!content,
            hasIcon: !!icon,
            contentDisplay: content ? content.style.display : 'N/A',
            iconExpanded: icon ? icon.classList.contains('expanded') : 'N/A'
        });
    });
    
    // Check major groups
    const majorGroups = document.querySelectorAll('.major-group');
    console.log(`Found ${majorGroups.length} major groups`);
    
    majorGroups.forEach((group, index) => {
        const header = group.querySelector('.major-group-header-content[onclick*="toggleMajorGroup"]');
        const content = group.querySelector('.major-group-content');
        const icon = group.querySelector('.expand-icon');
        
        console.log(`Major Group ${index + 1}:`, {
            hasHeader: !!header,
            hasContent: !!content,
            hasIcon: !!icon,
            contentDisplay: content ? content.style.display : 'N/A',
            iconExpanded: icon ? icon.classList.contains('expanded') : 'N/A'
        });
    });
}