def build_sic_hierarchy(parsed_by_industry):
    """
    Build hierarchical SIC structure from parsed_by_industry data
    
    Args:
        parsed_by_industry: List of dicts with keys: whole_sic_code, sic_desc, count
    
    Returns:
        Dictionary with hierarchical structure for template rendering
    """
    
    # SIC divisions definitions
    SIC_DIVISIONS = {
        "01-09": "Agriculture, Forestry, and Fishing",
        "10-14": "Mining",
        "15-17": "Construction", 
        "20-39": "Manufacturing",
        "40-49": "Transportation, Communications, Electric, Gas, and Sanitary Services",
        "50-51": "Wholesale Trade",
        "52-59": "Retail Trade",
        "60-67": "Finance, Insurance, and Real Estate",
        "70-89": "Services",
        "91-97": "Public Administration",
        "99": "Nonclassifiable Establishments"
    }

    # SIC major group definitions (first two digits)
    SIC_MAJOR_GROUPS = {
        "01": "Agricultural Production - Crops",
        "02": "Agricultural Production - Livestock", 
        "07": "Agricultural Services",
        "08": "Forestry",
        "09": "Fishing, Hunting, and Trapping",
        "10": "Metal Mining",
        "12": "Coal Mining", 
        "13": "Oil and Gas Extraction",
        "14": "Mining and Quarrying of Nonmetallic Minerals",
        "15": "Building Construction",
        "16": "Heavy Construction Other Than Building",
        "17": "Construction Special Trade Contractors",
        "20": "Food and Kindred Products",
        "21": "Tobacco Products",
        "22": "Textile Mill Products", 
        "23": "Apparel and Other Finished Products",
        "24": "Lumber and Wood Products",
        "25": "Furniture and Fixtures",
        "26": "Paper and Allied Products",
        "27": "Printing, Publishing, and Allied Industries",
        "28": "Chemicals and Allied Products",
        "29": "Petroleum Refining and Related Industries",
        "30": "Rubber and Miscellaneous Plastics Products",
        "31": "Leather and Leather Products", 
        "32": "Stone, Clay, Glass, and Concrete Products",
        "33": "Primary Metal Industries",
        "34": "Fabricated Metal Products",
        "35": "Industrial and Commercial Machinery",
        "36": "Electronic and Other Electrical Equipment",
        "37": "Transportation Equipment",
        "38": "Measuring, Analyzing, and Controlling Instruments",
        "39": "Miscellaneous Manufacturing Industries",
        "40": "Railroad Transportation",
        "41": "Local and Suburban Transit",
        "42": "Motor Freight Transportation",
        "43": "United States Postal Service",
        "44": "Water Transportation", 
        "45": "Transportation by Air",
        "46": "Pipelines, Except Natural Gas",
        "47": "Transportation Services",
        "48": "Communications",
        "49": "Electric, Gas, and Sanitary Services",
        "50": "Wholesale Trade-Durable Goods",
        "51": "Wholesale Trade-Nondurable Goods",
        "52": "Building Materials, Hardware, Garden Supply",
        "53": "General Merchandise Stores",
        "54": "Food Stores",
        "55": "Automotive Dealers and Gasoline Service Stations",
        "56": "Apparel and Accessory Stores",
        "57": "Home Furniture, Furnishings, and Equipment Stores",
        "58": "Eating and Drinking Places",
        "59": "Miscellaneous Retail",
        "60": "Depository Institutions",
        "61": "Nondepository Credit Institutions",
        "62": "Security and Commodity Brokers",
        "63": "Insurance Carriers",
        "64": "Insurance Agents, Brokers, and Service",
        "65": "Real Estate",
        "67": "Holding and Other Investment Offices",
        "70": "Hotels, Rooming Houses, Camps",
        "72": "Personal Services",
        "73": "Business Services",
        "75": "Automotive Repair, Services, and Parking",
        "76": "Miscellaneous Repair Services",
        "78": "Motion Pictures",
        "79": "Amusement and Recreation Services",
        "80": "Health Services",
        "81": "Legal Services",
        "82": "Educational Services",
        "83": "Social Services",
        "84": "Museums, Art Galleries, and Botanical Gardens",
        "86": "Membership Organizations",
        "87": "Engineering, Accounting, Research, Management",
        "88": "Private Households",
        "89": "Miscellaneous Services",
        "91": "Executive, Legislative, and General Government",
        "92": "Justice, Public Order, and Safety",
        "93": "Public Finance, Taxation, and Monetary Policy",
        "94": "Administration of Human Resource Programs",
        "95": "Administration of Environmental Quality",
        "96": "Administration of Economic Programs",
        "97": "National Security and International Affairs",
        "99": "Nonclassifiable Establishments"
    }

    def get_division_from_sic(sic_code):
        """Determine division from SIC code"""
        try:
            first_two_digits = int(sic_code[:2])
        except (ValueError, IndexError):
            return "Unknown"
        
        if 1 <= first_two_digits <= 9:
            return "01-09"
        elif 10 <= first_two_digits <= 14:
            return "10-14"
        elif 15 <= first_two_digits <= 17:
            return "15-17"
        elif 20 <= first_two_digits <= 39:
            return "20-39"
        elif 40 <= first_two_digits <= 49:
            return "40-49"
        elif 50 <= first_two_digits <= 51:
            return "50-51"
        elif 52 <= first_two_digits <= 59:
            return "52-59"
        elif 60 <= first_two_digits <= 67:
            return "60-67"
        elif 70 <= first_two_digits <= 89:
            return "70-89"
        elif 91 <= first_two_digits <= 97:
            return "91-97"
        elif first_two_digits == 99:
            return "99"
        else:
            return "Unknown"

    # Initialize hierarchy structure
    hierarchy = {}
    
    # Build hierarchy from parsed_by_industry data
    for industry in parsed_by_industry:
        sic_code = industry['whole_sic_code']
        sic_desc = industry['sic_desc']
        count = industry['count']
        
        # Determine division and major group
        division_code = get_division_from_sic(sic_code)
        major_group_code = sic_code[:2]
        
        # Initialize division if it doesn't exist
        if division_code not in hierarchy:
            hierarchy[division_code] = {
                'name': SIC_DIVISIONS.get(division_code, 'Unknown Division'),
                'groups': {},
                'total_count': 0
            }
        
        # Initialize major group if it doesn't exist
        if major_group_code not in hierarchy[division_code]['groups']:
            hierarchy[division_code]['groups'][major_group_code] = {
                'name': SIC_MAJOR_GROUPS.get(major_group_code, f'Division {major_group_code}'),
                'industries': {},
                'total_count': 0
            }
        
        # Add the industry
        hierarchy[division_code]['groups'][major_group_code]['industries'][sic_code] = {
            'desc': sic_desc,
            'count': count
        }
        
        # Update totals
        hierarchy[division_code]['total_count'] += count
        hierarchy[division_code]['groups'][major_group_code]['total_count'] += count
    
    # Sort hierarchy for consistent display
    sorted_hierarchy = {}
    for div_code in sorted(hierarchy.keys()):
        sorted_hierarchy[div_code] = {
            'name': hierarchy[div_code]['name'],
            'total_count': hierarchy[div_code]['total_count'],
            'groups': {}
        }
        
        # Sort major groups within divisions
        for group_code in sorted(hierarchy[div_code]['groups'].keys()):
            group = hierarchy[div_code]['groups'][group_code]
            sorted_hierarchy[div_code]['groups'][group_code] = {
                'name': group['name'],
                'total_count': group['total_count'],
                'industries': {}
            }
            
            # Sort industries within group
            for sic_code in sorted(group['industries'].keys()):
                sorted_hierarchy[div_code]['groups'][group_code]['industries'][sic_code] = group['industries'][sic_code]
    
    return sorted_hierarchy