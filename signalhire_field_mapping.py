#!/usr/bin/env python3
"""
SignalHire Candidate Data Field Mapping
Based on official SignalHire API documentation at signalhire.com/api/person
"""

def extract_candidate_data(record, batch_id, timestamp):
    """
    Extract candidate data from SignalHire webhook payload using correct field mapping
    
    Args:
        record: SignalHire webhook record
        batch_id: Batch identifier
        timestamp: Processing timestamp
        
    Returns:
        dict: Properly mapped candidate data
    """
    
    # Get the candidate object (contains all the rich data)
    candidate = record.get('candidate', {})
    
    # Extract basic info
    full_name = candidate.get('fullName', '')
    name_parts = full_name.split(' ', 1) if full_name else ['', '']
    first_name = name_parts[0] if len(name_parts) > 0 else ''
    last_name = name_parts[1] if len(name_parts) > 1 else ''
    
    # Extract location info
    locations = candidate.get('locations', [])
    location = locations[0].get('name', '') if locations else ''
    
    # Parse location for country/city
    location_parts = location.split(', ') if location else []
    city = location_parts[0] if len(location_parts) > 0 else ''
    country = location_parts[-1] if len(location_parts) > 0 else ''
    
    # Extract job info from experience (most recent)
    experience = candidate.get('experience', [])
    current_job = experience[0] if experience else {}
    job_title = current_job.get('position', candidate.get('headLine', ''))
    company = current_job.get('company', '')
    industry = current_job.get('industry', '')
    
    # Extract contact information
    contacts = candidate.get('contacts', [])
    emails = [c['value'] for c in contacts if c.get('type') == 'email']
    phones = [c['value'] for c in contacts if c.get('type') == 'phone']
    
    # Extract skills
    skills = candidate.get('skills', [])
    
    # Extract education
    education = candidate.get('education', [])
    
    # Build the mapped record
    csv_record = {
        'batch_id': batch_id,
        'item': record.get('item', ''),
        'status': record.get('status', ''),
        'first_name': first_name,
        'last_name': last_name,
        'full_name': full_name,
        'job_title': job_title,
        'company': company,
        'location': location,
        'country': country,
        'city': city,
        'industry': industry,
        'seniority': '',  # Not directly available in SignalHire data
        'department': '',  # Not directly available in SignalHire data
        'skills': ', '.join(skills) if skills else '',
        'education': str(education) if education else '',
        'experience': str(experience) if experience else '',
        'linkedin': record.get('item', ''),
        'received_at': timestamp.isoformat(),
        'emails': ', '.join(emails) if emails else '',
        'phones': ', '.join(phones) if phones else ''
    }
    
    return csv_record

# Field mapping reference for SignalHire API response structure
SIGNALHIRE_FIELD_MAPPING = {
    # Top-level fields
    'status': 'status',
    'item': 'item',
    
    # Candidate object fields
    'candidate.fullName': 'full_name',
    'candidate.headLine': 'job_title',
    'candidate.locations[0].name': 'location',
    'candidate.skills': 'skills (array)',
    'candidate.education': 'education (array)',
    'candidate.experience': 'experience (array)',
    'candidate.experience[0].position': 'job_title',
    'candidate.experience[0].company': 'company',
    'candidate.experience[0].industry': 'industry',
    'candidate.contacts[type=email].value': 'emails (array)',
    'candidate.contacts[type=phone].value': 'phones (array)',
    'candidate.social': 'social (array)',
    'candidate.summary': 'summary',
    'candidate.language': 'languages (array)',
    'candidate.organization': 'organizations (array)',
    'candidate.course': 'courses (array)',
    'candidate.project': 'projects (array)',
    'candidate.certification': 'certifications (array)',
    'candidate.patent': 'patents (array)',
    'candidate.publication': 'publications (array)',
    'candidate.honorAward': 'awards (array)'
}

# Example SignalHire webhook payload structure
EXAMPLE_WEBHOOK_PAYLOAD = {
    "status": "success",
    "item": "https://www.linkedin.com/in/john-doe-12345678",
    "candidate": {
        "uid": "abc123def456gh789ijk012lmn345op6",
        "fullName": "John Doe",
        "gender": None,
        "photo": {
            "url": "https://media.cdn.com/image/C4A03AQH-wVPhNTP7cw/0/1577297087305"
        },
        "locations": [
            {"name": "New York, New York, United States"}
        ],
        "skills": [
            "Civil Litigation", "Corporate Law", "Litigation", 
            "Mediation", "Negotiation", "Legal Research", "Legal Writing"
        ],
        "education": [
            {
                "faculty": "Law",
                "university": "New York University School of Law",
                "url": "https://www.linkedin.com/school/new-york-university-school-of-law/",
                "startedYear": 2005,
                "endedYear": 2008,
                "degree": ["JD"]
            }
        ],
        "experience": [
            {
                "position": "Owner / Managing Attorney",
                "location": None,
                "current": True,
                "started": "2015-01-01T00:00:00+00:00",
                "ended": None,
                "company": "Doe Law Offices LLC",
                "summary": "Helping clients navigate family law and criminal defense cases.",
                "companyUrl": "https://www.linkedin.com/company/doe-law-offices",
                "companyId": None,
                "companySize": "1-10",
                "staffCount": 5,
                "industry": "Law Practice",
                "website": "http://www.doe-law.com"
            }
        ],
        "headLine": "Founder / Owner Doe Law Offices",
        "summary": "John Doe, an experienced lawyer with expertise in family law and criminal defense.",
        "language": [
            {"name": "English", "proficiency": "Native or bilingual"},
            {"name": "Portuguese", "proficiency": "Professional working"}
        ],
        "organization": [
            {
                "name": "New York Bar Association",
                "position": None,
                "startDate": "January 2015",
                "endDate": None
            }
        ],
        "contacts": [
            {
                "type": "phone",
                "value": "+1 555-123-4567",
                "rating": "100",
                "subType": "work_phone",
                "info": "This phone number also belongs to https://www.linkedin.com/in/jane-doe"
            },
            {
                "type": "email",
                "value": "john.doe@doelaw.com",
                "rating": "100",
                "subType": "work"
            },
            {
                "type": "email",
                "value": "john.doe@gmail.com",
                "rating": "100",
                "subType": "personal"
            }
        ],
        "social": [
            {
                "type": "li",
                "link": "https://www.linkedin.com/in/john-doe-12345678",
                "rating": "100"
            },
            {
                "type": "fb",
                "link": "https://www.facebook.com/johndoe",
                "rating": "100"
            }
        ]
    }
}
