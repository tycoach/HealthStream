def get_health_record_schema():
    """
    Returns a list of dictionaries representing the schema for synthetic health records.
    Each dictionary contains the column name and the expected data type.
    """
    schema = [
        {"name": "patient_id", "type": "string"},
        {"name": "first_name", "type": "string"},
        {"name": "last_name", "type": "string"},
        {"name": "gender", "type": "string"},
        {"name": "date_of_birth", "type": "date"},
        {"name": "age", "type": "integer"},
        {"name": "email", "type": "string"},
        {"name": "phone_number", "type": "string"},
        {"name": "address", "type": "string"},
        {"name": "city", "type": "string"},
        {"name": "state", "type": "string"},
        {"name": "zip_code", "type": "string"},
        {"name": "country", "type": "string"},
        {"name": "insurance_provider", "type": "string"},
        {"name": "policy_number", "type": "string"},
        {"name": "primary_physician", "type": "string"},
        {"name": "hospital_id", "type": "string"},
        {"name": "hospital_name", "type": "string"},
        {"name": "admission_date", "type": "date"},
        {"name": "discharge_date", "type": "date"},
        {"name": "diagnosis", "type": "string"},
        {"name": "procedure", "type": "string"},
        {"name": "medications", "type": "string"},
        {"name": "allergies", "type": "string"},
        {"name": "blood_type", "type": "string"},
        {"name": "height_cm", "type": "float"},
        {"name": "weight_kg", "type": "float"},
        {"name": "bmi", "type": "float"},
        {"name": "emergency_contact_name", "type": "string"},
        {"name": "emergency_contact_phone", "type": "string"},
        {"name": "created_at", "type": "datetime"},
        {"name": "updated_at", "type": "datetime"}
    ]
    return schema
