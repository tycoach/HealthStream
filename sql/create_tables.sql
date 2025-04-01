-- Dimension Tables

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL,
    week_number INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_patient (
    patient_key SERIAL PRIMARY KEY,
    patient_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    gender VARCHAR(20),
    date_of_birth DATE,
    blood_type VARCHAR(5),
    current_age INT,
    email VARCHAR(255),
    phone_number VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_hospital (
    hospital_key SERIAL PRIMARY KEY,
    hospital_id VARCHAR(50) NOT NULL,
    hospital_name VARCHAR(255) NOT NULL,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_physician (
    physician_key SERIAL PRIMARY KEY,
    physician_id VARCHAR(50) NOT NULL,
    physician_name VARCHAR(255) NOT NULL,
    specialty VARCHAR(100),
    hospital_key INT REFERENCES dim_hospital(hospital_key),
    is_active BOOLEAN DEFAULT TRUE,
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_diagnosis (
    diagnosis_key SERIAL PRIMARY KEY,
    diagnosis_code VARCHAR(50) NOT NULL,
    diagnosis_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact Tables

CREATE TABLE fact_health_records (
    record_key SERIAL PRIMARY KEY,
    patient_key INT REFERENCES dim_patient(patient_key),
    hospital_key INT REFERENCES dim_hospital(hospital_key),
    physician_key INT REFERENCES dim_physician(physician_key),
    diagnosis_key INT REFERENCES dim_diagnosis(diagnosis_key),
    admission_date_key INT REFERENCES dim_date(date_key),
    discharge_date_key INT REFERENCES dim_date(date_key),
    length_of_stay INT,
    total_cost DECIMAL(10,2),
    medication_count INT,
    procedure_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fact_daily_patient_metrics (
    metric_key SERIAL PRIMARY KEY,
    patient_key INT REFERENCES dim_patient(patient_key),
    date_key INT REFERENCES dim_date(date_key),
    height_cm DECIMAL(5,2),
    weight_kg DECIMAL(5,2),
    bmi DECIMAL(4,2),
    blood_pressure_systolic INT,
    blood_pressure_diastolic INT,
    heart_rate INT,
    temperature DECIMAL(4,1),
    oxygen_saturation INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better query performance
CREATE INDEX idx_fact_health_admission ON fact_health_records(admission_date_key);
CREATE INDEX idx_fact_health_patient ON fact_health_records(patient_key);
CREATE INDEX idx_fact_daily_date ON fact_daily_patient_metrics(date_key);
CREATE INDEX idx_fact_daily_patient ON fact_daily_patient_metrics(patient_key);