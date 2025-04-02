# HealthStream

HealthStream is a data engineering project designed to handle data generation, processing, and streaming workflows using tools like Airflow, Spark, Kafka, and MinIO.

## Table of Contents

- [Project Overview](#project-overview)
- [Features](#features)
- [File Structure](#file-structure)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Project Overview

This project provides a robust pipeline for generating and processing data. It integrates multiple technologies to simulate real-world data engineering workflows.

## Features

- Data generation using Python and Faker.
- Workflow orchestration with Apache Airflow.
- Distributed data processing with Apache Spark.
- Object storage with MinIO.

## File Structure

The project is organized as follows:

## Setup Instructions

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd HealthStream
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables:

   Copy `.env.example` to `.env` and update the values as needed.

4. Start services using Docker Compose:
   ```bash
   docker-compose up
   ```

## Usage

Data Generation: Use the scripts in the `generator/` directory to generate synthetic data.

Airflow Workflows: Define and manage workflows in the `airflow/dags/` directory.

Spark Processing: Use the `spark/` directory for distributed data processing.


MinIO Storage: Store and retrieve data using MinIO

## Architecture

The architecture of the HealthStream project is as follows:

```
+-------------------+       +-------------------+       +-------------------+
|                   |       |                   |       |                   |
|   Data Generator  +------>+   Object Storage  +------>+   Data Processing |
|  (Python + Faker) |       |      (MinIO)      |       |  (Apache Spark)   |
|                   |       |                   |       |                   |
+-------------------+       +-------------------+       +-------------------+
        |                                                       |
        |                                                       |
        v                                                       v
+-------------------+                                   +-------------------+
|                   |                                   |                   |
|  Workflow Manager |                                   |   Output Storage  |
|   (Apache Airflow)|                                   |      (MinIO)      |
|                   |                                   |                   |
+-------------------+                                   +-------------------+
```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Commit your changes and push to your fork.
4. Submit a pull request.

## License

This project is licensed under the MIT License.

