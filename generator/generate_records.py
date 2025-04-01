import os
import pandas as pd
from faker import Faker   
from tqdm import tqdm
from schema import get_health_record_schema
from faker_utils import generate_record

# Initialize Faker
fake = Faker()

# Configuration settings
config = {
    "rows": 100000,  # Total number of rows to generate
    "batch_size": 1000,  # Number of rows per batch
    "output_format": "csv",
    "output_path": "output/health_recordsss.csv"  # Output file path
}



# Generate data in batches using schema
def generate_data(config):
    rows = config["rows"]
    batch_size = config["batch_size"]
    output_format = config["output_format"]
    output_path = config["output_path"]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    schema = get_health_record_schema()
    total_batches = rows // batch_size
    remainder = rows % batch_size

    with open(output_path, "w", encoding="utf-8") as f:
        header_written = False

        for _ in tqdm(range(total_batches), desc="Generating Batches"):
            batch = [generate_record(fake, schema) for _ in range(batch_size)]
            #df = pd.DataFrame(batch, columns=schema.keys())
            df = pd.DataFrame(batch)
            df.to_csv(f, index=False, header=not header_written, mode="a")
            header_written = True

        if remainder > 0:
            batch = [generate_record(fake, schema) for _ in range(remainder)]
            #df = pd.DataFrame(batch, columns=schema.keys())
            df = pd.DataFrame(batch)
            df.to_csv(f, index=False, header=not header_written, mode="a")

    print(f"Data generation complete: {output_path}")


if __name__ == "__main__":
    #config = load_config()
    generate_data(config)


