import pandas as pd
from faker import Faker
from .faker_utils import generate_record
from .schema import get_health_record_schema
import io



def generate_data_batch(fake, batch_size):
    """Generate a single batch of synthetic data using a consistent schema."""
    schema = get_health_record_schema()  # Only called once per batch
    batch = [generate_record(fake, schema) for _ in range(batch_size)]
    buffer = io.StringIO()
    df = pd.DataFrame(batch, columns=[field['name'] for field in schema])  # Ensure correct column mapping
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    return buffer


def generate_batches(fake, total_rows, batch_size):
    """Yield data in batches, avoiding redundant schema calls."""
    schema = get_health_record_schema()  # Called once for all batches
    total_batches = total_rows // batch_size
    remainder = total_rows % batch_size

    for _ in range(total_batches):
        yield generate_data_batch(fake, batch_size)

    if remainder > 0:
        yield generate_data_batch(fake, remainder)