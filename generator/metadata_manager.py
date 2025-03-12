# Handles metadata creation & upload

import io
import pandas as pd


def create_metadata(run_id, timestamp, total_rows, batch_size,total_batches, schema_length):
    metadata = {
        "run_id": run_id,
        "timestamp": timestamp,
        "total_rows": total_rows,
        "batch_size": batch_size,
        "total_batches": total_batches,
        "schema_fields": schema_length
    }
    buffer = io.StringIO()
    pd.DataFrame([metadata]).to_json(buffer, orient='records')
    buffer.seek(0)
    return buffer
