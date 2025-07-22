import os
import uuid
import json
import logging
from datetime import datetime
import functions_framework
from flask import jsonify, make_response
from google.cloud import bigquery

# Set your BigQuery dataset and table name
PROJECT_ID = "northern-cooler-464505-t9"
DATASET_ID = "Sales_data"
TABLE_ID = "sales_data"

# Logging setup
logger = logging.getLogger("HTTP Handler")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s : %(message)s'))
if not logger.handlers:
    logger.addHandler(handler)

# Cloud Function entry point
@functions_framework.http
def sales_data(request):
    if request.method != 'POST':
        logger.info("GET request not supported.")
        return make_response(jsonify({'error': "Method not allowed"}), 405)

    data = request.get_json(silent=True)
    if not data:
        logger.info("No data received.")
        return make_response(jsonify({"error": "No data present"}), 400)

    required_fields = ["transaction_id", "customer_id", "items", "total_amount", "payment_method"]
    for field in required_fields:
        if field not in data:
            logger.info(f"Missing field: {field}")
            return make_response(jsonify({"error": f"Missing field: {field}"}), 400)

    if not isinstance(data["items"], list):
        logger.info("Invalid type for items.")
        return make_response(jsonify({"error": "Items must be a list"}), 400)

    order_id = str(uuid.uuid4())
    processed_at = datetime.utcnow().isoformat()
    total_tax = round(0.07 * data["total_amount"], 2)

    row = {
        "order_id": order_id,
        "transaction_id": data["transaction_id"],
        "timestamp": data.get("timestamp", datetime.utcnow().isoformat()),
        "customer_id": data["customer_id"],
        "items": json.dumps(data["items"]),  # Store items list as JSON string
        "total_amount": data["total_amount"],
        "total_tax": total_tax,
        "payment_method": data["payment_method"],
        "processed_at": processed_at,
        "status": "success",
        "message": "Transaction processed and stored successfully"
    }

    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        # Define the schema
        schema = [
            bigquery.SchemaField("order_id", "STRING"),
            bigquery.SchemaField("transaction_id", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("items", "STRING"),
            bigquery.SchemaField("total_amount", "FLOAT"),
            bigquery.SchemaField("total_tax", "FLOAT"),
            bigquery.SchemaField("payment_method", "STRING"),
            bigquery.SchemaField("processed_at", "TIMESTAMP"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("message", "STRING"),
        ]

        # Create table if not exists
        try:
            client.get_table(table_ref)  # Raises NotFound if table doesn't exist
        except Exception:
            logger.info(f"Table {table_ref} not found. Creating it...")
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            logger.info("Table created.")

        # Insert row
        errors = client.insert_rows_json(table_ref, [row])
        if errors:
            logger.error(f"Insert errors: {errors}")
            return make_response(jsonify({"error": "Failed to insert row", "details": errors}), 500)

        return make_response(jsonify({"success": True, "order_id": order_id}), 200)

    except Exception as e:
        logger.error(f"Exception: {e}")
        return make_response(jsonify({"error": "Internal error", "details": str(e)}), 500)

# Launcher block for Cloud Run buildpack deployment
if __name__ == "__main__":
    from functions_framework import create_app
    app = create_app(target='sales_data')
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
