from unicef_api.schema_sync import sync_dataflow_schemas

if __name__ == "__main__":
    print("Starting schema sync with hybrid sampling...")
    sync_dataflow_schemas()
    print("Schema sync complete.")
