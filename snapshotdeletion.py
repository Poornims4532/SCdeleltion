import logging
import json
import os
from datetime import datetime
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
import azure.functions as func

# Initialize Azure Compute Management Client with DefaultAzureCredential
credential = DefaultAzureCredential()
compute_client = ComputeManagementClient(credential, os.environ["AZURE_SUBSCRIPTION_ID"])

DEFAULT_DAYS_OLD = 7

def fetch_snapshots(client, resource_group=None):
    snapshots = []
    if resource_group:
        snapshot_list = client.snapshots.list_by_resource_group(resource_group)
    else:
        snapshot_list = client.snapshots.list()
    for snapshot in snapshot_list:
        snapshots.append({
            "name": snapshot.name,
            "id": snapshot.id,
            "time_created": snapshot.time_created,
            "disk_type": "OS" if snapshot.os_type else "Data",
            "resource_group": snapshot.resource_group
        })
    return snapshots

def filter_snapshots(snapshots, days_old_os, days_old_data_disk, exclude_list):
    current_time = datetime.utcnow()
    filtered_snapshots = []

    exclude_all_os = "All_OS" in exclude_list
    exclude_all_data = "All_Data" in exclude_list

    for snapshot in snapshots:
        snapshot_age = (current_time - snapshot["time_created"].replace(tzinfo=None)).days

        if (snapshot["disk_type"] == "OS" and snapshot_age >= days_old_os) or \
           (snapshot["disk_type"] == "Data" and snapshot_age >= days_old_data_disk):
            
            if exclude_all_os and snapshot["disk_type"] == "OS":
                continue

            if exclude_all_data and snapshot["disk_type"] == "Data":
                continue
            
            if snapshot["id"] not in exclude_list:
                filtered_snapshots.append(snapshot)
    
    return filtered_snapshots

def export_to_excel(filtered_snapshots, file_name="filtered_snapshots.xlsx"):
    df = pd.DataFrame(filtered_snapshots)
    df.to_excel(file_name, index=False)
    return file_name

def delete_snapshots(client, snapshots):
    for snapshot in snapshots:
        logging.info(f"Deleting snapshot: {snapshot['name']} ({snapshot['id']})")
        client.snapshots.begin_delete(snapshot["resource_group"], snapshot["name"])

# The main function that handles the HTTP request
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing HTTP request to manage Azure snapshots.')

    try:
        # Parse the request body for user input (if sent as JSON)
        req_body = req.get_json()

        # Get user inputs from the HTTP request body or query parameters
        days_old_os = req_body.get('days_old_os', DEFAULT_DAYS_OLD)
        days_old_data_disk = req_body.get('days_old_data_disk', DEFAULT_DAYS_OLD)
        resource_group_name = req_body.get('resource_group_name', None)
        exclude_snapshots = req_body.get('exclude_snapshots', '').split(',')

        # Fetch snapshots
        snapshots = fetch_snapshots(compute_client, resource_group_name)

        # Filter snapshots
        filtered_snapshots = filter_snapshots(snapshots, days_old_os, days_old_data_disk, exclude_snapshots)

        # Export to Excel
        excel_file = export_to_excel(filtered_snapshots)

        # Check if the user wants to delete the filtered snapshots
        delete_confirm = req_body.get('delete_confirm', False)
        if delete_confirm:
            delete_snapshots(compute_client, filtered_snapshots)
            export_to_excel(filtered_snapshots, file_name="deleted_snapshots.xlsx")

        # Return the filtered Excel file as a response
        with open(excel_file, 'rb') as f:
            file_content = f.read()
        return func.HttpResponse(body=file_content, status_code=200, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": f"attachment; filename={excel_file}"})

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=400)
