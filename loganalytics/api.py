import frappe
import requests
from frappe.utils import now, get_datetime

@frappe.whitelist()
def sync_log_data():

    la_settings = frappe.get_doc("LA Settings")
    sites = la_settings.sites
    table_settings = la_settings.table_settings

    if len(sites) == 0:
        raise Exception("Missing site(s) in LA Settings")
    if len(table_settings) == 0:
        raise Exception("Missing table(s) in LA Settings")
    
    for site in sites:
        remote_site = site.site_url
        api_key = site.api_key
        api_secret = site.api_secret

        for table_setting in table_settings:
            records_synced_till = table_setting.records_synced_till
            url=f"{remote_site}/api/resource/{table_setting.source_table}"
            params = {
                "fields": '["name", "creation", "modified", "route", "user"]',
                "filters": f'[["modified", ">", "{records_synced_till}"]]'
            }
            headers={
                "Authorization": f"token {api_key}:{api_secret}"
                }
            response = requests.get(url=url, headers=headers, params=params)
            record_list = response.json()["data"]
            max_modified = records_synced_till
            records_synced_in_last_run = 0
            for record in record_list:
                frappe.get_doc({
                    "doctype": table_setting.target_table,
                    "site": remote_site, 
                    "id": record["name"],
                    "created_on": record["creation"],
                    "modified_on": record["modified"],
                    "route": record["route"],
                    "user": record["user"],
                }).insert(ignore_if_duplicate=True)
                records_synced_in_last_run = records_synced_in_last_run+1

                if get_datetime(record["modified"]) > get_datetime(max_modified):
                    max_modified = record["modified"]

            table_setting.db_set("records_synced_till", max_modified)
            table_setting.db_set("records_synced_in_last_run", records_synced_in_last_run)
            table_setting.db_set("last_sync_job_run_date", now())

    return {"status": "success", "last_sync_job_run_date": now()}

  