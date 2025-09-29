import frappe
import requests
from frappe.utils import now, get_datetime

@frappe.whitelist()
def sync_log_data():
    """
    Incremental sync from remote sites into local doctypes.
    Uses 'modified' field for filtering.
    """

    la_settings = frappe.get_doc("LA Settings")
    sites = la_settings.sites
    table_settings = la_settings.table_settings
    for site in sites:
        remote_site = site.site_url
        api_key = site.api_key
        api_secret = site.api_secret

        for table_setting in table_settings:
            last_sync = table_setting.last_sync_date
            url=f"{remote_site}/api/resource/{table_setting.source_table}"
            params = {
                "fields": '["name", "creation", "modified", "route", "user"]',
                "filters": f'[["modified", ">", "{last_sync}"]]'
            }
            headers={
                "Authorization": f"token {api_key}:{api_secret}"
                }
            response = requests.get(url=url, headers=headers, params=params)
            record_list = response.json()["data"]
            max_modified = last_sync
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

                if get_datetime(record["modified"]) > get_datetime(max_modified):
                    max_modified = record["modified"]
            # update last_sync_date
            table_setting.db_set("last_sync_date", max_modified)
    return {"status": "success", "synced_on": now()}

  