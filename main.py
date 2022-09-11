from dealcatcher_db import dealcatcher_db  # Shared SQLite DB with DealCatcher
from config_db import config_db  # Persistent config settings in SQLite
from heartbeat import heartbeat_daemon  # Basic Uptime Kuma heartbeat
from threading import Thread
import sheet_setup
from time import sleep

deal_sheet = sheet_setup.open_worksheet()

# Start the heartbeat
heartbeat = Thread(target=heartbeat_daemon, args=(295, True,), daemon=True)
heartbeat.start()

# Open up the worksheet
_vendors = dealcatcher_db.get_vendors()  # Tuple to unpack and load into dictionary

if False:
    sheet_setup.start(deal_sheet, _vendors)

while True:
    for vendor_acronym, vendor_name, vendor_website, vendor_thumbnail in _vendors:
        from_dealcatcher = dealcatcher_db.get_deals(vendor_acronym, raw=True)
        sheet = config_db.get_worksheet(deal_sheet, vendor_name)
        vendor_sheet = config_db.get_worksheet(deal_sheet, vendor_name).get_all_values()

        new_deals_list = list()  # New deals as compared between last known RastaBot deals and most recent known DealCatcher deals
        expired_deals_list = list()  # Same thing as above but different
        _expired_double_check = list()  # Used to only remove expired deals if they have been expired for 2 checks
        current_deals_matrix = list()

        for dc_vendor, dc_name, dc_price, dc_image in from_dealcatcher:
            new_deal = True
            for sheet_vendor, sheet_name, sheet_price, sheet_image in vendor_sheet:
                if 'Vendor' in sheet_vendor:  # Skip the title row
                    continue
                if dc_name == sheet_name:  # Next sheet_name if found
                    new_deal = False
                    continue

            if new_deal:
                name, url, image_url = dealcatcher_db.get_urls(dc_name)
                product_name = f"=HYPERLINK(\"{url}\", \"{name}\")" if not raw else name
                picture = f"=IMAGE(\"{dc_image}\", 1)" if not raw else ''
                next_row = len(sheet.col_values(1)) + 1
                config_db.new_deal(sheet, [vendor_acronym, product_name, dc_price, picture])
                print(f'New deal for {vendor_name}:{dc_name}')

        sleep(10)

        for sheet_vendor, sheet_name, sheet_price, sheet_image in vendor_sheet:
            if 'Vendor' in sheet_vendor:  # Skip the title row
                continue
            expired_deal = True
            for dc_vendor, dc_name, dc_price, dc_image in from_dealcatcher:
                if dc_name == sheet_name:  # Next sheet_name if found
                    expired_deal = False
                    continue

            if expired_deal:
                if sheet_name in _expired_double_check:
                    config_db.remove_deal(sheet, sheet_name)
                    print(f'Expired deal  for {vendor_name}:{sheet_name}')
                else:
                    _expired_double_check.append(sheet_name)
                    print(f'Expired deal cooldown  for {vendor_name}:{sheet_name}')
    sleep(60)
