import config_db  # Persistent config settings in SQLite
from dealcatcher_db import dealcatcher_db  # Shared SQLite DB with DealCatcher

from heartbeat import heartbeat_daemon  # Basic Uptime Kuma heartbeat
from threading import Thread

import pygsheets  # Google Sheets integration

# Start the heartbeat
heartbeat = Thread(target=heartbeat_daemon, args=(295, True,), daemon=True)
heartbeat.start()

# Open up the worksheet
sheet_client = pygsheets.authorize(service_file=f'{config_db.path}irie-genetics-seed-list-afac9fc804ef.json')
seed_list = sheet_client.open('Irie Genetics - Seed List')
active_sheet = seed_list.worksheet('title', 'Active Deals')  # This sheet should have all active deals

vendors = dict()  # Dictionary of vendor information in vendors[vendor_acronym]
_vendors = dealcatcher_db.get_vendors()  # Tuple to unpack and load into dictionary
worksheets = dict()  # Dictionary of every vendor worksheet in worksheets[vendor_acronym]

for vendor_acronym, vendor_name, vendor_website, vendor_thumbnail in _vendors:
    vendors[vendor_acronym] = (vendor_name, vendor_website, vendor_thumbnail)

    # Load or create and load (for new vendors) a worksheet
    try:
        worksheets[vendor_acronym] = seed_list.worksheet('title', vendor_name)
        print(f'Loaded worksheet for {vendor_name}')
    except pygsheets.exceptions.WorksheetNotFound:
        seed_list.add_worksheet(vendor_name, 50)
        worksheets[vendor_acronym] = seed_list.worksheet('title', vendor_name)
        print(f'Added worksheet for {vendor_name}')

# On startup, clear the contents of all vendor sheets and place titles
vendor_str = 'Vendor'
product_str = 'Product Name'
price_str = 'Price ($)'
thumbnail_str = 'Thumbnail'

for acronym in worksheets:
    worksheets[acronym].resize(rows=1)
    worksheets[acronym].update_row(1, [[vendor_str, product_str, price_str, thumbnail_str]])
    worksheets[acronym].resize(rows=150)

# Get each vendor's deals and update their pages initially
for acronym in vendors:
    #print(acronym)
    vendor_name, vendor_website, vendor_thumbnail = vendors[acronym]
    #print(vendor_name, vendor_website, vendor_thumbnail)
    vendor_matrix = dealcatcher_db.get_deals(acronym)

    # Update each deal
    for row in vendor_matrix:
        worksheets[acronym].append_table(row, start = 'A2')

# for acronym in worksheets:
#     print(f'{acronym} is {len('1')} big')
#     try:
#         worksheets[acronym].add_rows(len(deals[acronym]))
#         worksheets[acronym].update_values('A2', values=deals[acronym])
#     except pygsheets.exceptions.InvalidArgumentValue:
#         pass


while True:
    pass


