from gspread.exceptions import WorksheetNotFound
from dealcatcher_db import dealcatcher_db
from config_db import config_db
# Google Sheet interaction and credentials
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def open_worksheet():
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    sheets_creds_file = 'creds.json'
    deal_sheet_key = '1keivYZtyFbinx2WP5j3leI08Ic_Uwk36-zi399u9d9M'
    creds = ServiceAccountCredentials.from_json_keyfile_name(f'{config_db.environ_path}{sheets_creds_file}', scope)
    with config_db.rate_limiter:
        client = gspread.authorize(creds)
        return client.open_by_key(deal_sheet_key)


def start(deal_sheet, _vendors):
    # On startup, clear the contents of all vendor sheets and place titles
    vendor_str = 'Vendor'
    product_str = 'Product Name'
    price_str = 'Price ($)'
    thumbnail_str = 'Thumbnail'

    for vendor_acronym, vendor_name, vendor_website, vendor_thumbnail in _vendors:
        # Load or create and load (for new vendors) a worksheet
        try:
            sheet = deal_sheet.worksheet(vendor_name)
            print(f'Loaded worksheet for {vendor_name}')
            sheet.clear()
            sheet.update('A1:D1', [[vendor_str, product_str, price_str, thumbnail_str]])
        except WorksheetNotFound:
            deal_sheet.add_worksheet(title=vendor_name, rows=150, cols=4)
            sheet = deal_sheet.worksheet(vendor_name)
            print(f'Added worksheet for {vendor_name}')
            sheet.clear()
            sheet.update('A1:D1', [[vendor_str, product_str, price_str, thumbnail_str]])

    # Get each vendor's deals and update their pages initially
    for vendor_acronym, vendor_name, vendor_website, vendor_thumbnail in _vendors:
        sheet = deal_sheet.worksheet(vendor_name)
        vendor_matrix = dealcatcher_db.get_deals(vendor_acronym)
        for row in vendor_matrix:
            print(row)
        # Update each vendor
        sheet.update(f'A2:D{len(vendor_matrix) + 1}', vendor_matrix, value_input_option='USER_ENTERED')
        print(f'Placed {len(vendor_matrix)} records for {vendor_name}')
