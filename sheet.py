import os

from oauth2client import client, tools
from oauth2client.file import Storage

import settings

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def post_listing_to_sheet(sheet, listing):
    """
    """
    price = 0
    try:
        price = int(float(listing['price'].replace("$", "")))
    except Exception:
        pass
    bedrooms = int(listing['bedrooms'])
    price_per_bedroom = price // bedrooms
    body = {
        'values': [[listing['price'],
                    listing['bedrooms'],
                    price_per_bedroom,
                    listing['where'],
                    listing['name'],
                    listing['url']]]
    }
    sheet.spreadsheets().values().append(spreadsheetId=settings.SHEET_ID,
                                         range='A1:A1',
                                         body=body,
                                         valueInputOption='USER_ENTERED').execute()

def post_listings_to_sheet(sheet, listings):
    """
    """
    values = []
    for listing in listings:
        price = 0
        try:
            price = int(float(listing['price'].replace("$", "")))
        except Exception:
            pass
        bedrooms = int(listing['bedrooms'])
        price_per_bedroom = price // bedrooms
        values.append([listing['price'],
                       listing['bedrooms'],
                       price_per_bedroom,
                       listing['where'],
                       listing['name'],
                       listing['url']])

    body = {'values': values}
    sheet.spreadsheets().values().append(spreadsheetId=settings.SHEET_ID,
                                         range='Database!A1:A1',
                                         body=body,
                                         valueInputOption='USER_ENTERED').execute()
    sheet.spreadsheets().values().append(spreadsheetId=settings.SHEET_ID,
                                         range='A1:A1',
                                         body=body,
                                         valueInputOption='USER_ENTERED').execute()
