import gspread
from google.oauth2.service_account import Credentials
from google_sheets.google_sheets_utils import GoogleSheetManager

class GoogleSheetManager(GoogleSheetManager):

    # Overwrite connect_to_sheet function to return the raw worksheet instead of the first sheet
    def connect_to_sheet(self):
        """
        Connects to Google Sheets using service account credentials.
        """
        creds = Credentials.from_service_account_file(self.creds_file, scopes=self.scopes)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.sheet_id)
        return sheet
    
    def setup_editsheet(self):
        """
        Sets up edits worksheet to keep track of edits.
        """
        sheet = self.connect_to_sheet()
        if "Edits" not in [wsht.title for wsht in sheet.worksheets()]:
            sheet.add_worksheet(title="Edits", rows=1000, cols=10)
            sheet.worksheet("Edits").append_row(["username", "checksum", "last_updated"])
            return True
        return False