import logging
import os
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from google_sheets.google_sheets_utils import GoogleSheetManager

logger = logging.getLogger(__name__)
logging.basicConfig(filename="gsheet_api.log", filemode="a", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

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
    
    def setup_editsheet(self) -> bool:
        """
        Sets up edits worksheet to keep track of edits.
        `Edit Logs` worksheet contains: user email, sheet SHA256 checksum, datetime of last updated, and sheet row count
        """
        sheet = self.connect_to_sheet()
        if "Edit Logs" not in [wsht.title for wsht in sheet.worksheets()]:
            sheet.add_worksheet(title="Edit Logs", rows=1000, cols=10)
            logging.info(f"Successfully created `Edit Logs` sheet for worksheet: {self.sheet_id}")

            edit_sheet = sheet.worksheet("Edit Logs")

            # Appending headers and worksheet metadata
            edit_sheet.append_row(["user", "checksum", "last_updated", "row_count"])
            edit_sheet.update_acell("D2", "=COUNTA(A2:A)") # Keeps track of row count to avoid loading the entire edit log
            logging.info(f"Populated worksheet headers and rowcount metadata.")
            
            # Protecting sheets from edits
            ADMIN_EMAILS = os.getenv("ADMIN_EMAILS")
            request_body = {
                "requests": [
                    {
                        "addProtectedRange": {
                            "protectedRange": {
                                "range": {
                                    "sheetId": self.sheet_id
                                },
                                "editors": {
                                    "domainUsersCanEdit": False,
                                    "users": ADMIN_EMAILS
                                },
                                "warningOnly": False
                            }
                        }
                    },
                    {
                        'updateSheetProperties': {
                            'properties': {
                                'sheetId': self.sheet_id,
                                'hidden': True
                            },
                            'fields': 'hidden'
                        }
                    }
                ]
            }

            sheet.batch_update(request_body)
            logging.info(f"Protected sheet from edits")
            return True

        else:
            logging.info(f"`Edit Logs` sheet already exists for worksheet: {self.sheet_id}")
            return False