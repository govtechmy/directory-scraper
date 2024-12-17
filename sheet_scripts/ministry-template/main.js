function uploadData() {
    var sheet_id = SpreadsheetApp.getActiveSpreadsheet().getSheetId();
    var user_email = Session.getActiveUser().getEmail();
    DirectoryGovMain.uploadData(sheet_id, user_email)
  }
  
  function insertButton() {
    var spreadsheet_id = SpreadsheetApp.getActiveSpreadsheet().getSheetId();
    var sheet_id = 0;
    button_image = DirectoryGovMain.insertButton(spreadsheet_id, sheet_id)
    button_image.assignScript('uploadData');
  }