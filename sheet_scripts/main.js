/*
This Google Apps Script (GAS) functions are used to standardize the creation of the Google Sheet submit button
and upload the data from the sheet to be processed and cleaned before being uploaded to Elasticsearch.
*/

function insertButton() {
  // This function sets up the Google Sheet button and assigns the script to the button.
  var image_url = ""; // Use png of submit button from MyDS
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetById(0);
  var buttonImage = sheet.insertImage(image_url, 16, 1, 0, 0);
  buttonImage.assignScript('uploadData');
}

function uploadData() {
  // This function will be assigned to the submit button in the respective directory google sheets
    var sheet_id = SpreadsheetApp.getActiveSpreadsheet().getSheetId();
    var userEmail = Session.getActiveUser().getEmail();
    var URL_STRING = ["" /* Inject endpoint url later */, sheet_id, "&user_name=", userEmail].join("");
  
    var response = UrlFetchApp.fetch(URL_STRING);
    var json = response.getContentText();
    var data = JSON.parse(json);
}