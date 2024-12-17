/**
 * This Google Apps Script (GAS) functions are used to standardize the creation of the Google Sheet submit button
 * and upload the data from the sheet to be processed and cleaned before being uploaded to Elasticsearch.
 */

/**
 * Get a secret from Secret Manager
 * @param {String} secret_name
 * @param {String} version_number
 * @return {String} secret_key
 */
function getSecret_(secret_name) {
  let token, endpoint, response;
  endpoint = ["https://secretmanager.googleapis.com/v1/projects/326125806705/secrets/", secret_name, "/versions/latest/:access"].join();
  token = ScriptApp.getOAuthToken();
  response = UrlFetchApp.fetch(endpoint, {
    headers: {
      Authorization: 'Bearer ' + token,
      Accept: 'application/json',
    }
  });
  var decoded_secret = Utilities.base64Decode(JSON.parse(response.getContentText())['payload']['data']);
  var secret_key = Utilities.newBlob(decoded_secret).getDataAsString()
  return secret_key;
}

/**
 * Create `Submit` button in active spreadsheet and assign the `uploadData` function to it
 * @param {String} spreadsheet_id
 * @param {Number} sheet_id
 * @return {ImageButton} button_image
 */
function insertButton(spreadsheet_id, sheet_id) {
  var image_url = getSecret("button-image"); // Use png of submit button from MyDS
  var sheet = SpreadsheetApp.openById(spreadsheet_id).getSheetById(sheet_id);
  var button_image = sheet.insertImage(image_url, 16, 1, 0, 0);

  return button_image
}

/**
 * Uploads data to endpoint for processing
 * @param {String} spreadsheet_id
 * @param {String} user_email
 */
function uploadData(spreadsheet_id, user_email) {
    var sheet_id = SpreadsheetApp.openById(spreadsheet_id).getSheetId();
    var userEmail = Session.getActiveUser().getEmail();
    var URL_STRING = [getSecret("directory-api-url"), spreadsheet_id, "&user_name=", user_email].join("");
  
    var response = UrlFetchApp.fetch(URL_STRING);
    var json = response.getContentText();
    var data = JSON.parse(json);
}