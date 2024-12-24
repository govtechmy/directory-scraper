/**
 * Trigger function to set the default value for the position_sort column
 */
function posSort(e) {
  const sheet = e.source.getActiveSheet();
  if (sheet.getName() != "LookupSheet") {
    if(e.changeType === 'INSERT_ROW') {
      var row = sheet.getActiveRange().getRow();
      var orgSortFormula = "=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),LookupSheet!$D$2,LookupSheet!$B$2, 'Value not found')";
      var orgIdFormula = "=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),LookupSheet!$D$2,LookupSheet!$A$2, 'Value not found)')";
      var orgTypeFormula = "=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),LookupSheet!$D$2,LookupSheet!$C$2, 'Value not found)')";
      var divSortFormula = "=XLOOKUP(INDIRECT(ADDRESS(ROW(),6)),LookupSheet!$G$2:$G,LookupSheet!$F$2:$F, 'Value not found')";
      var posSortFormula = "=IF(INDIRECT(ADDRESS(ROW()-1, 5))=INDIRECT(ADDRESS(ROW(),5)), INDIRECT(ADDRESS(ROW()-1,8))+1, 1)";

      sheet.getRange(row, 1).setValue(orgSortFormula);
      sheet.getRange(row, 2).setValue(orgIdFormula);
      sheet.getRange(row, 4).setValue(orgTypeFormula);
      sheet.getRange(row, 5).setValue(divSortFormula);
      sheet.getRange(row, 8).setValue(posSortFormula);
    }
  }
}

/**
 * Automatically create spreadsheet trigger
 */
function createSpreadsheetChangeTrigger() {
  ScriptApp.newTrigger('onChange')
    .forSpreadsheet(ss)
    .onChange()
    .create();
}

function onEdit(e) {
  var sheet = e.source.getActiveSheet();
  var sheetName = sheet.getName();
  if (sheetName != "LookupSheet") {
    var rowStart = e.range.rowStart;
    var rowCount = e.range.rowEnd - rowStart + 1;
    var sortRange = sheet.getRange(rowStart, 8, rowCount, 1);
    var dataRowLeft = sheet.getRange(rowStart, 1, rowCount, 7);
    var dataRowRight = sheet.getRange(rowStart, 9, rowCount, 7);

    if (!dataRowLeft.isBlank() && !dataRowRight.isBlank()) {
      sortRange.setFormula("=IF(INDIRECT(ADDRESS(ROW()-1, 5))=INDIRECT(ADDRESS(ROW(),5)),INDIRECT(ADDRESS(ROW()-1,8))+1,1)");
    }
  }
}

/**
 * Function to setup the data validation columns for the data sheets
 */
function createValidation() {
  var dataSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetById(0);
  var dataRefSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("LookupSheet");
  var orgRefSheet = SpreadsheetApp.openById("11iszULwAgixtSgSfUmyQt4EX054CTe47xMn4QheVgJw").getSheetById(0);
  var lastRowRefSheet = orgRefSheet.getLastRow();
  
  // org_sort data validation
  var orgSortRule = SpreadsheetApp.newDataValidation().requireNumberBetween(1, 29).build();
  var orgSortRange = dataSheet.getRange("$A$2:$A");
  orgSortRange.setDataValidation(orgSortRule)

  // org_id data validation
  var orgIdRule = SpreadsheetApp.newDataValidation().requireValueInList([
    "JPM", "MOF", "RURALLINK", "PETRA", "MOT", "KPKM", "EKONOMI", "KPKT", "KLN",
    "KKR", "MOHA", "MITI", "MOD", "MOSTI", "KPWKM", "NRES", "KUSKOP", "KPT", "MOTAC",
    "KOMUNIKASI", "MOE", "KPN", "KBS", "JWP", "KPDN", "KPK", "DIGITAL", "MOH", "MOHR"], true).build();
  var orgIdRange = dataSheet.getRange("Sheet1!$B$2:$B");
  orgIdRange.setDataValidation(orgIdRule);

  // org_name data validation
  var orgArray = dataRefSheet.getRange("$D$2:$D$29").getValues();
  var orgNameRule = SpreadsheetApp.newDataValidation().requireValueInList(orgArray);
  var orgNameRange = dataSheet.getRange("$C$2:$C");
  orgNameRange.setDataValidation(orgNameRule);

  // org_type data validation
  var orgTypeRule = SpreadsheetApp.newDataValidation().requireValueInList(["ministry"], false).build();
  var orgTypeRange = dataSheet.getRange("$D$2:$D")
  orgTypeRange.setDataValidation(orgTypeRule);

  // division_sort data validation
  var divSortRule = SpreadsheetApp.newDataValidation().requireNumberGreaterThan(0).build();
  var divSortRange = dataSheet.getRange("$E$2:$E")
  divSortRange.setDataValidation(divSortRule)

  // division_name data validation
  var divArray = dataRefSheet.getRange(2, 6, lastRowRefSheet-1, 1).getValues().flat();
  var divNameRule = SpreadsheetApp.newDataValidation().requireValueInList(divArray, true).build();
  var divNameRange = dataSheet.getRange("$F$2:F");
  divNameRange.setDataValidation(divNameRule);

  // person_email data validation
  var emailRange = dataSheet.getRange("$K$2:$K");
  var emailRule = SpreadsheetApp.newDataValidation().requireTextIsEmail().build();
  emailRange.setDataValidation(emailRule);
}

/**
 * Main setup function for the google sheet
 */
function mainSetup() {
  var spreadsheets = SpreadsheetApp.getActiveSpreadsheet();
  var sheetName = spreadsheets.getName();
  var sheetOrgId = sheetName.split(" ").pop();
  var lookupSheet = spreadsheets.getSheetByName("LookupSheet");

  if (lookupSheet == null) {
    lookupSheet = spreadsheets.insertSheet("LookupSheet");
    lookupSheet.appendRow(["org_id", "org_sort", "org_type", "org_name", "division_sort", "division_name"]);
  }
  lookupSheet.getRange("A2").setValue(sheetOrgId);

  lookupSheet.getRange("B2").setFormula('=FILTER(IMPORTRANGE("https://docs.google.com/spreadsheets/d/11iszULwAgixtSgSfUmyQt4EX054CTe47xMn4QheVgJw/edit?gid=0#gid=0", "OrgLookup!$B$2:$D$29"), IMPORTRANGE("https://docs.google.com/spreadsheets/d/11iszULwAgixtSgSfUmyQt4EX054CTe47xMn4QheVgJw/edit?gid=0#gid=0", "OrgLookup!$A$2:$A$29")=$A$2)');

  lookupSheet.getRange("H2").setFormula('=FILTER(IMPORTRANGE("https://docs.google.com/spreadsheets/d/11iszULwAgixtSgSfUmyQt4EX054CTe47xMn4QheVgJw/edit?gid=325480181#gid=325480181", "DivisionLookup!$B$2:$C"), IMPORTRANGE("https://docs.google.com/spreadsheets/d/11iszULwAgixtSgSfUmyQt4EX054CTe47xMn4QheVgJw/edit?gid=325480181#gid=325480181", "DivisionLookup!A2:A")=$D$2)');

  createValidation();
}