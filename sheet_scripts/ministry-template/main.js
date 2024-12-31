/**
 * Trigger function to set the default value for the position_sort column
 */
function posSort(e) {
  const sheet = e.source.getActiveSheet();
  if (sheet.getName() != "LookupSheet") {
    if(e.changeType === 'INSERT_ROW') {
      var row = sheet.getActiveRange().getRow();
      if (row > 1){
        var orgSortFormula = '=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),LookupSheet!$D$2,LookupSheet!$B$2, "")';
        var orgIdFormula = '=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),LookupSheet!$D$2,LookupSheet!$A$2, "")';
        var orgTypeFormula = '=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),LookupSheet!$D$2,LookupSheet!$C$2, "")';
        var divSortFormula = '=XLOOKUP(INDIRECT(ADDRESS(ROW(),6)),LookupSheet!$F$2:$F,LookupSheet!$E$2:$E, "")';
        var posSortFormula = '=IF(AND(ARRAYFORMULA(OFFSET(INDIRECT(ADDRESS(ROW(), COLUMN())), 0, -7, 1, 7)="")),"",IF(INDIRECT(ADDRESS(ROW()-1, 5))=INDIRECT(ADDRESS(ROW(),5)), INDIRECT(ADDRESS(ROW()-1,8))+1, 1))';

        sheet.getRange(row, 1).setValue(orgSortFormula);
        sheet.getRange(row, 2).setValue(orgIdFormula);
        sheet.getRange(row, 4).setValue(orgTypeFormula);
        sheet.getRange(row, 5).setValue(divSortFormula);
        sheet.getRange(row, 8).setValue(posSortFormula);
      }
    }
  }
}

/**
 * Automatically create spreadsheet trigger
 */
function createSpreadsheetChangeTrigger() {
  var changeTriggerExists = false;
  var timedTriggerExists = false;
  var allTriggers = ScriptApp.getProjectTriggers();
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  for (let i=0;i<allTriggers.length;i++){
    if (allTriggers[i].getHandlerFunction()=="posSort") {
      changeTriggerExists = true;
    }
    else if (allTriggers[i].getHandlerFunction()=="mainSetup") {
      timedTriggerExists = true;
    }
  }

  if (changeTriggerExists) {
    console.log("Trigger posSort already exists, skipping creation step");
  } else {
    ScriptApp.newTrigger('posSort')
    .forSpreadsheet(spreadsheet)
    .onChange()
    .create();
    console.log("Successfully created trigger posSort")
  }
  if (timedTriggerExists) {
    console.log("Trigger mainSetupn already exists");
  } else {
    ScriptApp.newTrigger('mainSetup')
    .timeBased()
    .everyDays(1)
    .atHour(1)
    .nearMinute(10)
    .create();
    console.log("Successfully created trigger mainSetup")
  }
}

function onEdit(e) {
  var sheet = e.source.getActiveSheet();
  var sheetName = sheet.getName();
  if (sheetName != "LookupSheet") {
    var rowStart = e.range.rowStart;
    if (rowStart > 1){
      var rowCount = e.range.rowEnd - rowStart + 1;
      var sortRange = sheet.getRange(rowStart, 8, rowCount, 1);
      var dataRowLeft = sheet.getRange(rowStart, 1, rowCount, 7);
      var dataRowRight = sheet.getRange(rowStart, 9, rowCount, 7);

      if (!dataRowLeft.isBlank() && !dataRowRight.isBlank()) {
        sortRange.setFormula('=IF(AND(ARRAYFORMULA(OFFSET(INDIRECT(ADDRESS(ROW(), COLUMN())), 0, -7, 1, 7)="")),"",IF(INDIRECT(ADDRESS(ROW()-1, 5))=INDIRECT(ADDRESS(ROW(),5)), INDIRECT(ADDRESS(ROW()-1,8))+1, 1))');
      }
    }
  }
}

/**
 * Function to setup the data validation columns for the data sheets
 */
function createValidation() {
  var dataSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetById(0);
  var dataRefSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("LookupSheet");
  var lastRowRefSheet = dataRefSheet.getLastRow();
  
  // org_sort data validation
  var orgSortRule = SpreadsheetApp.newDataValidation().requireNumberBetween(1, 29).build();
  var orgSortRange = dataSheet.getRange("$A$2:$A");
  orgSortRange.setDataValidation(orgSortRule);
  orgSortRange.protect().setDescription("The sorting order of the Organisation");

  // org_id data validation
  var orgIdArray = dataRefSheet.getRange("$A$2:$A").getValues().filter(String);
  var orgIdRule = SpreadsheetApp.newDataValidation().requireValueInList(orgIdArray, false).build();
  var orgIdRange = dataSheet.getRange("Sheet1!$B$2:$B");
  orgIdRange.setDataValidation(orgIdRule);
  orgIdRange.protect().setDescription("The ID of the Organisation");

  // org_name data validation
  var orgArray = dataRefSheet.getRange("$D$2:$D").getValues().filter(String);
  var orgNameRule = SpreadsheetApp.newDataValidation().requireValueInList(orgArray).build();
  var orgNameRange = dataSheet.getRange("$C$2:$C");
  orgNameRange.setDataValidation(orgNameRule);
  orgNameRange.protect().setDescription("The full name of the Organisation");

  // org_type data validation
  var orgTypeRule = SpreadsheetApp.newDataValidation().requireValueInList(["ministry"], false).build();
  var orgTypeRange = dataSheet.getRange("$D$2:$D")
  orgTypeRange.setDataValidation(orgTypeRule);
  orgTypeRange.protect().setDescription("The type of the Organisation");

  // division_sort data validation
  var divSortRule = SpreadsheetApp.newDataValidation().requireNumberGreaterThan(0).build();
  var divSortRange = dataSheet.getRange("$E$2:$E")
  divSortRange.setDataValidation(divSortRule)
  divSortRange.protect().setDescription("The sort order of the Organisation's divisions");

  // division_name data validation
  var divArray = dataRefSheet.getRange(2, 6, lastRowRefSheet-1, 1).getValues().flat();
  var divNameRule = SpreadsheetApp.newDataValidation().requireValueInList(divArray, true).build();
  var divNameRange = dataSheet.getRange("$F$2:F");
  divNameRange.setDataValidation(divNameRule);
  divNameRange.protect().setDescription("The division names under the Organisation");

  // person_email data validation
  var emailRange = dataSheet.getRange("$K$2:$K");
  var emailRule = SpreadsheetApp.newDataValidation().requireTextIsEmail().build();
  emailRange.setDataValidation(emailRule);
}

/**
 * Function to setup lookup sheet
 */
function setupLookupSheet() {
  var spreadsheets = SpreadsheetApp.getActiveSpreadsheet();
  var sheetName = spreadsheets.getName();
  var lookupSheet = spreadsheets.getSheetByName("LookupSheet");
  var refSheet = SpreadsheetApp.openById("")

  // Checks if sheetname contains negeri
  var [sheetOrgId, sheetDivision] = sheetName.split("-")
  .pop()
  .trim()
  .toUpperCase()
  .split("_");

  if (sheetDivision == null){
    sheetDivision = "standard";
  } else {
    sheetDivision = sheetDivision.toLowerCase();
  }

  if (lookupSheet == null) {
    lookupSheet = spreadsheets.insertSheet("LookupSheet");
    lookupSheet.appendRow(["org_id", "org_sort", "org_type", "org_name", "division_sort", "division_name"]);
    var orgSheet = refSheet.getSheetByName("OrgLookup");
    var rowCountOrg = orgSheet.getDataRange().getLastRow()-1;
    var orgData = orgSheet
    .getRange(2, 1, rowCountOrg, 4)
    .getValues()
    .filter(function (x) {return (x[0]==sheetOrgId)});

    var divSheet = refSheet.getSheetByName("DivisionLookup");
    var rowCountDiv = divSheet.getDataRange().getLastRow()-1;
    var divData = refSheet.getSheetByName("DivisionLookup")
    .getRange(2, 1, rowCountDiv, 5)
    .getValues()
    .filter(function (x) {return (x[0]==sheetOrgId && x[4]==sheetDivision)})
    .map(function (x) {return x.slice(2, 4)});

    lookupSheet.getRange("$A$2:$D$2").setValues(orgData);
    lookupSheet.getRange(2, 5, divData.length, 2).setValues(divData);
  } else {
    console.log("LookupSheet already exists, skipping setup")
  }

}

/**
 * Converting columns org_sort, org_id, org_type, div_sort, pos_sort into gsheet formulas
 */
function columnConversion() {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetById(0);
  var orgSortFormula = '=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),LookupSheet!$D$2,LookupSheet!$B$2, "")';
  var orgIdFormula = '=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),LookupSheet!$D$2,LookupSheet!$A$2, "")';
  var orgTypeFormula = '=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),LookupSheet!$D$2,LookupSheet!$C$2, "")';
  var divSortFormula = '=XLOOKUP(INDIRECT(ADDRESS(ROW(),6)),LookupSheet!$F$2:$F,LookupSheet!$E$2:$E, "")';
  var posSortFormula = '=IF(AND(ARRAYFORMULA(OFFSET(INDIRECT(ADDRESS(ROW(), COLUMN())), 0, -7, 1, 7)="")),"",IF(INDIRECT(ADDRESS(ROW()-1, 5))=INDIRECT(ADDRESS(ROW(),5)), INDIRECT(ADDRESS(ROW()-1,8))+1, 1))';

  sheet.getRange("$A$2:$A").setValue(orgSortFormula);
  sheet.getRange("$B$2:$B").setValue(orgIdFormula);
  sheet.getRange("$D$2:$D").setValue(orgTypeFormula);
  sheet.getRange("$E$2:$E").setValue(divSortFormula);
  sheet.getRange("$H$2:$H").setValue(posSortFormula);
}

/**
 * Main setup function for the google sheet
 */
function mainSetup() {
  console.log("Setting Lookup Sheet");
  setupLookupSheet();
  console.log("Successfully set up Lookup Sheet");

  console.log("Converting prepopulated columns to XLOOKUP formulas");
  columnConversion();
  console.log("Finished column conversion");

  console.log("Creating data validation rules");
  createValidation();
  console.log("Successfully created validation rules");

  console.log("Creating new row spreadsheet trigger");
  createSpreadsheetChangeTrigger();
  console.log("Spreadsheet trigger successfully created");
}