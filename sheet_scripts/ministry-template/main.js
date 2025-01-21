/**
 * Function to setup lookup sheet
 */
function setupDivisionSheet(spreadsheet, refSheet) {
  var sheetName = spreadsheet.getName();
  var divisionSheet = spreadsheet.getSheetByName("DivisionSheet");

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

  if (divisionSheet == null) {
    divisionSheet = spreadsheet.insertSheet("DivisionSheet").activate();
    spreadsheet.moveActiveSheet(3);
  }
  if (divisionSheet.getLastRow() == 0) {
    divisionSheet.appendRow(["division_sort", "division_name", "sheet_name"]);
    var divSheet = refSheet.getSheetByName("DivisionLookup");
    var rowCountDiv = divSheet.getDataRange().getLastRow()-1;
    var divData = refSheet.getSheetByName("DivisionLookup")
    .getRange(2, 1, rowCountDiv, 5)
    .getValues()
    .filter(function (x) {return (x[0]==sheetOrgId && x[4]==sheetDivision)})
    .map(function (x) {return x.slice(2, 4).concat(x.slice(3, 4))});

    divisionSheet.getRange(2, 1, divData.length, 3).setValues(divData);
  } else {
    console.log("Division Sheet already exists, skipping setup");
  }
  return divisionSheet;
}

/**Setup two pages:
 * Division sort
 * and sheet metadata: org_id etc
 */
function setupMetadataSheet(spreadsheet, refSheet) {
  var sheetName = spreadsheet.getName();
  var metadataSheet = spreadsheet.getSheetByName("MetadataSheet");

  // Checks if sheetname contains negeri
  var [sheetOrgId, sheetDivision] = sheetName.split("-")
  .pop()
  .trim()
  .toUpperCase()
  .split("_");

  if (metadataSheet == null) {
    metadataSheet = spreadsheet.insertSheet("MetadataSheet").activate();
    spreadsheet.moveActiveSheet(2);
  }
  if (metadataSheet.getLastRow() == 0) {
    metadataSheet.getRange("A1:A4").setValues([["org_id"], ["org_sort"], ["org_type"], ["org_name"]]);
    var orgSheet = refSheet.getSheetByName("OrgLookup");
    var rowCountOrg = orgSheet.getDataRange().getLastRow()-1;
    var orgData = orgSheet
    .getRange(2, 1, rowCountOrg, 4)
    .getValues()
    .filter(function (x) {return (x[0]==sheetOrgId)})
    .flat()
    .map(function (x) {return [x]});
    metadataSheet.getRange("B1:B4").setValues(orgData);
  } else {
    console.log("Metadata Sheet already exists, skipping setup");
  }
  return metadataSheet;
}

/**
 * Converting columns org_sort, org_id, org_type, div_sort, pos_sort into gsheet formulas
 */
function columnConversion(sheet, divisionSheetName, metadataSheetName) {
  var orgIdFormula = ['=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),', metadataSheetName, '!$B$4,', metadataSheetName, '!$B$1, "")'].join("");
  var orgSortFormula = ['=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),', metadataSheetName, '!$B$4,', metadataSheetName, '!$B$2, "")'].join("");
  var orgTypeFormula = ['=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),', metadataSheetName, '!$B$4,', metadataSheetName, '!$B$3, "")'].join("");
  var divSortFormula = ['=XLOOKUP(INDIRECT(ADDRESS(ROW(), 6)),', divisionSheetName, '!$B$2:$B,', divisionSheetName, '!$A$2:$A, "")'].join("");
  var posSortFormula = '=IF(AND(ARRAYFORMULA(OFFSET(INDIRECT(ADDRESS(ROW(), COLUMN())), 0, -7, 1, 7)="")),"",IF(INDIRECT(ADDRESS(ROW()-1, 5))=INDIRECT(ADDRESS(ROW(),5)), INDIRECT(ADDRESS(ROW()-1,8))+1, 1))';

  sheet.getRange("$A$2:$A").setValue(orgSortFormula);
  sheet.getRange("$B$2:$B").setValue(orgIdFormula);
  sheet.getRange("$D$2:$D").setValue(orgTypeFormula);
  sheet.getRange("$E$2:$E").setValue(divSortFormula);
  sheet.getRange("$H$2:$H").setValue(posSortFormula);
}

/**
 * Trigger function to set the default value for the position_sort column
 */
function posSort(e) {
  const sheet = e.source.getActiveSheet();
  if (sheet.getName() != "DivisionSheet" && sheet.getName() != "MetadataSheet") {
    if(e.changeType === 'INSERT_ROW') {
      var row = sheet.getActiveRange().getRow();
      if (row > 1){
        var orgIdFormula = ['=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),', metadataSheetName, '!$B$4,', metadataSheetName, '!$B$1, "")'].join("");
        var orgSortFormula = ['=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),', metadataSheetName, '!$B$4,', metadataSheetName, '!$B$2, "")'].join("");
        var orgTypeFormula = ['=XLOOKUP(INDIRECT(ADDRESS(ROW(), 3)),', metadataSheetName, '!$B$4,', metadataSheetName, '!$B$3, "")'].join("");
        var divSortFormula = ['=XLOOKUP(INDIRECT(ADDRESS(ROW(), 6)),', divisionSheetName, '!$B$2:$B,', divisionSheetName, '!$A$2:$A, "")'].join("");
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
 * Automatically create spreadsheet triggers
 */
function createSpreadsheetChangeTrigger(spreadsheet) {
  var triggerDict = {
    "changeTriggerExists": false,
    "timedTriggerExists": false,
    "mergeDivisionsExists": false
  };
  var allTriggers = ScriptApp.getProjectTriggers();
  for (let i=0;i<allTriggers.length;i++){
    if (allTriggers[i].getHandlerFunction()=="posSort") {
      triggerDict["changeTriggerExists"] = true;
    }
    else if (allTriggers[i].getHandlerFunction()=="mainSetup") {
      triggerDict["timedTriggerExists"] = true;
    }
    else if (allTriggers[i].getHandlerFunction()=="mergeDivisions") {
      triggerDict["mergeDivisionsExists"] = true;
    }
  }

  if (triggerDict["changeTriggerExists"]) {
    console.log("Trigger posSort already exists, skipping creation step");
  } else {
    ScriptApp.newTrigger("posSort")
    .forSpreadsheet(spreadsheet)
    .onChange()
    .create();
    console.log("Successfully created trigger posSort");
  }
  if (triggerDict["timedTriggerExists"]) {
    console.log("Trigger mainSetup already exists");
  } else {
    ScriptApp.newTrigger("mainSetup")
    .timeBased()
    .everyDays(1)
    .atHour(1)
    .nearMinute(10)
    .create();
    console.log("Successfully created trigger mainSetup");
  }
  if (triggerDict["mergeDivisionsExists"]) {
    console.log("Trigger mergeDivision already exists");
  } else {
    ScriptApp.newTrigger("mergeDivisions")
    .timeBased()
    .everyMinutes(30)
    .create();
    console.log("Successfully created trigger mergeDivisions");
  }
}

/**
 * Function to compile data from division sheets into a main sheet
 */
function mergeDivisions () {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var divisionSheet = spreadsheet.getSheetByName("DivisionSheet");
  var divisionName = divisionSheet.getRange(2, 2, divisionSheet.getLastRow()-1, 1).getValues().flat();
  var compiledData = [];
  var fullDataSheet = spreadsheet.getSheetByName("Keseluruhan Direktori");

  for (var sheetIdx in divisionName) {
    var sheet = spreadsheet.getSheetByName(divisionName[sheetIdx]);
    if (sheet != null) {
      var sheetLength = sheet.getRange("O:O").getValues().flat().filter(String).length-1;
      compiledData = compiledData.concat(sheet.getRange(2, 1, sheetLength, 15).getValues());
    }
  }
  if (fullDataSheet != null) {
    fullDataSheet.getRange(1, 1, 1, 15)
    .setValues([[
      "org_sort", "org_id", "org_name", "org_type","division_sort",
      "division_name", "subdivision_name", "position_sort",
      "position_name", "person_name", "person_email",
      "person_phone", "person_fax", "parent_org_id", "last_uploaded"
    ]])
    fullDataSheet.getRange(2, 1, compiledData.length, 15).setValues(compiledData);
  }
}

/**
 * Main setup function for the google sheet
 */
function mainSetup() {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var refSheet = SpreadsheetApp.openById("");

  console.log("Setting Metadata Sheet");
  setupMetadataSheet(spreadsheet, refSheet);
  console.log("Successfully set up Metadata Sheet");

  console.log("Setting Division Sheet");
  setupDivisionSheet(spreadsheet, refSheet);
  console.log("Successfully set up Division Sheet");

  // Get list of all data sheet names
  var sheetList = spreadsheet.getSheetByName("DivisionSheet")
  .getRange("C2:C")
  .getValues()
  .flat()
  .filter(String)
  .concat(["Keseluruhan Direktori", "Sheet1"]);

  // run columnConversion functions over all data sheets
  for (var sheetIdx in sheetList) {
    var sheetName = sheetList[sheetIdx];
    var sheet = spreadsheet.getSheetByName(sheetName)
    if (sheet == null) {
      console.log("Skipping division sheet: ", sheetName);
      continue
    }
    console.log("Converting prepopulated columns to XLOOKUP formulas for sheet: ", sheetName);
    columnConversion(sheet, "DivisionSheet", "MetadataSheet");
    console.log("Finished column conversion for sheet: ", sheetName);
  }

  console.log("Creating new row spreadsheet trigger");
  createSpreadsheetChangeTrigger(spreadsheet);
  console.log("Spreadsheet trigger successfully created");
}