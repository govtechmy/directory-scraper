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
 * Function to setup a button to refresh the data validation rules
 */
function setupButton(divisionSheet, imageId) {
  var buttonImages = divisionSheet.getImages().filter(function (x) {return x.getAltTextDescription() == "Refresh data validation rules"});
  if (buttonImages.length == 0) {
    var image = DriveApp.getFileById(imageId).getThumbnail().getAs("image/png");
    var sheetImage = divisionSheet.insertImage(image, 6, 1).setHeight(34).setWidth(200);
    sheetImage.assignScript("createValidation").setAltTextDescription("Refresh data validation rules");
  } else {
    console.log("Refresh data validation button already exists, skipping step");
  }
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
 * Function to setup the data validation columns for the data sheets
 */
function createValidation(dataSheet, divisionSort, divisionName, metadataDictionary) {
  var protectedRanges = dataSheet.getProtections(SpreadsheetApp.ProtectionType.RANGE)
  .map(function (x) {return x.getRange().getA1Notation()});

  
  // org_sort data validation
  var orgSortArray = [metadataDictionary["org_sort"]];
  var orgSortRule = SpreadsheetApp.newDataValidation().requireValueInList(orgSortArray, false).build();
  var orgSortRange = dataSheet.getRange("$A$2:$A");
  orgSortRange.setDataValidation(orgSortRule);
  if (protectedRanges.indexOf("A2:A") == -1) {
    orgSortRange.protect().setDescription("The sorting order of the Organisation");
  }

  // org_id data validation
  var orgIdArray = [metadataDictionary["org_id"]];
  var orgIdRule = SpreadsheetApp.newDataValidation().requireValueInList(orgIdArray, false).build();
  var orgIdRange = dataSheet.getRange("B2:B");
  orgIdRange.setDataValidation(orgIdRule);
  if (protectedRanges.indexOf("B2:B") == -1) {
    orgIdRange.protect().setDescription("The ID of the Organisation");
  }

  // org_name data validation
  var orgArray = [metadataDictionary["org_name"]];
  var orgNameRule = SpreadsheetApp.newDataValidation().requireValueInList(orgArray).build();
  var orgNameRange = dataSheet.getRange("C2:C");
  orgNameRange.setDataValidation(orgNameRule);
  if (protectedRanges.indexOf("C2:C") == -1) {
    orgNameRange.protect().setDescription("The full name of the Organisation");
  }

  // org_type data validation
  var orgTypeArray = [metadataDictionary["org_type"]];
  var orgTypeRule = SpreadsheetApp.newDataValidation().requireValueInList(orgTypeArray, false).build();
  var orgTypeRange = dataSheet.getRange("D2:D")
  orgTypeRange.setDataValidation(orgTypeRule);
  if (protectedRanges.indexOf("D2:D") == -1) {
    orgTypeRange.protect().setDescription("The type of the Organisation");
  }

  // division_sort data validation
  var divSortRule = SpreadsheetApp.newDataValidation().requireValueInList(divisionSort, false).build();
  var divSortRange = dataSheet.getRange("E2:E")
  divSortRange.setDataValidation(divSortRule)
  if (protectedRanges.indexOf("E2:E") == -1) {
    divSortRange.protect().setDescription("The sort order of the Organisation's divisions");
  }

  // division_name data validation
  var divNameRule = SpreadsheetApp.newDataValidation().requireValueInList(divisionName, true).build();
  var divNameRange = dataSheet.getRange("F2:F");
  divNameRange.setDataValidation(divNameRule);
  if (protectedRanges.indexOf("F2:F") == -1) {
    divNameRange.protect().setDescription("The division names under the Organisation");
  }

  // person_email data validation
  var emailRange = dataSheet.getRange("K2:K");
  var emailRule = SpreadsheetApp.newDataValidation().requireTextIsEmail().build();
  emailRange.setDataValidation(emailRule);
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
    .forSpreadsheet(spreadsheet)
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

// function onEdit(e) {
//   var sheet = e.source.getActiveSheet();
//   var sheetName = sheet.getName();
//   if (sheetName != "LookupSheet") {
//     var rowStart = e.range.rowStart;
//     if (rowStart > 1){
//       var rowCount = e.range.rowEnd - rowStart + 1;
//       var sortRange = sheet.getRange(rowStart, 8, rowCount, 1);
//       var dataRowLeft = sheet.getRange(rowStart, 1, rowCount, 7);
//       var dataRowRight = sheet.getRange(rowStart, 9, rowCount, 7);

//       if (!dataRowLeft.isBlank() && !dataRowRight.isBlank()) {
//         sortRange.setFormula('=IF(AND(ARRAYFORMULA(OFFSET(INDIRECT(ADDRESS(ROW(), COLUMN())), 0, -7, 1, 7)="")),"",IF(INDIRECT(ADDRESS(ROW()-1, 5))=INDIRECT(ADDRESS(ROW(),5)), INDIRECT(ADDRESS(ROW()-1,8))+1, 1))');
//       }
//     }
//   }
// }

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
  var metadataSheet = setupMetadataSheet(spreadsheet, refSheet);
  console.log("Successfully set up Metadata Sheet");

  console.log("Setting Division Sheet");
  var divisionSheet = setupDivisionSheet(spreadsheet, refSheet);
  console.log("Successfully set up Division Sheet");

  var divisionSheet = spreadsheet.getSheetByName("DivisionSheet");
  var metadataSheet = spreadsheet.getSheetByName("MetadataSheet");
  var imageId = "";

  console.log("Inserting data validation refresh button into Division sheet");
  setupButton(divisionSheet, imageId);
  console.log("Successfully inserted refresh button")

  // Get list of all data sheet names
  var sheetList = spreadsheet.getSheetByName("DivisionSheet")
  .getRange("C2:C")
  .getValues()
  .flat()
  .filter(String)
  .concat(["Keseluruhan Direktori", "Sheet1"]);

  // Get data values for columnConversion and createValidation functions
  var divisionSort = divisionSheet.getRange(2, 1, divisionSheet.getLastRow()-1, 1).getValues().flat();
  var divisionName = divisionSheet.getRange(2, 2, divisionSheet.getLastRow()-1, 1).getValues().flat();
  var metadataValues = metadataSheet.getRange(1, 1, metadataSheet.getLastRow(), 2)
  .getValues();
  var metadataDictionary = {};
  metadataValues
  .forEach(x => {
    metadataDictionary[x[0]] = x[1];
  });

  // run columnConversion and createValidation functions over all data sheets
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

    console.log("Creating data validation rules for sheet: ", sheetName);
    createValidation(sheet, divisionSort, divisionName, metadataDictionary);
    console.log("Successfully created validation rules for sheet: ", sheetName);
  }

  console.log("Creating new row spreadsheet trigger");
  createSpreadsheetChangeTrigger(spreadsheet);
  console.log("Spreadsheet trigger successfully created");
}