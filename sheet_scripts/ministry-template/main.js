/**
 * Trigger function to set the default value for the position_sort column
 */
function posSort(e) {
  var sheet = SpreadsheetApp.getActiveSheet();
  if(e.changeType === 'INSERT_ROW') {
    var row = sheet.getActiveRange().getRow();
    var sort_formula = "=IF(INDIRECT(ADDRESS(ROW()-1, 5))=INDIRECT(ADDRESS(ROW(),5)),INDIRECT(ADDRESS(ROW()-1,8))+1,1)";
    sheet.getRange(row, 8).setValue(sort_formula)
  }
}