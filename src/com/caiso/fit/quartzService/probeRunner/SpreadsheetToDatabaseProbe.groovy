package com.caiso.fit.quartzService.probeRunner

import com.caiso.fit.quartzService.impl.probeImpl.Probe

import com.caiso.fit.quartzService.rules.RuleFactory
import com.caiso.fit.quartzService.util.Log
import com.caiso.fit.quartzService.util.probeUtil.AuditUpdater
import com.caiso.fit.quartzService.util.probeUtil.ExcelDataLoader
import com.caiso.fit.quartzService.util.probeUtil.SpreadsheetArchiver

import java.sql.Timestamp

import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.usermodel.DateUtil

// ******************************************************
// Class : SpreadsheetToDatabaseProbe
//
// ******************************************************
public class SpreadsheetToDatabaseProbe extends Probe {
  // ******************************************************
  // Method : run
  //
  // ******************************************************
  @Override
  public void run() {
    try {
      runInternal()
    }
    catch (Throwable t) {
      Log.error "Error occurred: $t"
      
      StringWriter stringWriter = new StringWriter()
      t.printStackTrace(new PrintWriter(stringWriter))
      
      Log.debug stringWriter.toString()
        
      audit.fitExceptions = stringWriter.toString()
    }
  }

  // ******************************************************
  // Method : runInternal
  //
  // ******************************************************
  private void runInternal() {
    Long runStartTimeMillis = System.currentTimeMillis()
    Date runStartTime       = new Timestamp(runStartTimeMillis)

    if (probeDefinition.startRow > probeDefinition.sheet?.physicalNumberOfRows) {
      audit.notes = "Given start row value exceeded the last physical row in $probeDefinition.sourceSpreadsheetName"
      Log.error "Given start row value exceeded the last physical row in $probeDefinition.sourceSpreadsheetName, sheet number : $probeDefinition.sheetNumber."
      return
    }

    if (!probeDefinition.mapping.size()) {
      audit.notes = 'No Measurements were Queried'
      return
    }

    if (audit.rowsExpected >= 1) {
      audit.notes       = "Expected Rows was -ve, defaulted to null"
      audit.successFlag = 'Success'
    }
    
    Row  row  = probeDefinition.sheet?.getRow(probeDefinition.startRow)
    
    Cell cell = row?.getCell(probeDefinition.timeStampColumnNumber)
    
    // **********************************
    // CellType static final integers:
    // CELL_TYPE_NUMERIC = 0
    // CELL_TYPE_STRING  = 1
    // CELL_TYPE_FORMULA = 2
    // **********************************
    switch (cell.cellType) {
      case Cell.CELL_TYPE_STRING:
        Log.info "Timestamp Column Cell type is String."
        
        new ExcelDataLoader().loadXLtoDB(cell.cellType, audit, probeDefinition)
        break
      case Cell.CELL_TYPE_NUMERIC:
        if (DateUtil.isCellDateFormatted(cell)) {
          Log.info "Timestamp Column Cell type is Numeric and Date Cell is Formatted in Timestamp Column"
             
          new ExcelDataLoader().loadXLtoDB(cell.cellType, audit, probeDefinition)
        }
        else {
          Log.info "Timestamp Column Cell type is Numeric and Date Cell is Unformatted in Timestamp Column"
             
          new ExcelDataLoader().loadXLtoDB(cell.cellType + 1, audit, probeDefinition)
        }
        break
      case Cell.CELL_TYPE_FORMULA:
        Log.error "Timestamp Column Cell Type is a Formula:" + cell.getCellFormula() + "Formula Type is Unsupported for Timestamp Column"
        break
      case Cell.CELL_TYPE_BLANK:
        Log.error "Blank Cell Has Been Encountered For The First Row # of Timestamp Column"
        break
      default:
        Log.error "Error! Encountered Unsupported Cell Type for Timestamp column"
        break
    }

    Log.info "Now archiving spreadsheet..."
    
    new SpreadsheetArchiver().archiveXL(probeDefinition.sourceSpreadsheetName)
    
    Log.info "Archiving completed. Updating Audit for the probe run..."
    
    Long runEndTimeMillis      = System.currentTimeMillis()
    audit.elapsedRunTimeMillis = runEndTimeMillis - runStartTimeMillis
    audit.runStartTime         = runStartTime
    audit.runEndTime           = new Timestamp(runEndTimeMillis)
    
    new AuditUpdater().updateAudit(probeDefinition, audit)
    
    new RuleFactory().executeRules(audit)
  }
}