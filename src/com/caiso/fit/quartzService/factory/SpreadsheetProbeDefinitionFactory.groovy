package com.caiso.fit.quartzService.factory

import com.caiso.fit.quartzService.impl.probeImpl.Audit
import com.caiso.fit.quartzService.impl.probeImpl.ProbeDefinition

import com.caiso.fit.quartzService.util.Log
import com.caiso.fit.quartzService.util.probeUtil.ColumnConverter

import groovy.sql.GroovyRowResult
import groovy.sql.Sql

import org.apache.poi.poifs.filesystem.POIFSFileSystem

import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.ss.usermodel.WorkbookFactory

import org.apache.poi.xssf.usermodel.XSSFWorkbook

// ******************************************************
// Class: SpreadsheetProbeDefinitionFactory
//
// ******************************************************
public class SpreadsheetProbeDefinitionFactory {
  // ******************************************************
  // Method: getXlConfig
  //
  // ******************************************************
  public void getXlConfig(Audit           audit,
                          ProbeDefinition probeDefinition,
                          Sql             configSql) {
    try {
      // ***************
      // queryStatement
      // ***************
      String queryStatement = """\
        select * from SPREADSHEET_PROBE_CONFIG
          where SPREADSHEET_PROBE_ID = $probeDefinition.probeId
        and SPREADSHEET_PROBE_NAME = '$probeDefinition.probeName'
        """

      GroovyRowResult row = configSql.firstRow(queryStatement)

      if (!row) {
        Log.error "Excel Probe : $probeDefinition.probeName is not configured in FIT_EXCEL_CONFIG"
        return
      }

      probeDefinition.sourceSpreadsheetName = row.SPREADSHEET_NAME
      probeDefinition.sheetNumber           = row.SHEET_NUMBER
      probeDefinition.startRow              = row.START_ROW
      probeDefinition.endRow                = row?.END_ROW
      probeDefinition.timestampColumnName   = row.SOURCE_TIMESTAMP_COLUMN_NAME
      probeDefinition.timestampColumnFormat = row?.SOURCE_TIMESTAMP_COLUMN_FORMAT

      if (!probeDefinition.sourceSpreadsheetName) {
        Log.error "Source XL Sheet name for the probe $probeDefinition.probeName is not configured in FIT_EXCEL_CONFIG."
        return
      }

      if (!probeDefinition.sheetNumber) {
        Log.error "Sheet Number for the probe $probeDefinition.probeName is not configured in FIT_EXCEL_CONFIG."
        return
      }

      if (!probeDefinition.startRow) {
        Log.info "Start Row for the probe $probeDefinition.probeName is not configured in FIT_EXCEL_CONFIG."
        return
      }

      if (!probeDefinition.timestampColumnName) {
        Log.error "Timestamp column name for the probe $probeDefinition.probeName is not configured in FIT_DATABASE_CONFIG."
        return
      }

      // *****************
      // Validate Source
      // *****************
      File parentDir  = new File(System.getProperty("user.dir")).getParentFile()
      File excelStage = new File("$parentDir/excel_stage")
      File sourceFile = new File("$excelStage/$probeDefinition.sourceSpreadsheetName")

      if (!sourceFile.exists()) {
        Log.error "Source Spreadsheet $probeDefinition.sourceSpreadsheetName NOT found in excel_stage directory!"
        return
      }
         
      int    dotPosition         = probeDefinition.sourceSpreadsheetName.lastIndexOf(".")
      String sourceFileExtension = probeDefinition.sourceSpreadsheetName.substring(dotPosition + 1)
      // *****************************
      // Getting Sheet from Workbook
      // *****************************
      if (sourceFileExtension == "xls") {
        Log.info "File Format of Spreadsheet is Microsoft Office Excel 2003 or older!"
        
        Workbook workbook     = WorkbookFactory.create(new POIFSFileSystem(new FileInputStream("$excelStage/$probeDefinition.sourceSpreadsheetName")))
        probeDefinition.sheet = workbook.getSheetAt(probeDefinition.sheetNumber - 1)
      }
      else if (sourceFileExtension == "xlsx") {
        Log.info "File Format of Spreadsheet is Microsoft Office Excel 2007 or later!"
        
        XSSFWorkbook workbook = new XSSFWorkbook(new FileInputStream("$excelStage/$probeDefinition.sourceSpreadsheetName"))
        probeDefinition.sheet = workbook.getSheetAt(probeDefinition.sheetNumber - 1)
      }
      else {
        Log.error "Unsupported format of Source Spreadsheet! Only XLS and XLSX are supported formats!"
        return
      }
        
      // ******************************
      // Validating sheet and end row
      // ******************************
      validateSheetAndEndRow(probeDefinition.sheet, probeDefinition.endRow)
      // *************************************************
      // Converting Timestamp Column Name to Column Number
      // *************************************************
      probeDefinition.timeStampColumnNumber = new ColumnConverter().convertColumnLetters(probeDefinition.timestampColumnName) - 1
      // *************************************************
      // Renaming Current Source File Extension to .wrk
      // *************************************************
      String fileNameWithoutExtension = sourceFile.name.substring(0, dotPosition)
      String fileNameWithNewExtension = fileNameWithoutExtension+"."+"wrk"
      sourceFile.renameTo(new File("$excelStage/$fileNameWithNewExtension"))
    }
    catch (Throwable t) {
     Log.error "Error occurred: $t"
      
      StringWriter stringWriter = new StringWriter()
      t.printStackTrace(new PrintWriter(stringWriter))
      
      Log.debug stringWriter.toString()
        
      audit.fitExceptions = stringWriter.toString()
    }
    finally {
      configSql?.connection?.commit()
      configSql?.connection?.close()
    }
  }

  // ******************************************************
  // Method: validateSheetAndEndRow
  //
  // ******************************************************
  private void validateSheetAndEndRow(Sheet   sheet,
                                      Integer endRow) {
    Log.info "Now validating Sheet and End Row..."
    
    if (!sheet) {
      Log.error "The Sheet Number configured in FIT_EXCEL_CONFIG table does not exist"
      return
    }

    if (!sheet.physicalNumberOfRows) {
      Log.error "The given sheet contains zero rows. Cannot load any data from this sheet"
      return
    }

    if (!endRow) {
      endRow = sheet.physicalNumberOfRows
      Log.info "End Row is not configured for the probe. Last Row loaded will be end of file."
    }
  }
}