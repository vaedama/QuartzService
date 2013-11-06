package com.caiso.fit.quartzService.factory

import com.caiso.fit.quartzService.impl.probeImpl.Audit
import com.caiso.fit.quartzService.impl.probeImpl.ProbeDefinition

import com.caiso.fit.quartzService.util.Log

import groovy.sql.Sql

import javax.naming.InitialContext

import javax.sql.DataSource

// ******************************************************
// Class: AuditFactory
//
// ******************************************************
public class AuditFactory {  
  // **********
  // Constants
  // **********
  private static final String FIT_CONFIG_JNDI = 'FIT_CONFIG_TEST'
  
  // ******************************************************
  // Method: getAudit
  //
  // ******************************************************
  public void auditProbeRun(ProbeDefinition probeDefinition,
                            Audit           audit) {
    try {
      getSql(FIT_CONFIG_JNDI)?.connection?.autoCommit = false

      audit.probeID             = probeDefinition.probeId
      audit.probeType           = probeDefinition.probeType
      audit.probeDescription    = probeDefinition.probeDescription
      audit.sourceJndiName      = probeDefinition.sourceJndiName
      audit.rowsExpected        = probeDefinition?.rowsExpected
      audit.estimateFlag        = probeDefinition.estimateFlag
      audit.offset              = probeDefinition.offset
      audit.dataInterval        = probeDefinition.dataInterval
      audit.sourceName          = probeDefinition.sourceSpreadsheetName
      audit.sourceQuery         = probeDefinition.sqlQuery
      audit.timestampColumnName = probeDefinition.timestampColumnName

      saveAudit     (audit)
      saveNotes     (audit)
      saveExceptions(audit)

      getSql(FIT_CONFIG_JNDI)?.connection?.commit()
    }
    catch (Throwable t) {
      Log.error "Error occurred: $t"
      
      StringWriter stringWriter = new StringWriter()   
      t.printStackTrace(new PrintWriter(stringWriter))
      
      Log.debug stringWriter.toString()
        
      audit.fitExceptions = stringWriter.toString()
      
      getSql(FIT_CONFIG_JNDI)?.connection?.rollback()  
    } finally {
      getSql(FIT_CONFIG_JNDI)?.connection?.commit()
      getSql(FIT_CONFIG_JNDI)?.connection?.close()
    }
  }

  // ******************************************************
  // Method: Save Audit
  //
  // ******************************************************
  private saveAudit(Audit audit) {
    String insertStatement = """\
       insert into FIT_DATA_LOADS_AUDIT
       (DATA_LOAD_ID,                   DELETED_DATA_LOAD_ID,
        REPLACEMENT_DATA_LOAD_ID,       DATA_PROBE_NAME,
        GIVEN_START_DATE,               GIVEN_HOUR_STARTING,
        GIVEN_END_DATE,                 GIVEN_HOUR_ENDING,
        DATA_PROBE_ID,                  DATA_PROBE_TYPE,
        DATA_PROBE_DESCRIPTION,         TIMESTAMP_COLUMN_NAME,          
        DATA_INTERVAL,                  OFFSET,                         
        SOURCE_NAME,                    SOURCE_QUERY,                   
        SOURCE_JNDI_NAME,               CONFIG_JNDI_NAME,               
        TARGET_JNDI_NAME,               RUN_START_TIME,                 
        RUN_END_TIME,                   ELAPSED_RUN_TIME,               
        ROWS_EXPECTED,                  ROWS_QUERIED,                   
        MEASUREMENTS_QUERIED,           ROWS_INSERTED,                  
        ROWS_DELETED,                   ESTIMATE_FLAG,                  
        START_TIME,                     END_TIME,                       
        SUCCESS_FLAG) 
        values 
       (?.dataLoadID,                   ?.deletedDataLoadID,
        ?.replacementDataLoadID,        ?.dataProbeName,
        ?.givenStartDate,               ?.givenHourStarting,
        ?.givenEndDate,                 ?.givenHourEnding,   
        ?.probeID,                      ?.probeType,
        ?.probeDescription,             ?.timestampColumnName,          
        ?.dataInterval,                 ?.offset,                       
        ?.sourceName,                   ?.sourceQuery,                  
        ?.sourceJndiName,               ?.configJndiName,               
        ?.targetJndiName,               ?.runStartTime,                 
        ?.runEndTime,                   ?.elapsedRunTimeMillis,               
        ?.rowsExpected,                 ?.rowsQueried,                  
        ?.measurementsQueried,          ?.rowsInserted,                 
        ?.rowsDeleted,                  ?.estimateFlag,                 
        ?.queryLowTime,                 ?.queryHighTime,                      
        ?.successFlag)
     """
    
    getSql(FIT_CONFIG_JNDI).execute(insertStatement, audit)
  }

  // ******************************************************
  // Method: saveNotes
  //
  // ******************************************************
  private saveNotes(Audit audit) {
    
    getSql(FIT_CONFIG_JNDI).execute('insert into FIT_NOTES (DATA_LOAD_ID, NOTES) values (?,?)', [audit.dataLoadID, audit.notes])
  }

  // ******************************************************
  // Method: saveExceptions
  //
  // ******************************************************
  private saveExceptions(Audit audit) {
    
    getSql(FIT_CONFIG_JNDI).execute('insert into FIT_DATA_LOAD_EXCEPTIONS (DATA_LOAD_ID, EXCEPTION_TEXT) values (?,?)', [audit.dataLoadID, audit.fitExceptions])
  }
  
  // ******************************************************
  // Method: getSql
  //
  // ******************************************************
  private Sql getSql(String jndiName) {
    DataSource dataSource = (DataSource) new InitialContext().lookup("java:$jndiName")
    
    return (dataSource == null ? null : new Sql(dataSource))
  }
}