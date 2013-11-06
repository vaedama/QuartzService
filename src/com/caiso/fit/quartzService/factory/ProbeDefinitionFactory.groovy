package com.caiso.fit.quartzService.factory

import com.caiso.fit.quartzService.impl.probeImpl.Audit
import com.caiso.fit.quartzService.impl.probeImpl.ProbeDefinition
import com.caiso.fit.quartzService.util.Log

import groovy.sql.GroovyRowResult
import groovy.sql.Sql

import javax.naming.InitialContext

import javax.sql.DataSource

import org.quartz.JobExecutionContext

// ******************************************************
// Class: ProbeDefinitionFactory
//
// ******************************************************
public class ProbeDefinitionFactory {
  // **********
  // Constants
  // **********
  private static final String FIT_CONFIG_JNDI = 'FIT_CONFIG_TEST'
  private static final String DB_PROBE        = 'Database Probe'
  private static final String XL_PROBE        = 'Spreadsheet Probe'
  
  // ******************************************************
  // Method: getDefinition
  //
  // ******************************************************
  public ProbeDefinition getDefinition(Audit               audit, 
                                       JobExecutionContext jobExecutionContext) {
    try {
      // ****************
      // queryStatement
      // ****************
      String queryStatement = "select * from PROBE_CONFIG where PROBE_NAME = '$audit.dataProbeName'"
      
      GroovyRowResult row = getSql(FIT_CONFIG_JNDI).firstRow(queryStatement)

      if (!row) {
        Log.error "Probe : $audit.dataProbeName is not configured!"
        return
      }

      ProbeDefinition probeDefinition = new ProbeDefinition()
      
      probeDefinition.probeName        = row.PROBE_NAME
      probeDefinition.probeType        = row.PROBE_TYPE
      probeDefinition.probeId          = row.PROBE_ID
      probeDefinition.probeDescription = row.PROBE_DESCRIPTION
      probeDefinition.rowsExpected     = row?.ROWS_EXPECTED
      probeDefinition.offset           = row.OFFSET_FROM_FIRETIME
      probeDefinition.estimateFlag     = row.ESTIMATE_FLAG
      probeDefinition.dataInterval     = row.DATA_INTERVAL
      probeDefinition.timeZone         = row.SOURCE_TIMEZONE
        
      // *********************************************
      // Query Low Timestamp and Query High Timestamp
      // *********************************************
      if (!jobExecutionContext.getTrigger().getJobDataMap().containsKey("DataLoadID")) {
        Long dataIntervalMillis = probeDefinition.dataInterval * 60 * 1000  
        Long offsetMillis       = probeDefinition.offset * 60 * 1000
 
        audit.queryLowTime  = format(probeDefinition.timeZone, audit.fireTimeMillis - offsetMillis)
        audit.queryHighTime = format(probeDefinition.timeZone, audit.fireTimeMillis - offsetMillis + dataIntervalMillis)
      }
      
      if (probeDefinition.probeType == DB_PROBE) {
        createMapObject(probeDefinition)
        
        Log.info "Probe Type found is $DB_PROBE."
        
        new DatabaseProbeDefinitionFactory().getDbConfig(audit, probeDefinition, getSql(FIT_CONFIG_JNDI))  
        
        return probeDefinition
      }
      else if (probeDefinition.probeType == XL_PROBE) {
        createMapObject(probeDefinition)
        
        Log.info "Probe Type found is $XL_PROBE."
        
        new SpreadsheetProbeDefinitionFactory().getXlConfig(audit, probeDefinition, getSql(FIT_CONFIG_JNDI))

        return probeDefinition
      }

      audit.notes = "Invalid Probe Type"
      
      Log.error "Probe Type is NOT yet defined in Probe Definition Factory. Valid probe types are ExcelToDB and DBtoDb only"
      
      return 
    } catch (Throwable t) {
      Log.error "Error occurred: $t"
      
      StringWriter stringWriter = new StringWriter()   
      t.printStackTrace(new PrintWriter(stringWriter))
      
      Log.debug stringWriter.toString()
        
      audit.fitExceptions = stringWriter.toString()
    } finally {
      getSql(FIT_CONFIG_JNDI)?.connection?.commit()
      getSql(FIT_CONFIG_JNDI)?.connection?.close()
    }
  }

  // ******************************************************
  // Method: createMapObject
  //
  // ******************************************************
  private void createMapObject(ProbeDefinition probeDefinition) {
    // *****************
    // mappingStatement
    // *****************
    String mappingStatement = """\
      select SOURCE_COLUMN, TARGET_ATTRIBUTE_ID
        from FIT_COLUMN_CONFIG
      where PROBE_NAME_MAPPING_FOR_COLS = '$probeDefinition.probeName'
      """

    probeDefinition.mapping = [:]

    getSql(FIT_CONFIG_JNDI).rows(mappingStatement).each { row ->
      probeDefinition.mapping."$row.SOURCE_COLUMN" = row.TARGET_ATTRIBUTE_ID as int
    }

    if (!probeDefinition.mapping) {
      Log.error "No mapping found for $probeDefinition.probeName in COLUMN_CONFIG"
      return
    }
  }
  
  // ******************************************************
  // Method: format
  //
  // ******************************************************
  private String format(String timeZone, Long milliSeconds) {
    Calendar sourceCalendar     = Calendar.getInstance(TimeZone.getTimeZone("$timeZone"))
    sourceCalendar.timeInMillis = milliSeconds
    return sourceCalendar.format('yyyyMMddHHmmss')
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