package com.caiso.fit.quartzService.util.jobUtil

import com.caiso.fit.quartzService.impl.probeImpl.Audit

import com.caiso.fit.quartzService.util.Log

import groovy.sql.GroovyRowResult
import groovy.sql.Sql

import java.sql.Timestamp

import javax.naming.InitialContext

import javax.sql.DataSource

import org.quartz.JobDataMap
import org.quartz.JobExecutionContext

// ******************************************************
// Class: FitJobTaskAuditRunner
//
// ******************************************************
public class FitJobTaskAuditRunner {
  // ************
  // Constants
  // ************
  private static final String FIT_CONFIG_JNDI = 'FIT_CONFIG_TEST'
  
  // ******************************************************
  // Method: auditFitJobTask
  //
  // ******************************************************
  public void auditFitJobTask(Audit               audit,
                              JobExecutionContext jobExecutionContext) {
    try {
      audit.successFlag    = 'Fail'
      audit.configJndiName = FIT_CONFIG_JNDI

      String queryStatement = "select FIT_DATA_LOAD_SEQ.nextval as DATA_LOAD_ID from dual"

      audit.dataLoadID = getSql(FIT_CONFIG_JNDI).firstRow(queryStatement)?.DATA_LOAD_ID as Integer

      if (!audit.dataLoadID) {
        Log.error "Next Value for FIT_DATA_LOADS_SEQ could NOT be generated!"
        audit.notes = "Could NOT save Data Load ID for the probe!"
        return
      }

      Log.info "Created Data Load ID from oracle sequence for the Probe. Data Load ID : $audit.dataLoadID"
      
      JobDataMap jobDataMap = jobExecutionContext.getTrigger().getJobDataMap()

      Log.info "Checking if the current job's job execution context has any values in job data map..."
      
      if (!jobDataMap.containsKey("DataLoadID")) {
        Log.info "Job data map does NOT have any values. The current job is a normal job!"
        
        auditNormalFitJobTask(audit, jobDataMap, jobExecutionContext)
      }
      else {
        Log.info "Job data map has rerun id value. The current job is a rerun job!"
        
        auditRerunFitJobTask(audit, jobDataMap)
      }
    }
    catch (Throwable t) {
      Log.error "Error occurred: $t"
      
      StringWriter stringWriter = new StringWriter()
      t.printStackTrace(new PrintWriter(stringWriter))
      
      Log.debug stringWriter.toString()
        
      audit.fitExceptions = stringWriter.toString()
    }
    finally {
      getSql(FIT_CONFIG_JNDI)?.connection?.commit()
      getSql(FIT_CONFIG_JNDI)?.connection?.close()
    }
  }

  // ******************************************************
  // Method: auditNormalFitJobTask
  //
  // ******************************************************
  private void auditNormalFitJobTask(Audit               audit,
                                     JobDataMap          jobDataMap,
                                     JobExecutionContext jobExecutionContext) {
    audit.dataProbeName     = jobDataMap.getString('JobName')   
    audit.givenStartDate    = jobExecutionContext.trigger.startTime.toString()
    audit.givenHourStarting = jobExecutionContext.trigger.startTime.hours
    audit.fireTimeMillis    = jobExecutionContext.fireTime.time
    
    audit.rowsDeleted = 0
    
    if (jobExecutionContext.trigger.endTime) {
      audit.givenEndDate    = jobExecutionContext.trigger.endTime.toString()
      audit.givenHourEnding = jobExecutionContext.trigger.endTime.hours
    }
  }
                                       
  // ******************************************************
  // Method: auditRerunFitJobTask
  //
  // ******************************************************
  private void auditRerunFitJobTask(Audit      audit,
                                    JobDataMap jobDataMap) {
    audit.deletedDataLoadID = jobDataMap.getIntValue("DataLoadID")
    // ****************
    // queryStatement
    // ****************
    String queryStatement = "select * from FIT_DATA_LOADS_AUDIT where DATA_LOAD_ID = $audit.deletedDataLoadID"

    GroovyRowResult row = getSql(FIT_CONFIG_JNDI).firstRow(queryStatement)

    audit.dataProbeName = row.DATA_PROBE_NAME
    audit.rowsDeleted   = row?.ROWS_INSERTED
    audit.queryLowTime  = row.START_TIME
    audit.queryHighTime = row.END_TIME

    Log.info "Probe: $audit.dataProbeName with ID: $audit.deletedDataLoadID will be re-run."
    
    // *****************
    // updateStatement
    // *****************
    String updateStatement = """\
      update FIT_DATA_LOADS_AUDIT
        set REPLACEMENT_DATA_LOAD_ID = $audit.dataLoadID
      where DATA_PROBE_NAME = '$audit.dataProbeName'
         and DATA_LOAD_ID = $audit.deletedDataLoadID
      """

    getSql(FIT_CONFIG_JNDI).executeUpdate(updateStatement)
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