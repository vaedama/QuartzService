package com.caiso.fit.quartzService.probeRunner

import com.caiso.fit.quartzService.impl.probeImpl.Probe

import com.caiso.fit.quartzService.rules.RuleFactory
import com.caiso.fit.quartzService.util.Log
import com.caiso.fit.quartzService.util.probeUtil.AuditUpdater

import groovy.sql.GroovyResultSet
import groovy.sql.Sql

import java.sql.Timestamp

import javax.naming.InitialContext

import javax.sql.DataSource

// ******************************************************
// Class: DatabaseToDatabaseProbe
//
// ******************************************************
public class DatabaseToDatabaseProbe extends Probe {
  // ***********
  // Constants
  // ***********
  private static final String FIT_TARGET_JNDI = 'FIT_DATA_TEST'

  // ******************************************************
  // Method: run
  //
  // ******************************************************
  @Override 
  public void run() {
    try {
      audit.targetJndiName = FIT_TARGET_JNDI
      
      getSql(FIT_TARGET_JNDI).connection?.autoCommit = false
      
      runInternal()
      
      getSql(FIT_TARGET_JNDI).connection?.commit()
    } 
    catch (Throwable t) {
      Log.error "Error occurred: $t"
      
      StringWriter stringWriter = new StringWriter()   
      t.printStackTrace(new PrintWriter(stringWriter))
      
      Log.debug stringWriter.toString()
        
      audit.fitExceptions = stringWriter.toString()
      
      getSql(FIT_TARGET_JNDI)?.connection?.rollback()
    }
    finally {
      getSql(FIT_TARGET_JNDI)?.connection?.commit()
      
      getSql(probeDefinition.sourceJndiName)?.connection?.rollback()
      
      getSql(FIT_TARGET_JNDI)?.connection?.close()  
      
      getSql(probeDefinition.sourceJndiName)?.connection?.close()
    }
  }

  // ******************************************************
  // Method: runInternal
  //
  // ******************************************************
  private void runInternal() {
    Long   runStartTimeMillis = System.currentTimeMillis()
    String runStartTimeText   = new Timestamp(runStartTimeMillis).toString()
    
    List columnList = []
    
    Integer rowsQueried   = 0
    Integer rowsInserted  = 0
    
    probeDefinition.mapping.each { sourceColumn, targetAttributeID ->
      columnList  << sourceColumn
    }
    
    DataSource targetDS = new InitialContext().lookup("java:$FIT_TARGET_JNDI")
    
    if (!targetDS) {
      Log.error("Could NOT find the $FIT_TARGET_JNDI Target JNDI Configuration")
      return
    }
    
    Sql targetSql = new Sql(targetDS)
    // *****************
    // QueryParameters
    // *****************
    List queryParameters = [ audit.queryLowTime, audit.queryHighTime ] 
    
    Log.info "StartTime, EndTime: $audit.queryLowTime, $audit.queryHighTime"
    
    // *****************
    // insertStatement
    // *****************
    String insertStatement = "insert into FIT_DATA (DATA_LOAD_ID , SOURCE_TIMESTAMP, ATTRIBUTE_ID, ATTRIBUTE_VALUE) values (?, ?, ?, ?)"
    
    if (!probeDefinition.sourceJndiName) {
      Log.info "Source JNDI Connection Pool: $probeDefinition.sourceJndiName does not exist"
      return
    }
    
    Log.info "*********************************"
    
    getSql(probeDefinition.sourceJndiName).eachRow(probeDefinition.sqlQuery, queryParameters) { GroovyResultSet groovyResultSet->
      
      Log.info "##############################################"
      
      for (int j=0; j<probeDefinition.mapping.size(); j++) {
        // *****************
        // InsertParameters
        // *****************
        List insertParameters =
          [
           audit.dataLoadID,
           groovyResultSet."$probeDefinition.timestampColumnName" as String,
           probeDefinition.mapping.get(columnList.getAt(j)) as Integer,
           groovyResultSet.getAt(columnList.getAt(j)) as String
          ]
          
          Log.info "================================================"
          
          targetSql.executeInsert(insertStatement, insertParameters)

          Log.info "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
          
         rowsInserted += targetSql.updateCount
        }
        rowsQueried += targetSql.updateCount
      }
    audit.measurementsQueried = probeDefinition.mapping.size()
    audit.rowsInserted        = rowsInserted
    audit.rowsQueried         = rowsQueried
    
    new AuditUpdater().updateAudit(probeDefinition, audit)
    
    Long runEndTimeMillis      = System.currentTimeMillis()
    audit.elapsedRunTimeMillis = runEndTimeMillis - runStartTimeMillis
    audit.runStartTime         = runStartTimeText
    audit.runEndTime           = new Timestamp(runEndTimeMillis).toString()
    
    new RuleFactory().executeRules(audit)
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