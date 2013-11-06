package com.caiso.fit.quartzService.factory

import com.caiso.fit.quartzService.impl.probeImpl.Audit
import com.caiso.fit.quartzService.impl.probeImpl.ProbeDefinition

import com.caiso.fit.quartzService.util.Log

import groovy.sql.GroovyRowResult
import groovy.sql.Sql

// ******************************************************
// Class: DatabaseProbeDefinitionFactory
//
// ******************************************************
public class DatabaseProbeDefinitionFactory {
  // ******************************************************
  // Method: getDbConfig
  //
  // ******************************************************
  public void getDbConfig(Audit           audit,
                          ProbeDefinition probeDefinition, 
                          Sql             configSql) {
    try {
      // *****************
      // queryStatement
      // *****************
      String queryStatement = """\
        select * from DATABASE_PROBE_CONFIG
          where DATABASE_PROBE_ID = $probeDefinition.probeId 
        and DATABASE_PROBE_NAME = '$probeDefinition.probeName' 
        """
        
      GroovyRowResult row = configSql.firstRow(queryStatement)
      
      if (!row) {
        Log.error "Database Probe : $probeDefinition.probeName is not configured in FIT_DATABASE_CONFIG. Probe ID : $probeDefinition.probeId"
        return
      }
     
      probeDefinition.sourceJndiName      = row.SOURCE_JNDI_NAME
      probeDefinition.sqlQuery            = row.SOURCE_QUERY
      probeDefinition.timestampColumnName = row.SOURCE_TIMESTAMP_COLUMN_NAME

      if (!probeDefinition.sourceJndiName) {
        Log.error "Source Jndi Name for the Database probe $probeDefinition.probeName is not configured in FIT_DATABASE_CONFIG."
        return
      }
      
      if (!probeDefinition.sqlQuery) {
        Log.error "Source Query for the Database probe $probeDefinition.probeName is not configured in FIT_DATABASE_CONFIG."
        return
      }
     
      if (!probeDefinition.timestampColumnName) {
        Log.error "Timestamp column name for the probe $probeDefinition.probeName is not configured in FIT_DATABASE_CONFIG."
        return
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
      configSql?.connection?.commit()
      configSql?.connection?.close()
    }
  }
}