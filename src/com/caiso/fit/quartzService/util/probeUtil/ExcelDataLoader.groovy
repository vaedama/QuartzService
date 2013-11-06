package com.caiso.fit.quartzService.util.probeUtil

import com.caiso.fit.quartzService.impl.probeImpl.Audit
import com.caiso.fit.quartzService.impl.probeImpl.ProbeDefinition

import com.caiso.fit.quartzService.util.Log

import groovy.sql.GroovyResultSet
import groovy.sql.Sql

import java.sql.Timestamp

import java.text.SimpleDateFormat

import javax.naming.InitialContext

import javax.sql.DataSource

import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.Row

// ******************************************************
// Class: ExcelDataLoader
//
// ******************************************************
public class ExcelDataLoader {
  // ***********
  // Constants
  // ***********
  private static final String FIT_TARGET_JNDI = 'FIT_DATA_TEST'
  private static final String INSERT_STATEMENT = "insert into FIT_DATA (DATA_LOAD_ID , SOURCE_TIMESTAMP, ATTRIBUTE_ID,  ATTRIBUTE_VALUE) values (?, ?, ?, ?)"

  // ******************************************************
  // Method: loadXLtoDB
  //
  // ******************************************************
  public void loadXLtoDB(Integer         cellType,
                         Audit           audit,
                         ProbeDefinition probeDefinition) {
    try {
      Log.info "Initiated ExcelDataLoader..."

      audit.targetJndiName = FIT_TARGET_JNDI

      Sql targetSql = getSql(FIT_TARGET_JNDI)

      targetSql?.connection?.autoCommit = false

      loadInternal(cellType, audit, probeDefinition, targetSql)
      
      Log.info "Data Load Completed!"

      targetSql?.connection?.commit()
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
      getSql(FIT_TARGET_JNDI)?.connection?.close()
    }
  }

  // ******************************************************
  // Method: loadInternal
  //
  // ******************************************************
  private void loadInternal(Integer         cellType,
                            Audit           audit,
                            ProbeDefinition probeDefinition,
                            Sql             targetSql) {
    Integer skippedRows  = 0
    Integer rowsQueried  = 0
    List    columnList   = []
    
    probeDefinition.mapping.each { sourceColumn, targetAttributeID ->
      columnList  << sourceColumn
    }
    
    for (int rowNumber = probeDefinition.startRow; rowNumber < probeDefinition.endRow; rowNumber++) {
      
      audit.rowsQueried += targetSql.updateCount
      
      // *****************************************************************
      // Converting source time in source time zone to time in GMT format
      // *****************************************************************
      Row      row                = probeDefinition.sheet?.getRow(rowNumber)
      Cell     timestampCell      = row?.getCell(probeDefinition.timeStampColumnNumber)
      Calendar sourceCalendar     = Calendar.getInstance(TimeZone.getTimeZone("$probeDefinition.timeZone"))
      sourceCalendar.timeInMillis = epochTimeFromSourceTimestampCell(cellType, timestampCell)
      Calendar gmtCalendar        = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
      gmtCalendar.timeInMillis    = sourceCalendar.timeInMillis
      String timestamp            = gmtCalendar.format('yyyyMMddHHmmss')
      
      if (!timestampCell) {
        skippedRows = targetSql.updateCount + skippedRows
      }
      
      if (cellType == 0) {
        dataLoadForDateFormatTimestampCell(audit, probeDefinition, targetSql, gmtCalendar.timeInMillis, row)
      }
      else {
        dataLoadForStringFormatTimestampCell(probeDefinition, audit, targetSql, columnList)
      }
    }
  }
                            
  // ******************************************************
  // Method: epochTimeFromTimestampCell
  //
  // ******************************************************
  private Long epochTimeFromSourceTimestampCell(Integer cellType,
                                                Cell    timestampCell) {
    if (cellType == 0) {
      
      return timestampCell?.getDateCellValue().time
    }
    
    String           dateString = timestampCell?.richStringCellValue.string
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    Date             parsedDate = dateFormat.parse(dateString)
  
    return parsedDate.time
  }
  
  // ******************************************************
  // Method: gmtTime
  //
  // ******************************************************
  private Long gmtTime(Long milliSeconds) {
    Calendar gmtCalendar     = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
    gmtCalendar.timeInMillis = milliSeconds
             
    return gmtCalendar.timeInMillis
  }
  
  // ******************************************************
  // Method: dataLoadForDateFormatTimestampCell
  //
  // ******************************************************
  private void dataLoadForDateFormatTimestampCell(Audit           audit,
                                                  ProbeDefinition probeDefinition,
                                                  Sql             targetSql,
                                                  Long            time,
                                                  Row             row) {
    Long startTime = gmtTime(audit.fireTimeMillis - probeDefinition.offset * 60 * 1000)
    Long endTime   = gmtTime(audit.fireTimeMillis - probeDefinition.offset * 60 * 1000 + probeDefinition.dataInterval * 60 * 1000)
    
    if (time >= startTime && time < endTime) {
  
      probeDefinition.mapping.each { String sourceColumn, Integer targetAttributeId ->
        
        String attributeValue = row?.getCell(targetAttributeId - 1) as String
        
        // *****************
        // insertParameters
        // *****************
        List insertParameters =
          [
           audit.dataLoadID,
           new Timestamp(time),
           targetAttributeId,
           attributeValue
          ]
          
        targetSql.executeInsert(INSERT_STATEMENT, insertParameters)
          
        audit.rowsInserted += targetSql.updateCount
      }
      audit.measurementsQueried += targetSql.updateCount
    }
  }
                                               
  // ******************************************************
  // Method: dataLoadForStringFormatTimestampCell
  //
  // ******************************************************
  private void dataLoadForStringFormatTimestampCell(ProbeDefinition probeDefinition, Audit audit, Sql targetSql, List columnList) {
                                                      
    List queryParameters = [ audit.queryLowTime, audit.queryHighTime ]
    
    targetSql.eachRow(probeDefinition.sqlQuery, queryParameters) { GroovyResultSet groovyResultSet->
                                               
      for (int j=0; j<probeDefinition.mapping.size(); j++) {
        // *****************
        // insertParameters
        // *****************
        List insertParameters =
          [
           audit.dataLoadID,
           groovyResultSet."$probeDefinition.timestampColumnName",
           probeDefinition.mapping.get(columnList.getAt(j)),
           groovyResultSet.getAt(columnList.getAt(j)) as String
          ]
                                                      
        targetSql.executeInsert(INSERT_STATEMENT, insertParameters)
                                                
        audit.rowsInserted += targetSql.updateCount
      }
      audit.measurementsQueried += targetSql.updateCount
    }
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