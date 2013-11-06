package com.caiso.fit.quartzService.impl.probeImpl

// ******************************************************
// Class: Audit
//
// ******************************************************
public class Audit {
  Integer dataLoadID
  Integer deletedDataLoadID
  Integer replacementDataLoadID
  String  dataProbeName
  String  givenStartDate
  Integer givenHourStarting
  String  givenEndDate
  Integer givenHourEnding
  Integer probeID
  String  probeType
  String  probeDescription
  String  timestampColumnName
  Integer dataInterval
  Integer offset
  String  sourceName
  String  sourceQuery
  String  sourceJndiName
  String  targetJndiName
  String  configJndiName
  Long    fireTimeMillis
  String  runStartTime
  String  runEndTime
  Integer elapsedRunTimeMillis
  Integer rowsExpected
  Integer rowsQueried
  Integer measurementsQueried
  Integer rowsInserted
  Integer rowsDeleted
  Integer estimateFlag
  String  queryLowTime
  String  queryHighTime
  String  successFlag
  String  notes
  String  fitExceptions
}