package com.caiso.fit.quartzService.impl.probeImpl

import org.apache.poi.ss.usermodel.Sheet

// ******************************************************
// Class: ProbeDefinition
//
// ******************************************************
public class ProbeDefinition {
  String  probeName
  String  probeType
  Integer probeId
  String  probeDescription
  String  sourceJndiName
  Integer estimateFlag
  Integer rowsExpected
  Integer offset
  Integer dataInterval
  String  sqlQuery
  String  sourceSpreadsheetName
  Integer startRow
  Integer endRow
  Integer sheetNumber
  String  timestampColumnName
  String  timestampColumnFormat
  Integer timeStampColumnNumber
  Map     mapping
  String  renamedSpreadsheet
  Sheet   sheet
  String  timeZone
}