package com.caiso.fit.quartzService.factory

import com.caiso.fit.quartzService.impl.probeImpl.Probe

import com.caiso.fit.quartzService.probeRunner.DatabaseToDatabaseProbe
import com.caiso.fit.quartzService.probeRunner.SpreadsheetToDatabaseProbe

import com.caiso.fit.quartzService.util.Log

// ******************************************************
// Class: ProbeFactory
//
// ******************************************************
public class ProbeFactory {
  // ************
  // Constants
  // ************
  private static final String DB_PROBE        = 'Database Probe'
  private static final String XL_PROBE        = 'Spreadsheet Probe'
  private static final String UNDEFINED_PROBE = 'Probe Type NOT Defined. Valid Probe Types are XL and DB only'

  // ******************************************************
  // Method: getProbe
  //
  // ******************************************************
  public Probe getProbe(String probeType) {
    if (probeType == DB_PROBE) {
      
      return new DatabaseToDatabaseProbe()
    }
    else if (probeType == XL_PROBE) {
      
      return new SpreadsheetToDatabaseProbe()
    }

    Log.error UNDEFINED_PROBE
    
    return null
  }
}