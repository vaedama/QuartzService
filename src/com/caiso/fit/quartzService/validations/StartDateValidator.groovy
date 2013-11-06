package com.caiso.fit.quartzService.validations

import com.caiso.fit.quartzService.util.Log

// ******************************************************
// Class: StartDateValidator
//
// ******************************************************
public class StartDateValidator {
  // ******************************************************
  // Method: validate
  //
  // ******************************************************
  public Date validate(String textStartDate,
                       String textStartTime,
                       String jobName) {
    if (textStartDate && !textStartTime) {
      Log.error "StartTime parameter HHmm cannot be empty"

      throw new Exception("Start Time cannot be EMPTY")
    }
    else {
      Date startDate      = new Date().parse('yyyyMMddHHmm', textStartDate + textStartTime)
      Date currentDate    = new Date()
      currentDate.minutes = new Date().minutes

      if (startDate <= currentDate){
        Log.error "StartDate $startDate has been Expired, Pass a Future Date"

        throw new Exception("Expired date has been passed. Please pass a Future Date")
      }
      else {
        Log.info "Start Date: $startDate is accepted"
      }

      return startDate
    }
  }
}
