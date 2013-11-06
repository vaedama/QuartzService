package com.caiso.fit.quartzService.validations

import com.caiso.fit.quartzService.services.QueryJobDetails
import com.caiso.fit.quartzService.util.Log

// ******************************************************
// Class: EndDateValidator
//
// ******************************************************
public class EndDateValidator {
  // ******************************************************
  // Method: validateEndDate
  //
  // ******************************************************
  public Date validate(String textEndDate,
                       String textEndTime,
                       String jobName) {
    if (!textEndDate) return new QueryJobDetails().internalQueryJob(jobName)

    if (textEndDate && !textEndTime) {
      Log.error "EndTIME path parameter is empty"

      throw new Exception("End Time cannot be EMPTY")
    }

    Date endDate     = new Date().parse('yyyyMMddHHmm', textEndDate + textEndTime)
    Date currentDate = new Date()
    currentDate.minutes = new Date().minutes + 2

    if (endDate <= currentDate) {
      Log.error "Expired EndDate: $endDate has been passed"

      throw new Exception("Expired Enddate: $endDate has been passed. Please pass a Future Date")
    }

    Log.info "EndDate: $endDate accepted"

    return endDate
  }
}
