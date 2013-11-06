package com.caiso.fit.quartzService.validations

import com.caiso.fit.quartzService.util.Log

// ******************************************************
// Class: RepeatIntervalValidator
//
// ******************************************************
public class RepeatIntervalValidator {
  // ******************************************************
  // Method: validate
  //
  // ******************************************************
  public Integer validate(Integer repeatInterval){
    if (!repeatInterval) {
      Log.error "repeat Interval not specified"
    }
    else {
      if(repeatInterval > 1 && repeatInterval < 1440) {
        Log.info "Given Repeat Interval: $repeatInterval has been accepted"
      }

      else {
        Log.error "Repeat interval: $repeatInterval not in the integer range [1 - 1440]"

        throw new Exception("Enter a a Repeat Interval between the integer range [1 -1440]")

        return
      }
    }

    return repeatInterval
  }
}
