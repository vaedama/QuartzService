package com.caiso.fit.quartzService.validations

import com.caiso.fit.quartzService.util.Log

import javax.naming.InitialContext

import org.quartz.JobKey

import org.quartz.impl.StdScheduler

import static org.quartz.JobKey.DEFAULT_GROUP

// ******************************************************
// Class: JobNameValidator
//
// ******************************************************
public class JobNameValidator {
  // ******************************************************
  // Method: validate
  //
  // ******************************************************
  public validate(String jobName){
    StdScheduler scheduler = (StdScheduler) new InitialContext().lookup('FitScheduler')

    if (!jobName || !jobName?.trim()) {
      Log.error "Job name is MANDATORY, cannot be EMPTY or NULL - returning empty list"

      throw new Exception("Job name cannot be EMPTY")
    }

    JobKey jobKey = new JobKey(jobName, DEFAULT_GROUP)

    if (scheduler.checkExists(jobKey)) {
      Log.error "Cannot create $jobName as a Job already exists with the same name"

      throw new Exception("Job already exists with the same name")
    }
  }
}
