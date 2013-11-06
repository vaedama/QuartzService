package com.caiso.fit.quartzService.services.create

import com.caiso.fit.quartzService.impl.jobImpl.QuartzJobDetails

import com.caiso.fit.quartzService.services.QueryJobDetails
import com.caiso.fit.quartzService.validations.JobNameValidator

// ******************************************************
// Class: JobWithoutTrigger
//
// ******************************************************
public class JobWithoutTrigger { 
  // ******************************************************
  // Method: create
  //
  // ******************************************************
  private List<QuartzJobDetails> create(String  jobName) {
    new JobNameValidator().validate(jobName)

    new CreateJob().getJobDetail(jobName)
    
    return new QueryJobDetails().queryJob(jobName)
  }
}
