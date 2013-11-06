package com.caiso.fit.quartzService.services

import com.caiso.fit.quartzService.impl.jobImpl.QuartzJobDetails;
import com.caiso.fit.quartzService.util.Log

import javax.naming.InitialContext

import org.quartz.JobDetail
import org.quartz.JobKey

import org.quartz.impl.StdScheduler

import static org.quartz.JobKey.DEFAULT_GROUP

// ******************************************************
// Class: Delete
//
// ******************************************************
public class Delete {
  // ******************************************************
  // Method: deleteJob
  //
  // Deleting Job automatically deletes the trigger
  // associated with that Job
  // ******************************************************
  public String deleteJob(String jobName) {
    StdScheduler scheduler = (StdScheduler) new InitialContext().lookup('FitScheduler')

    Log.info "Deleting Job: $jobName If Exists"

    JobKey    jobKey    = new JobKey(jobName, DEFAULT_GROUP)
    JobDetail jobDetail = scheduler.getJobDetail(jobKey)

    if (!jobDetail){
      Log.error "$jobName Does Not Exist"

      throw new Exception ("Job - $jobName does not exist")
    }

    String isDeleted = scheduler.deleteJob(jobKey).toString()

    Log.info "Job - $jobName deleted"
    
    return "Job - $jobName is deleted? $isDeleted"
  }
}
