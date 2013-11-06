package com.caiso.fit.quartzService.services

import com.caiso.fit.quartzService.impl.jobImpl.QuartzJobDetails;
import com.caiso.fit.quartzService.util.Log

import javax.naming.InitialContext

import org.quartz.JobKey
import org.quartz.Trigger
import org.quartz.TriggerKey

import org.quartz.impl.StdScheduler

import static org.quartz.JobKey.DEFAULT_GROUP

// ******************************************************
// Class: Pause
//
// ******************************************************
public class Pause {
  // ******************************************************
  // Method: pauseJob
  //
  // ******************************************************
  public List<QuartzJobDetails> pauseJob(String jobName) {
    StdScheduler scheduler = (StdScheduler) new InitialContext().lookup('FitScheduler')

    Log.info "Pausing the job - $jobName If Exists.."

    JobKey     jobKey     = new JobKey    (jobName,             DEFAULT_GROUP)
    TriggerKey triggerKey = new TriggerKey(jobName + 'Trigger', DEFAULT_GROUP)
    Trigger    trigger    = scheduler.getTrigger(triggerKey)

    if (!scheduler.getJobDetail(jobKey) && !scheduler.getTrigger(triggerKey)){
      Log.info "Cannot pause the Job as the trigger/job does not exist."
      
      throw new Exception ("Trigger/Job is not found. Could NOT pause the Job!")
    }
    
    scheduler.pauseJob(jobKey)
      
    Log.info "Successfully paused the job - $jobName!"
    
    return new QueryJobDetails().queryJob(jobName)
  }
}