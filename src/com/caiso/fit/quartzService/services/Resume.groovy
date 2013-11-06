package com.caiso.fit.quartzService.services

import java.util.List;

import com.caiso.fit.quartzService.impl.jobImpl.QuartzJobDetails;
import com.caiso.fit.quartzService.util.Log

import javax.naming.InitialContext

import org.quartz.JobKey
import org.quartz.Trigger
import org.quartz.TriggerKey

import org.quartz.impl.StdScheduler

import static org.quartz.JobKey.DEFAULT_GROUP

// ******************************************************
// Class: Resume
//
// ******************************************************
public class Resume {
  // ******************************************************
  // Method: resumeJob
  //
  // ******************************************************
  public List<QuartzJobDetails> resumeJob(String jobName) {
    StdScheduler scheduler = (StdScheduler) new InitialContext().lookup('FitScheduler')

    Log.info "Resuming the job If Exists"

    JobKey     jobKey     = new JobKey    (jobName,             DEFAULT_GROUP)
    TriggerKey triggerKey = new TriggerKey(jobName + 'Trigger', DEFAULT_GROUP)
    Trigger    trigger    = scheduler.getTrigger(triggerKey)

    if (!scheduler.getJobDetail(jobKey) && !scheduler.getTrigger(triggerKey)){
      
      Log.info "Cannot resume the Job as the trigger/job does not exist."
      
      throw new Exception ("Trigger/Job is not found. Could NOT resume the Job!")
    }
    
    scheduler.resumeJob(jobKey)
      
    Log.info "Successfully resumed (un-paused) the job - $jobName!"
    
    return new QueryJobDetails().queryJob(jobName)
  }
}