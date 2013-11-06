package com.caiso.fit.quartzService.services.create

import com.caiso.fit.quartzService.util.Log
import com.caiso.fit.quartzService.common.FitJobTask

import javax.naming.InitialContext

import org.quartz.JobDataMap;
import org.quartz.JobDetail
import org.quartz.JobKey
import org.quartz.impl.StdScheduler

import static org.quartz.JobKey.DEFAULT_GROUP

import static org.quartz.JobBuilder.newJob

// ******************************************************
// Class: CreateJob
//
// ******************************************************
public class CreateJob {
  // ******************************************************
  // Method: getJobDetail
  //
  // Definition of Job Instance. Job is Stored DURABLY
  // but not scheduled. Durable Jobs do not get deleted
  // even if there is no trigger associated with them.
  // ******************************************************
  public JobDetail getJobDetail(String jobName){
    StdScheduler scheduler = (StdScheduler) new InitialContext().lookup('FitScheduler')
    
    JobKey jobKey = new JobKey(jobName, DEFAULT_GROUP)
    
    Log.info "$jobName does not exist! Trying to create $jobKey.name ..."
    
    JobDetail jobDetail = newJob(FitJobTask.class)
                         .withIdentity(jobName, DEFAULT_GROUP)
                         .storeDurably(true)
                         .build()

    if (!jobDetail){
      Log.error "Job not created. Check if the Scheduler is ready"
      
      throw new Exception ("Job with name $jobDetail.key.name not created. Check the Scheduler instantiation")
    }
    
    Log.info "Job has been successfully created with name $jobDetail.key.name"
    
    scheduler.addJob(jobDetail, false)
    
    return jobDetail
  }
}
