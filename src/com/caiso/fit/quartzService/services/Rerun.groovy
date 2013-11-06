package com.caiso.fit.quartzService.services

import com.caiso.fit.quartzService.impl.jobImpl.QuartzJobDetails

import com.caiso.fit.quartzService.common.QuartzServiceException

import com.caiso.fit.quartzService.util.Log

import javax.naming.InitialContext

import com.caiso.fit.quartzService.common.FitJobTask

import javax.ws.rs.Path
import javax.ws.rs.GET
import javax.ws.rs.Produces
import javax.ws.rs.PathParam

import org.quartz.CronTrigger
import org.quartz.JobDataMap
import org.quartz.JobDetail
import org.quartz.JobKey
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;

import org.quartz.impl.StdScheduler

import static org.quartz.JobBuilder.newJob

import static org.quartz.JobKey.DEFAULT_GROUP

import static org.quartz.TriggerBuilder.newTrigger

import static org.quartz.CronScheduleBuilder.cronSchedule
import static org.quartz.SimpleScheduleBuilder.simpleSchedule

// ******************************************************
// Class: Rerun
//
// ******************************************************
public class Rerun {
  // ******************************************************
  // Method: rerun
  //
  // ******************************************************
  public List<QuartzJobDetails> rerunJob(Integer id) {
    StdScheduler scheduler = (StdScheduler) new InitialContext().lookup('FitScheduler')
    
    JobKey jobKey = new JobKey('RERUN' + id, DEFAULT_GROUP)

    Log.info "Job: RERUN$id does not exist. Trying to create.."

    JobDetail jobDetail = newJob(FitJobTask.class)
                          .withIdentity(jobKey)
                          .storeDurably(false)
                          .build()

    if (!jobDetail){
      Log.error "Job not created. Check if the Scheduler is ready"

      throw new Exception ("Job with name $jobDetail.key.name not created. Check the Scheduler instantiation")
    }
    else {
      Log.info "Job has been successfully created with name $jobDetail.key.name"
    }

    JobDataMap jobDataMap = new JobDataMap()
    jobDataMap.put("JobName", "RERUN" + id)
    jobDataMap.put("DataLoadID", id)

    SimpleTrigger fireOnceTrigger = newTrigger()
                                    .withIdentity('RERUN' + id + 'TriggerRERUN', DEFAULT_GROUP)
                                    .startNow()
                                    .usingJobData(jobDataMap)
                                    .withSchedule(simpleSchedule().repeatSecondlyForTotalCount(1))
                                    .build()
                                    
    scheduler.scheduleJob(jobDetail, fireOnceTrigger)

    return new QueryJobDetails().queryJob('RERUN' + id)
  }
}
