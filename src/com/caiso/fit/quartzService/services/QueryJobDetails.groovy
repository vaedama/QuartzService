package com.caiso.fit.quartzService.services

import com.caiso.fit.quartzService.common.QuartzServiceException
import com.caiso.fit.quartzService.impl.jobImpl.QuartzJobDetails

import com.caiso.fit.quartzService.util.Log

import javax.naming.InitialContext

import org.quartz.CronTrigger
import org.quartz.JobDetail
import org.quartz.JobKey
import org.quartz.Trigger
import org.quartz.TriggerKey

import org.quartz.impl.StdScheduler

import org.quartz.impl.matchers.GroupMatcher

import static org.quartz.JobKey.DEFAULT_GROUP

// ******************************************************
// Class: QueryJobDetails
//
// ******************************************************
public class QueryJobDetails {
  // ******************************************************
  // Method: queryJob
  //
  // ******************************************************
  public List<QuartzJobDetails> queryJob(String jobName) {
    try {
      return internalQueryJob (jobName)
    } catch (Throwable t) {
      new QuartzServiceException().throwException(t)
    }
  }
  
  // ******************************************************
  // Method: internalQueryJob
  //
  // ******************************************************
  private List<QuartzJobDetails> internalQueryJob(String jobName) {
    StdScheduler scheduler = (StdScheduler) new InitialContext().lookup('FitScheduler')

    Log.info "Querying the Job: $jobName"

    List<QuartzJobDetails> getList = new ArrayList()

    JobKey     jobKey     = new JobKey    (jobName, DEFAULT_GROUP)
    TriggerKey triggerKey = new TriggerKey(jobName + 'Trigger', DEFAULT_GROUP)

    JobDetail   jobDetail = scheduler.getJobDetail(jobKey)
    CronTrigger trigger   = scheduler?.getTrigger (triggerKey)
    
    if (!jobDetail) {
      Log.error "Job $jobName NOT found"

      throw new Exception("Job with JOBNAME $jobName is not found, returning empty list")
    }

    else{
      QuartzJobDetails quartzJobDetails = new QuartzJobDetails(jobName  : jobDetail.key.name,
                                                               jobGroup : jobDetail.key.group,
                                                               jobClass : jobDetail.jobClass.name
                                                              )

      if (trigger) {
        quartzJobDetails.triggerName    = trigger.key?.name
        quartzJobDetails.triggerGroup   = trigger.key?.group
        quartzJobDetails.startDate      = trigger.startTime?.format('yyyyMMdd')
        quartzJobDetails.startTime      = trigger.startTime?.format('HHmm')
        quartzJobDetails.repeatInterval = Character.getNumericValue(trigger?.cronExpression?.charAt(4))
        quartzJobDetails.endDate        = trigger.endTime?.format('yyyyMMdd')
        quartzJobDetails.endTime        = trigger.endTime?.format('HHmm')
        quartzJobDetails.status         = scheduler.getTriggerState(trigger.key)
      }

      getList.add(quartzJobDetails)
    }

    Log.info "Successfully Queried the Job: $jobName"

    return getList
  }
}