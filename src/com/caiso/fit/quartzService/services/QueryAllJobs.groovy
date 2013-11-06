package com.caiso.fit.quartzService.services

import java.util.List;

import com.caiso.fit.quartzService.impl.jobImpl.QuartzJobDetails
import com.caiso.fit.quartzService.util.Log
import javax.naming.InitialContext
import org.quartz.CronTrigger
import org.quartz.JobDetail
import org.quartz.JobKey
import org.quartz.Trigger
import org.quartz.impl.StdScheduler

import org.quartz.impl.matchers.GroupMatcher

import static org.quartz.JobKey.DEFAULT_GROUP

// ******************************************************
// Class: QueryAllJobs
//
// ******************************************************
public class QueryAllJobs {
  // ******************************************************
  // Method: getAllJobs
  //
  // ******************************************************
  public List<QuartzJobDetails> getAllJobs() {
    StdScheduler scheduler = (StdScheduler) new InitialContext().lookup('FitScheduler')
  
    Log.info "Querying all jobs"

    List<QuartzJobDetails> allJobsList    = new ArrayList()
    Set                    jobKeySet      = scheduler.getJobKeys    (GroupMatcher.groupEquals(DEFAULT_GROUP))
    Set                    triggerKeySet  = scheduler.getTriggerKeys(GroupMatcher.groupEquals(DEFAULT_GROUP))
    Iterator               jobKeyIterator = jobKeySet.iterator()

    while (jobKeyIterator.hasNext()) {
      JobKey    jobKey    = (JobKey)jobKeyIterator?.next()
      JobDetail jobDetail = scheduler.getJobDetail(jobKey)

      if (!jobDetail) continue
    
      QuartzJobDetails quartzJobDetails = new QuartzJobDetails(jobName  : jobDetail.key.name,
                                                               jobGroup : jobDetail.key.group,
                                                               jobClass : jobDetail.jobClass.name
                                                              )
    
      List<Trigger> triggersOfJob = scheduler.getTriggersOfJob(jobKey)

      if (triggersOfJob) {
        CronTrigger trigger        = (CronTrigger)triggersOfJob.get(0)
        Integer     repeatInterval = Character.getNumericValue(trigger.cronExpression.charAt(4))
      
        if (trigger){
          quartzJobDetails.triggerName    = trigger.key?.name
          quartzJobDetails.triggerGroup   = trigger.key?.group
          quartzJobDetails.startDate      = trigger.startTime?.format('yyyyMMdd')
          quartzJobDetails.startTime      = trigger.startTime?.format('HHmm')
          quartzJobDetails.repeatInterval = repeatInterval
          quartzJobDetails.endDate        = trigger.endTime?.format('yyyyMMdd')
          quartzJobDetails.endTime        = trigger.endTime?.format('HHmm')
          quartzJobDetails.status         = scheduler.getTriggerState(trigger.key)
        }
      }
    
      allJobsList << quartzJobDetails
    }
    
    return allJobsList
  }
}