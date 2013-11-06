package com.caiso.fit.quartzService.services.create

import com.caiso.fit.quartzService.impl.jobImpl.QuartzJobDetails

import com.caiso.fit.quartzService.services.QueryJobDetails
import com.caiso.fit.quartzService.util.Log

import com.caiso.fit.quartzService.validations.JobNameValidator
import com.caiso.fit.quartzService.validations.RepeatIntervalValidator
import com.caiso.fit.quartzService.validations.StartDateValidator

import javax.naming.InitialContext

import org.quartz.CronTrigger
import org.quartz.JobDataMap

import org.quartz.impl.StdScheduler

import static org.quartz.JobKey.DEFAULT_GROUP

import static org.quartz.TriggerBuilder.newTrigger

import static org.quartz.CronScheduleBuilder.cronSchedule

// ******************************************************
// Class: JobWithoutEndDate
//
// ******************************************************
public class JobWithoutEndDate {
  // ******************************************************
  // Method: create
  //
  // ******************************************************
  public List<QuartzJobDetails> create(String  jobName,
                                       String  textStartDate,
                                       String  textStartTime,
                                       Integer repeatInterval,
                                       String  textEndDate    = ' ',
                                       String  textEndTime    = ' '
                                      ) {

    new JobNameValidator().validate(jobName)
    
    new CreateJob().getJobDetail(jobName)

    Date startDate = new StartDateValidator().validate(textStartDate, textStartTime, jobName)

    repeatInterval = new RepeatIntervalValidator().validate(repeatInterval)

    JobDataMap jobDataMap = new JobDataMap()
    jobDataMap.put('JobName', jobName)

    String cronExp = "0 0/$repeatInterval * * * ?"

    CronTrigger trigger = newTrigger()
                         .withIdentity (jobName + 'Trigger', DEFAULT_GROUP)
                         .usingJobData (jobDataMap)
                         .startAt      (startDate)
                         .withSchedule (cronSchedule(cronExp))
                         .forJob       (jobName)
                         .build()

    trigger.timeZone = TimeZone.getTimeZone('GMT')

    StdScheduler scheduler = (StdScheduler) new InitialContext().lookup('FitScheduler')
    scheduler.scheduleJob(trigger)

    return new QueryJobDetails().queryJob(jobName)
  }
}
