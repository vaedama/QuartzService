package com.caiso.fit.quartzService.services.create

import com.caiso.fit.quartzService.services.QueryJobDetails
import com.caiso.fit.quartzService.util.Log

import com.caiso.fit.quartzService.impl.jobImpl.QuartzJobDetails

import com.caiso.fit.quartzService.validations.EndDateValidator
import com.caiso.fit.quartzService.validations.JobNameValidator
import com.caiso.fit.quartzService.validations.RepeatIntervalValidator
import com.caiso.fit.quartzService.validations.StartDateValidator

import javax.naming.InitialContext

import org.quartz.CronTrigger
import org.quartz.JobDataMap

import org.quartz.impl.StdScheduler

import static org.quartz.TriggerBuilder.newTrigger

import static org.quartz.CronScheduleBuilder.cronSchedule

import static org.quartz.JobKey.DEFAULT_GROUP

// ******************************************************
// Class: JobWithEndDate
//
// ******************************************************
public class JobWithEndDate {
  // ******************************************************
  // Method: create
  //
  // ******************************************************
  public List<QuartzJobDetails> create(String  jobName,
                                       String  textStartDate,
                                       String  textStartTime,
                                       Integer repeatInterval,
                                       String  textEndDate,
                                       String  textEndTime
                                      ) {
    new JobNameValidator().validate(jobName)

    new CreateJob().getJobDetail(jobName)

    Date startDate = new StartDateValidator().validate(textStartDate, textStartTime, jobName)

    repeatInterval = new RepeatIntervalValidator().validate(repeatInterval)

    Date endDate   = new EndDateValidator().validate(textEndDate, textEndTime, jobName)

    String cronExpression = "0 0/$repeatInterval * * * ?"

    JobDataMap jobDataMap = new JobDataMap()
    jobDataMap.put('JobName', jobName)

    CronTrigger trigger = newTrigger()
                          .withIdentity (jobName + 'Trigger', DEFAULT_GROUP)
                          .startAt      (startDate)
                          .withSchedule (cronSchedule(cronExpression))
                          .usingJobData (jobDataMap)
                          .endAt        (endDate)
                          .forJob       (jobName)
                          .build()

    trigger.timeZone = TimeZone.getTimeZone('GMT')

    StdScheduler scheduler = (StdScheduler) new InitialContext().lookup('FitScheduler')
    scheduler.scheduleJob(trigger)

    Log.info "Job has been successfully scheduled!"

    return new QueryJobDetails().queryJob(jobName)
  }
}