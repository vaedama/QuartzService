package com.caiso.fit.quartzService

import java.util.List;

import com.caiso.fit.quartzService.common.QuartzServiceException

import com.caiso.fit.quartzService.impl.jobImpl.QuartzJobDetails

import com.caiso.fit.quartzService.services.Delete
import com.caiso.fit.quartzService.services.Pause
import com.caiso.fit.quartzService.services.QueryAllJobs
import com.caiso.fit.quartzService.services.QueryJobDetails
import com.caiso.fit.quartzService.services.Rerun
import com.caiso.fit.quartzService.services.Resume

import com.caiso.fit.quartzService.services.create.JobWithEndDate
import com.caiso.fit.quartzService.services.create.JobWithoutEndDate
import com.caiso.fit.quartzService.services.create.JobWithoutTrigger

import com.caiso.fit.quartzService.util.Log

import javax.ws.rs.Path
import javax.ws.rs.GET
import javax.ws.rs.Produces
import javax.ws.rs.PathParam

import javax.naming.InitialContext

import org.quartz.impl.StdScheduler

// ******************************************************
// Class: RestfulQuartzService
//
// ******************************************************
@Path('Jobs')
public class RestfulQuartzService {
  // ******************************************************
  // Method: allJobs
  //
  // ******************************************************
  @GET
  @Produces('application/xml')
  public List<QuartzJobDetails> allJobs() {
    
    return new QueryAllJobs().getAllJobs()
  }
 
  // ******************************************************
  // Method: clearAll
  //
  // ******************************************************
  @GET
  @Path('DeleteAll')
  @Produces ('application/xml')
  public String clearAll() {
    StdScheduler scheduler = (StdScheduler) new InitialContext().lookup('FitScheduler')
  
    Log.info "Trying to Clear $scheduler.schedulerName..."
  
    scheduler.clear()
  
    Log.info "$scheduler.schedulerName has been cleared. All the Jobs and triggers associated with this scheduler have been deleted!"
  
    return "Scheduler - $scheduler.schedulerName has been cleared"
  }
  
  // ******************************************************
  // Method: delete
  //
  // ******************************************************
  @GET
  @Path('Delete/{JOBNAME}')
  @Produces ('text/plain')
  public String delete(@PathParam('JOBNAME') String jobName) {
    try {
      return new Delete().deleteJob(jobName)
    } catch (Throwable t) {
      new QuartzServiceException().throwException(t)
    }
  }
  
  // ******************************************************
  // Method: createWithEndDate
  //
  // ******************************************************
  @GET
  @Path('Create/{JOBNAME}/{STARTDATE}/{STARTTIME}/{REPEATINTERVAL}/{ENDDATE}/{ENDTIME}')
  @Produces('application/xml')
  public List<QuartzJobDetails> createWithEndDate(@PathParam ('JOBNAME')        String  jobName,
                                                  @PathParam ('STARTDATE')      String  textStartDate,
                                                  @PathParam ('STARTTIME')      String  textStartTime,
                                                  @PathParam ('REPEATINTERVAL') Integer repeatInterval,
                                                  @PathParam ('ENDDATE')        String  textEndDate,
                                                  @PathParam ('ENDTIME')        String  textEndTime
                                                 ) {
    try {
      return new JobWithEndDate().create(jobName, textStartDate, textStartTime, repeatInterval, textEndDate, textEndTime)
    } catch (Throwable t) {
      new QuartzServiceException().throwException(t)
    }
  }
                                      
  // ******************************************************
  // Method: createWithNoEndDate
  //
  // ******************************************************
  @GET
  @Path('Create/{JOBNAME}/{STARTDATE}/{STARTTIME}/{REPEATINTERVAL}')
  @Produces('application/xml')
  public List<QuartzJobDetails> createWithNoEndDate(@PathParam ('JOBNAME')        String  jobName,
                                                    @PathParam ('STARTDATE')      String  textStartDate,
                                                    @PathParam ('STARTTIME')      String  textStartTime,
                                                    @PathParam ('REPEATINTERVAL') Integer repeatInterval,
                                                    @PathParam ('ENDDATE')        String  textEndDate     = ' ',
                                                    @PathParam ('ENDTIME')        String  textEndTime     = ' '
                                                   ) {
    try {
      return new JobWithoutEndDate().create(jobName, textStartDate, textStartTime, repeatInterval, ' ', ' ')
    } catch (Throwable t) {
      new QuartzServiceException().throwException(t)
    }
  }
  
  // ******************************************************
  // Method: createWithoutTrigger
  //
  // ******************************************************
  @GET
  @Path('Create/{JOBNAME}')
  @Produces('application/xml')
  public List<QuartzJobDetails> createWithoutTrigger(@PathParam ('JOBNAME') String jobName) {
    try {
      return new JobWithoutTrigger().create(jobName)
    } catch (Throwable t) {
      new QuartzServiceException().throwException(t)
    }
  }
  
  // ******************************************************
  // Method: rerun
  //
  // ******************************************************
  @GET
  @Path('Rerun/{ID}')
  @Produces('application/xml')
  public List<QuartzJobDetails> rerun(@PathParam ('ID') Integer id) {
    try {
      return new Rerun().rerunJob(id)
    } catch (Throwable t) {
      new QuartzServiceException().throwException(t)
    }
  }
  
  // ******************************************************
  // Method: pause
  //
  // ******************************************************
  @GET
  @Path('Stop/{JOBNAME}')
  @Produces ('application/xml')
  public List<QuartzJobDetails> pause(@PathParam('JOBNAME') String jobName) {
    try {
      return new Pause().pauseJob(jobName)
    } catch (Throwable t) {
      new QuartzServiceException().throwException(t)
    }
  }
  
  // ******************************************************
  // Method: resume
  //
  // ******************************************************
  @GET
  @Path('Start/{JOBNAME}')
  @Produces ('application/xml')
  public List<QuartzJobDetails> resume(@PathParam('JOBNAME') String jobName) {
    try {
      return new Resume().resumeJob(jobName)
    } catch (Throwable t) {
      new QuartzServiceException().throwException(t)
    }
  }
  
  // ******************************************************
  // Method: queryJob
  //
  // ******************************************************
  @GET
  @Path('Query/{JOBNAME}')
  @Produces ('application/xml')
  public List<QuartzJobDetails> queryJob(@PathParam('JOBNAME') String jobName) {
    try {
      return new QueryJobDetails().queryJob(jobName)
    } catch (Throwable t) {
      new QuartzServiceException().throwException(t)
    }
  }
}
