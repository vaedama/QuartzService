package com.caiso.fit.quartzService.common

import org.quartz.Job
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException

import com.caiso.fit.quartzService.factory.AuditFactory
import com.caiso.fit.quartzService.factory.ProbeDefinitionFactory
import com.caiso.fit.quartzService.factory.ProbeFactory
import com.caiso.fit.quartzService.impl.probeImpl.Audit
import com.caiso.fit.quartzService.impl.probeImpl.Probe
import com.caiso.fit.quartzService.impl.probeImpl.ProbeDefinition

import com.caiso.fit.quartzService.util.Log
import com.caiso.fit.quartzService.util.jobUtil.FitJobTaskAuditRunner

// ******************************************************
// Class: FitJobTask
//
// ******************************************************
public class FitJobTask implements Job {
  // ******************************************************
  // Constructor: FitJobTask
  //
  // Instances of Job must have a public no-argument
  // constructor for job initialization
  // ******************************************************
  public FitJobTask() {
  }
  
  // *******************************************************
  // Method: execute
  //
  // When the Quartz Scheduler determines that it's time to
  // fire a job, it instantiates the job class and invokes
  // the execute() method. The Scheduler calls the execute()
  // method with no expectations other than throw of
  // JobExecutionException if there's a problem with the job.
  // ********************************************************
  @Override public void execute(JobExecutionContext jobExecutionContext)
            throws JobExecutionException {
    try {
      Log.info "Entered execute method of FitJobTask class"
      
      executeInternal(jobExecutionContext)
    }
    catch (Throwable t) {
      Log.error "Error occurred: $t"
      
      StringWriter stringWriter = new StringWriter()   
      t.printStackTrace(new PrintWriter(stringWriter))
      
      Log.debug stringWriter.toString()
        
      new Audit().fitExceptions = stringWriter.toString()
    }
  }
            
  // ******************************************************
  // Method: executeInternal
  //
  // ******************************************************
  public void executeInternal(JobExecutionContext jobExecutionContext) {
    Log.info "Entered executeInternal method of FitJobTask class"
    
    Audit audit = new Audit()
   
    new FitJobTaskAuditRunner().auditFitJobTask(audit, jobExecutionContext)

    Log.info "Getting Probe Definition for Probe - $audit.dataProbeName from the Configuration"

    ProbeDefinition probeDefinition  = new ProbeDefinitionFactory().getDefinition(audit, jobExecutionContext)
    
    Log.info "Getting Probe for Type: $probeDefinition.probeName" 
    
    Probe probe           = new ProbeFactory().getProbe(probeDefinition.probeType)
    probe.audit           = audit
    probe.probeDefinition = probeDefinition
    
    Log.info "Calling RUN on probe"
    
    probe.run()
  
    Log.info "Now running Audit Factory... Saving Audit, Notes and FitExceptions"

    new AuditFactory().auditProbeRun(probeDefinition, audit)

    Log.info "Run completed for Data Load ID: $audit.dataLoadID!"
  }
}