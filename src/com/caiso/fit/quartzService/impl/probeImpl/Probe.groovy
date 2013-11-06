package com.caiso.fit.quartzService.impl.probeImpl

// ******************************************************
// Class: Probe
//
// ******************************************************
public abstract class Probe implements Runnable {
  Audit           audit
  ProbeDefinition probeDefinition
  
  public abstract void run()
}
