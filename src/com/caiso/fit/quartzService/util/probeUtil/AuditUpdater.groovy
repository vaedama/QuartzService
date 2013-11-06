package com.caiso.fit.quartzService.util.probeUtil

import com.caiso.fit.quartzService.impl.probeImpl.Audit
import com.caiso.fit.quartzService.impl.probeImpl.ProbeDefinition

import com.caiso.fit.quartzService.util.Log

// ******************************************************
// Class : AuditUpdater
//
// ******************************************************
public class AuditUpdater {
  // ******************************************************
  // Method : updateAudit
  //
  // ******************************************************
  public void updateAudit(ProbeDefinition probeDefinition, Audit audit) {
    
    if (audit.rowsInserted > 0) {
      Integer error = Math.abs(probeDefinition.rowsExpected - audit.rowsInserted)
    
      Integer errorPercentage = (error/audit.rowsInserted) * 100
    
      if (probeDefinition.estimateFlag == 1) {
        if (error == 0) {
          audit.successFlag = 'Success'
          audit.notes       = "Rows Inserted were exactly equal to the Estimated Rows. NOT an error Condition!"
        }
        else {
          audit.successFlag = 'Fail'    
          audit.notes = "Error Condition! Rows Inserted NOT equal to Estimated Rows. Error = $error; Error% = $errorPercentage"
        }
      }
      else {
        if (error == 0) {
          audit.successFlag = 'Success'
          audit.notes       = "Estimate defaulted to NULL. Rows Inserted were exactly equal to Rows Expected. NOT an error Condition!"
        }
        else {
          audit.successFlag = 'Success'
          audit.notes = "Estimate defaulted to NULL! Rows Inserted NOT equal to Estimated Rows. Error = $error; Error% = $errorPercentage"
        }
      }
    }
    else {
      audit.notes       = "NO ROWS were Inserted for the Data Load"
      audit.successFlag = 'Fail'
    }
  }
}