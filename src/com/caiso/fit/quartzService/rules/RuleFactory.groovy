package com.caiso.fit.quartzService.rules

import groovy.sql.GroovyRowResult
import groovy.sql.Sql

import javax.naming.InitialContext
import javax.sql.DataSource

import com.caiso.fit.quartzService.impl.probeImpl.Audit
import com.caiso.fit.quartzService.impl.probeImpl.Probe
import com.caiso.fit.quartzService.impl.probeImpl.Rule
import com.caiso.fit.quartzService.impl.probeImpl.RuleAudit;
import com.caiso.fit.quartzService.util.Log

// ******************************************************
// Class: RuleFactory
//
// ******************************************************
public class RuleFactory {
  // ***********
  // Constants
  // ***********
  private static final String FIT_CONFIG_JNDI = 'FIT_CONFIG_TEST'

  public void executeRules(Audit audit) {
    try {
      Log.info "Entered RuleFactory..."

      List ruleList = new KnowledgeBase().getRulesForProbe(audit.dataProbeName)

      if (ruleList.size() == 0) {
        Log.info("No Rules have been configured for the probe");
        
        return
      }
      else {
        for (int i = 0; i < ruleList?.size(); i++) {
          Log.info "Total Number of rules = $ruleList.size"
        
          RuleAudit ruleAudit = new RuleAudit()
          
          ruleAudit.ruleLoadID        = getSql(FIT_CONFIG_JNDI).firstRow("select RULE_LOAD_ID_SEQ.nextval as RULE_LOAD_ID from dual")?.RULE_LOAD_ID as Integer
          ruleAudit.ruleID            = ruleList.get(i).getAt("ruleID")
          ruleAudit.ruleExecutionTime = new Date(System.currentTimeMillis()).toString()
          ruleAudit.probeName         = audit.dataProbeName
          
          if (ruleList.get(i).getAt("ruleType").equals("NULL_CHECK")) {
            fireNullCheckRule(ruleList, i, ruleAudit, audit)
          }
          else if (ruleList.get(i).getAt("ruleType").equals("RANGE_CHECK")){
            fireRangeCheckRule(ruleList, i, ruleAudit, audit)
          }
          else {
            fireRuleScript(ruleList, i, ruleAudit, audit)
          }
        }
      }
    }
    catch (Throwable t) {
      Log.error "Error occurred: $t"

      StringWriter stringWriter = new StringWriter()
      t.printStackTrace(new PrintWriter(stringWriter))

      Log.debug stringWriter.toString()

      getSql(FIT_CONFIG_JNDI)?.connection?.rollback()
    }
    finally {
      getSql(FIT_CONFIG_JNDI)?.connection?.commit()
      getSql(FIT_CONFIG_JNDI)?.connection?.close()
    }
  }
  
  // ******************************************************
  // Method: fireNullCheckRule
  //
  // ******************************************************
  private void fireNullCheckRule(List ruleList, Integer i, RuleAudit ruleAudit, Audit audit) {
    String  attributeName   = ruleList.get(i).getAt("attributeName") as String
    Integer attributeNumber = getSql(FIT_CONFIG_JNDI).firstRow("select TARGET_ATTRIBUTE_ID from FIT_COLUMN_CONFIG where PROBE_NAME_MAPPING_FOR_COLS = $audit.dataProbeName and SOURCE_COLUMN = '$attributeName'").TARGET_ATTRIBUTE_ID as Integer
    List    queryParameters = []
    String  queryStatement = "select attribute_value from fit_data where data_load_id = ? and attribute_id = ?"
    
    queryParameters = [audit.dataLoadID, attributeNumber]
    
    if (!getSql(FIT_CONFIG_JNDI).firstRow(queryStatement, queryParameters)) {
      Log.info "Rule with rule load id: $ruleAudit.ruleLoadID Failed!"
      ruleAudit.ruleExecutionStatus = 0
      ruleAudit.executionNotes      = "$attributeName value loaded is NULL"
    }
    else {
       Log.info "Rule with rule load id: $ruleAudit.ruleLoadID Succeeded!"
       ruleAudit.ruleExecutionStatus = 1
       ruleAudit.executionNotes      = "$attributeName value loaded is NOT null"
    }
    saveRuleAudit(ruleAudit)
  }
  
  // ******************************************************
  // Method: fireRangeCheckRule
  //
  // ******************************************************
  private void fireRangeCheckRule(List ruleList, Integer i, RuleAudit ruleAudit, Audit audit) {
    String  attributeName   = ruleList.get(i).getAt("attributeName") as String
    Integer minValue        = ruleList.get(i).getAt("minValue") as Integer
    Integer maxValue        = ruleList.get(i).getAt("maxValue") as Integer
    Integer attributeNumber = getSql(FIT_CONFIG_JNDI).firstRow("select TARGET_ATTRIBUTE_ID from FIT_COLUMN_CONFIG where PROBE_NAME_MAPPING_FOR_COLS = $audit.dataProbeName and SOURCE_COLUMN = '$attributeName'").TARGET_ATTRIBUTE_ID as Integer
    List    queryParameters = []
    
    String  queryStatement = "select ATTRIBUTE_VALUE from FIT_DATA where DATA_LOAD_ID = ? and ATTRIBUTE_ID = ?"
    
    queryParameters = [audit.dataLoadID, attributeNumber]
    
    Integer atributeValue = 0
    
    try {
      atributeValue = Integer.parseInt(getSql(FIT_CONFIG_JNDI).firstRow(queryStatement, queryParameters)?.ATTRIBUTE_VALUE as String)
    } catch (NumberFormatException npe) {
      Log.error "Error occurred: $npe"
      
      StringWriter stringWriter = new StringWriter()
      npe.printStackTrace(new PrintWriter(stringWriter))
      
      Log.debug stringWriter.toString()
    }
   
    if (atributeValue < minValue || atributeValue > maxValue) {
      Log.info "Rule with rule load id: $ruleAudit.ruleLoadID Failed!"
      ruleAudit.ruleExecutionStatus = 0
      ruleAudit.executionNotes      = "$attributeName value loaded is NOT in the range of minimum value: $minValue and maximum value: $maxValue"
    }
    else {
       Log.info "Rule with rule load id: $ruleAudit.ruleLoadID Succeeded!"
       ruleAudit.ruleExecutionStatus = 1
       ruleAudit.executionNotes      = "$attributeName value loaded is in the range of minimum value: $minValue and maximum value: $maxValue"
    }
    saveRuleAudit(ruleAudit)
  }
  
  // ******************************************************
  // Method: fireRuleScript
  //
  // ******************************************************
  private void fireRuleScript(List ruleList, Integer i, RuleAudit ruleAudit, Audit audit) {
    String ruleText = ruleList.get(i)?.getAt("ruleScript") as String
  }
  
  // ******************************************************
  // Method: saveRuleAudit
  //
  // ******************************************************
  private List<Rule> saveRuleAudit(RuleAudit ruleAudit) {
    String insertStatement = """\
      insert into RULES_AUDIT (RULE_LOAD_ID,                    RULE_ID,
                               PROBE_NAME,                      RULE_EXEC_NOTES,
                               RULE_EXEC_STATUS,                RULE_EXEC_TIME) values
                              ($ruleAudit.ruleLoadID,           $ruleAudit.ruleID,
                              '$ruleAudit.probeName',          '$ruleAudit.executionNotes',
                               $ruleAudit.ruleExecutionStatus, '$ruleAudit.ruleExecutionTime'
                              )"""

    getSql(FIT_CONFIG_JNDI).executeInsert(insertStatement)
  }
  
  // ******************************************************
  // Method: getSql
  //
  // ******************************************************
  private Sql getSql(String jndiName) {
    DataSource dataSource = (DataSource) new InitialContext().lookup("java:$jndiName")

    return (dataSource == null ? null : new Sql(dataSource))
  }
}
