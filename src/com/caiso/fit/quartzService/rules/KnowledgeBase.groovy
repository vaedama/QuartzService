package com.caiso.fit.quartzService.rules

import groovy.sql.Sql

import java.util.List

import javax.naming.InitialContext

import javax.sql.DataSource

import com.caiso.fit.quartzService.impl.probeImpl.Rule
import com.caiso.fit.quartzService.util.Log

// ******************************************************
// Class: KnowledgeBase
//
// ******************************************************
public class KnowledgeBase {
  // ***********
  // Constants
  // ***********
  private static final String FIT_CONFIG_JNDI = 'FIT_CONFIG_TEST'
  
  // ******************************************************
  // Method: getRulesForProbe
  //
  // ******************************************************
  private List<Rule> getRulesForProbe(String probeName) {
    List ruleList = []

    Log.info "Creating Rule list for probe.."

    getSql(FIT_CONFIG_JNDI).rows("select * from RULES_CONFIG where PROBE_NAME = '$probeName'").each { row ->
      Rule rule = new Rule()

      rule.probeName       = probeName
      rule.ruleID          = row.RULE_ID
      rule.ruleName        = row.RULE_NAME
      rule.ruleType        = row.RULE_TYPE
      rule.ruleDescription = row?.RULE_DESCRIPTION
      rule.attributeName   = row?.ATTRIBUTE_NAME
      rule.minValue        = row?.MIN_VALUE
      rule.maxValue        = row?.MAX_VALUE
      rule.ruleScript      = row?.RULE_SCRIPT
      
      ruleList << rule
    }

    return ruleList
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
