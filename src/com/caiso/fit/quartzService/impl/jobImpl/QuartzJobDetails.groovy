package com.caiso.fit.quartzService.impl.jobImpl

import javax.xml.bind.annotation.XmlAccessType
import javax.xml.bind.annotation.XmlAccessorType
import javax.xml.bind.annotation.XmlRootElement
import javax.xml.bind.annotation.XmlTransient

@XmlRootElement (name="job")
@XmlAccessorType(XmlAccessType.FIELD)

// ******************************************************
// Class: QuartzJobDetails
//
// ******************************************************
public class QuartzJobDetails {
  String  jobName
  String  startDate
  String  startTime
  String  endDate
  String  endTime
  Integer repeatInterval
  String  status
  
  @XmlTransient String jobGroup
  @XmlTransient Class  jobClass
  @XmlTransient String triggerName
  @XmlTransient String triggerGroup
}
