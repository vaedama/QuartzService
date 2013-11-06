package com.caiso.fit.quartzService.util

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.apache.log4j.PatternLayout

// ******************************************************
// Class: Log
//
// ******************************************************
public class Log {
  // *************************************************
  // Constructor
  //
  // *************************************************
  private Log() {}

  // ************************
  // log method shortcuts
  // ************************
  public static debug(Object... messages) { debug(null, messages) }
  public static error(Object... messages) { error(null, messages) }
  public static fatal(Object... messages) { fatal(null, messages) }
  public static info (Object... messages) { info (null, messages) }
  public static trace(Object... messages) { trace(null, messages) }
  public static warn (Object... messages) { warn (null, messages) }
  
  public static debug(Throwable t, Object... messages) { log(Level.DEBUG, t, messages) }
  public static error(Throwable t, Object... messages) { log(Level.ERROR, t, messages) }
  public static fatal(Throwable t, Object... messages) { log(Level.FATAL, t, messages) }
  public static info (Throwable t, Object... messages) { log(Level.INFO,  t, messages) }
  public static trace(Throwable t, Object... messages) { log(Level.TRACE, t, messages) }
  public static warn (Throwable t, Object... messages) { log(Level.WARN,  t, messages) }
  
  // *****************************
  // setLevel method shortcuts
  // *****************************
  public static setLevelDebug() { setLevel(Level.DEBUG) }
  public static setLevelError() { setLevel(Level.ERROR) }
  public static setLevelFatal() { setLevel(Level.FATAL) }
  public static setLevelInfo () { setLevel(Level.INFO)  }
  public static setLevelTrace() { setLevel(Level.TRACE) }
  public static setLevelWarn () { setLevel(Level.WARN)  }

  // *************************************************
  // Method: log
  //
  // *************************************************
  private static log(Level level, Throwable t, Object... messages) {
  if (messages) {
    Logger logger = getLogger()
    
    if (logger.isEnabledFor(level)) {
    logger."${level.toString().toLowerCase()}" (messages.join(), t)
    }
  }
  }
  
  // *************************************************
  // Method: getLogger
  //
  // *************************************************
  private static Logger getLogger() {
  String callingClassName  = null
  String callingMethodName = null
  
  for (stackTraceEntry in Thread.currentThread().stackTrace) {
    if ((stackTraceEntry =~ /.*groovy:.*/) && !(stackTraceEntry =~ /.*Log.groovy:.*/)) {
    callingClassName  = stackTraceEntry.className
    callingMethodName = stackTraceEntry.methodName
    break
    }
  }
  
  if (callingClassName && callingClassName.contains('$')) {
    callingClassName = callingClassName.substring(0, callingClassName.indexOf('$'))
  }
  
  return Logger.getLogger(callingClassName)
  }

  // *************************************************
  // Method: setLevel
  //
  // *************************************************
  private static setLevel(Level level) { LogManager.rootLogger.level = level }
}
