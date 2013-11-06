package com.caiso.fit.quartzService.common

import com.caiso.fit.quartzService.util.Log

import javax.ws.rs.WebApplicationException

import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response

// ******************************************************
// Class: QuartzServiceException
//
// ******************************************************
public class QuartzServiceException {
  // ******************************************************
  // Method: throwException
  //
  // ******************************************************
  public void throwException(Throwable t) {
    Log.error "Exception occured - $t.message"
    
    throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(t.getMessage()).type('text/plain').build())
  }
}
