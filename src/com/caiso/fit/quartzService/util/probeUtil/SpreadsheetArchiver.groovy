package com.caiso.fit.quartzService.util.probeUtil

import com.caiso.fit.quartzService.util.Log

// ******************************************************
// Class: SpreadsheetArchiver
//
// ******************************************************
public class SpreadsheetArchiver {
  // ******************************************************
  // Method: archiveXL
  //
  // ******************************************************
  public void archiveXL(String sourceName) {
    Log.info "Running Spreadsheet Archiver"
    
    int    dotPositionForSource       = sourceName.lastIndexOf(".")
    String sourceNameWithoutExtension = sourceName.substring(0, dotPositionForSource)
    File   parentDirectory            = new File(System.getProperty("user.dir")).getParentFile()

    new File("$parentDirectory/excel_stage").eachFileMatch(~/.*\.wrk/) { File file ->
      int    dotPositionForFile       = file.name.lastIndexOf(".")
      String fileNameWithoutExtension = sourceName.substring(0, dotPositionForSource)

      if (fileNameWithoutExtension.equalsIgnoreCase(sourceNameWithoutExtension)) {
        Log.info "Now archiving $file.name"
        
        File    fileTobeCopied   = new File("$parentDirectory/excel_stage/$file.name")
        File    excelArchiveFile = new File("$parentDirectory/excel_archive/$sourceName")
        boolean createNewFile    = excelArchiveFile.createNewFile()

        if (!createNewFile) {
          Log.info "Temp file with the same name has been created.. copying $file.name to excel_archive directory with name $sourceName"
        }
        else {
          Log.info "Overwriting existing file with same name.. copying $file.name to excel_archive directory"
        }

        BufferedReader reader = fileTobeCopied.newReader()
        
        excelArchiveFile.withWriter { writer ->
          writer << reader
        }
        
        reader.close()
        
        Log.info "Successfully archived $file.name in excel_archive directory"
      }
    }
  }
}