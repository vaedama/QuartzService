package com.caiso.fit.quartzService.util.probeUtil

// ******************************************************
// Class: ColumnConverter
//
// ******************************************************
public class ColumnConverter {
  // ******************************************************
  // Method: convertColumnLetters
  //
  // ******************************************************
  public int convertColumnLetters(String columnLetter) {
    int    result    = 0
    char[] charArray = columnLetter.toCharArray()

    for (int count = 0; count < charArray.length; count++) {
      int columnValue = 26 << charArray.length - count - 2
      int unitValue   = (int) columnLetter.charAt(count) - 64
      columnValue     = (columnValue == 0) ? 1 : columnValue
      result         += (unitValue * columnValue)
    }

    return result
  }
}