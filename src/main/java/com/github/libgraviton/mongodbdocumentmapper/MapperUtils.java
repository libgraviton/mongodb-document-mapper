package com.github.libgraviton.mongodbdocumentmapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

class MapperUtils {

  private static final Pattern isIntegerPattern = Pattern.compile("-?\\d+(\\.\\d+)?");

  public static List<String> explode(String expr) {
    return new ArrayList<>(Arrays.asList(expr.split("\\.")));
  }

  public static boolean isInteger(String expr) {
    return isIntegerPattern.matcher(expr).matches();
  }

}
