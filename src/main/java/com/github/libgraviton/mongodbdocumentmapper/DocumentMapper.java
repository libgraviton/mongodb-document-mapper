package com.github.libgraviton.mongodbdocumentmapper;

import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

public class DocumentMapper {

  public enum ArrayIndexMode {
    ADDITIVE,
    EXPLICIT;
  }

  final private boolean setNulls;
  final private ArrayIndexMode arrayIndexMode;

  public DocumentMapper() {
    this.setNulls = true;
    this.arrayIndexMode = ArrayIndexMode.ADDITIVE;
  }

  public DocumentMapper(boolean setNulls) {
    this(setNulls, ArrayIndexMode.ADDITIVE);
  }

  public DocumentMapper(boolean setNulls, ArrayIndexMode arrayIndexMode) {
    this.setNulls = setNulls;
    this.arrayIndexMode = arrayIndexMode;
  }

  public void map(Document source, String sourceExpr, Document target, String targetExpr)
      throws DocumentMapperException {
    if (source == null || target == null) {
      return;
    }

    Object sourceValue = getValue(source, MapperUtils.explode(sourceExpr));

    if (!setNulls && sourceValue == null) {
      return;
    }

    setValue(target, MapperUtils.explode(targetExpr), sourceValue);
  }

  public void setValue(Document target, String targetExpr, Object value)
      throws DocumentMapperException {
    setValue(target, MapperUtils.explode(targetExpr), value);
  }

  public Object getValue(Document source, String sourceExpr) {
    return getValue(source, MapperUtils.explode(sourceExpr));
  }

  /**
   * can accept multiple exprs, returns the first non null match if available
   *
   * @param source
   * @param sourceExpr
   * @return
   */
  public Object getValue(Document source, String... sourceExpr) {
    Object value;
    for (String singleExpr : sourceExpr) {
      value = getValue(source, MapperUtils.explode(singleExpr));
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private Object getValue(Document source, List<String> nameParts) {
    if (nameParts.size() == 1) {
      return source.get(nameParts.get(0));
    }

    String firstName = nameParts.get(0);
    nameParts.remove(0);

    if (!source.containsKey(firstName)) {
      return null;
    }

    Document subObj = source.get(firstName, Document.class);
    if (subObj == null) {
      return null;
    }

    return getValue(subObj, nameParts);
  }

  private void setValue(Document target, List<String> nameParts, Object value)
      throws DocumentMapperException {
    if (nameParts.isEmpty() || (!setNulls && value == null)) {
      return;
    }

    if (nameParts.size() == 1) {
      target.put(nameParts.get(0), value);
      return;
    }

    String currentKey = nameParts.get(0);
    String nextKey = nameParts.get(1);
    nameParts.remove(0);

    /**
     * simple array list case (subobj.0) -> do it directly
     */
    if (nameParts.size() == 1 && MapperUtils.isInteger(nextKey)) {
      target.putIfAbsent(currentKey, new ArrayList<>());

      try {
        List<Object> currentList = target.getList(currentKey, Object.class);

        if (arrayIndexMode == ArrayIndexMode.ADDITIVE) {
          currentList.add(value);
        } else {
          int arrayIndex = Integer.parseInt(nextKey);
          if (arrayIndex+1 <= currentList.size()) {
            currentList.set(arrayIndex, value);
          } else {
            currentList.add(value);
          }
        }
      } catch (Throwable t) {
        throw new DocumentMapperException(
            "Could not set index '" + nextKey + "' on list (is it a list?) with key '"
                + currentKey + "'", t);
      }

      return;
    }

    Document baseObject = new Document();

    /**
     * list of objects case (subobj.0.subprop)
     */
    if (nameParts.size() > 1 && MapperUtils.isInteger(nextKey)) {
      // remove the integer from the list
      nameParts.remove(0);

      target.putIfAbsent(currentKey, new ArrayList<>());
      List<Object> currentList = target.getList(currentKey, Object.class);

      int arrayIndex = Integer.parseInt(nextKey);
      if (arrayIndexMode == ArrayIndexMode.ADDITIVE) {
        if (currentList.isEmpty()) {
          currentList.add(new Document());
          arrayIndex = 0;
        } else {
          arrayIndex = currentList.size();
          currentList.add(arrayIndex, new Document());
        }
      } else {
        if (currentList.size() -1 < arrayIndex) {
          currentList.add(new Document());
        }
      }

      if (currentList.get(arrayIndex) instanceof Document) {
        try {
          baseObject = target.getList(currentKey, Document.class).get(arrayIndex);
        } catch (Throwable t) {
          throw new DocumentMapperException("Could not set value on list. Either property '"+currentKey+"' is not a list or index '"+arrayIndex+"' does not exist.", t);
        }
      }
    } else {
      if (target.containsKey(currentKey)) {
        baseObject = target.get(currentKey, Document.class);
      }

      target.put(currentKey, baseObject);
    }

    setValue(baseObject, nameParts, value);

  }
}
