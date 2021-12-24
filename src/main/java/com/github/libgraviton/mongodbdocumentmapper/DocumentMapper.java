package com.github.libgraviton.mongodbdocumentmapper;

import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

/*
 * The MIT License
 *
 * Copyright (c) 2021 Swisscom (Schweiz) AG, Dario Nuevo
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/**
 * DocumentMapper helps setting and getting values on MongoDB BSON Document instances using
 * an expression syntax with dot notation. See README.md.
 */
public class DocumentMapper {

  /**
   * Array Index Mode describes the behavior when using array indexes in expressions when setting
   * values.
   */
  public enum ArrayIndexMode {
    /**
     * ADDITIVE will always *add* to the specified list, basically ignoring the specified index.
     * this way, one will never have OutOfBounds errors, but you cannot overwrite values.
     */
    ADDITIVE,
    /**
     * EXPLICIT takes the array index literally, trying to set the index as given and throwing
     * an Exception if it's not possible.
     */
    EXPLICIT;
  }

  final private boolean setNulls;
  final private ArrayIndexMode arrayIndexMode;

  /**
   * Creates an instance with default behavior (does set null, array index ADDITIVE)
   */
  public DocumentMapper() {
    this.setNulls = true;
    this.arrayIndexMode = ArrayIndexMode.ADDITIVE;
  }

  /**
   * Creates an instance
   *
   * @param setNulls whether to ever set null values on the target object.
   */
  public DocumentMapper(boolean setNulls) {
    this(setNulls, ArrayIndexMode.ADDITIVE);
  }

  /**
   * Creates an instance
   *
   * @param setNulls whether to ever set null values on the target object.
   * @param arrayIndexMode array index mode to use, see documentation there.
   */
  public DocumentMapper(boolean setNulls, ArrayIndexMode arrayIndexMode) {
    this.setNulls = setNulls;
    this.arrayIndexMode = arrayIndexMode;
  }

  /**
   * Maps a value from a source instance to a target instance using an expression.
   *
   * @param source source instance
   * @param sourceExpr expression done on the source instance (value to be selected)
   * @param target target instance
   * @param targetExpr expression done on the target instance (value to be set)
   * @throws DocumentMapperException
   */
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

  /**
   * sets a value on an instance using an expression
   *
   * @param target target instance
   * @param targetExpr expression (where should the value be set)
   * @param value value to set
   * @throws DocumentMapperException
   */
  public void setValue(Document target, String targetExpr, Object value)
      throws DocumentMapperException {
    setValue(target, MapperUtils.explode(targetExpr), value);
  }

  /**
   * gets a value form an instance using an expression
   *
   * @param source source instance
   * @param sourceExpr expression (value to get)
   * @return
   * @throws DocumentMapperException
   */
  public Object getValue(Document source, String sourceExpr) throws DocumentMapperException {
    return getValue(source, MapperUtils.explode(sourceExpr));
  }

  /**
   * can accept multiple expressions, returns the first non null match if available
   *
   * @param source source instance
   * @param sourceExpr expressions to apply
   * @return
   */
  public Object getValue(Document source, String... sourceExpr) {
    Object value = null;
    for (String singleExpr : sourceExpr) {
      try {
        value = getValue(source, MapperUtils.explode(singleExpr));
      } catch (Throwable t) {
        // ignored..
      }
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  /**
   * actual logic function that executes an expression
   *
   * @param source source instance
   * @param nameParts splitted parts of the expression
   *
   * @return value
   * @throws DocumentMapperException
   */
  private Object getValue(Document source, List<String> nameParts) throws DocumentMapperException {
    if (nameParts == null || nameParts.isEmpty()) {
      return null;
    }

    if (nameParts.size() == 1) {
      return source.get(nameParts.get(0));
    }

    String currentKey = nameParts.get(0);
    String nextKey = nameParts.get(1);
    nameParts.remove(0);

    if (!source.containsKey(currentKey)) {
      return null;
    }

    /**
     * simple array list case (subobj.0) -> do it directly
     */
    if (nameParts.size() == 1 && MapperUtils.isInteger(nextKey)) {
      try {
        int arrayIndex = Integer.parseInt(nextKey);
        return source.getList(currentKey, Object.class).get(arrayIndex);
      } catch (Throwable t) {
        throw new DocumentMapperException(
            "Could not get index '" + nextKey + "' on list (is it a list?) with key '"
                + currentKey + "'", t);
      }
    }

    Document subObj;

    try {
      if (MapperUtils.isInteger(nextKey)) {
        int arrayIndex = Integer.parseInt(nextKey);
        nameParts.remove(0);
        subObj = source.getList(currentKey, Document.class).get(arrayIndex);
      } else {
        subObj = source.get(currentKey, Document.class);
      }
    } catch (Throwable t) {
      throw new DocumentMapperException(
          "Unable to get next Document instance in recusion for key '"+currentKey+"'",
          t
      );
    }

    if (subObj == null) {
      return null;
    }

    return getValue(subObj, nameParts);
  }

  /**
   * actual logic function that sets a value
   *
   * @param target target instance
   * @param nameParts splitted parts of the expression
   * @param value value to set
   *
   * @throws DocumentMapperException
   */
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
