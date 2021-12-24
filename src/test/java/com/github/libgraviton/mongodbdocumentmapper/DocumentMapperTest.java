package com.github.libgraviton.mongodbdocumentmapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import com.github.libgraviton.mongodbdocumentmapper.DocumentMapper.ArrayIndexMode;
import java.util.List;
import org.bson.Document;
import org.junit.Test;

public class DocumentMapperTest {

  @Test
  public void testSetValueExplicit() throws DocumentMapperException {
    Document docB = new Document();
    DocumentMapper documentMapper = new DocumentMapper(true, ArrayIndexMode.EXPLICIT);
    documentMapper.setValue(docB, "objectList.0.subDude", "fred");
    documentMapper.setValue(docB, "objectList.1.subDude", "fred2");
    documentMapper.setValue(docB, "objectList.2.subDude", "fred3");
    // overwrite -> possible only in explicit
    documentMapper.setValue(docB, "objectList.1.subDude", "fred1");

    documentMapper.setValue(docB, "bagOfInts.0", 1);
    documentMapper.setValue(docB, "bagOfInts.0", 2);
    documentMapper.setValue(docB, "bagOfInts.1", 3);

    assertEquals(3, docB.getList("objectList", Document.class).size());
    assertEquals(docB.getList("objectList", Document.class).get(0), new Document("subDude", "fred"));
    assertEquals(docB.getList("objectList", Document.class).get(1), new Document("subDude", "fred1"));
    assertEquals(docB.getList("objectList", Document.class).get(2), new Document("subDude", "fred3"));

    assertEquals(2, docB.getList("bagOfInts", Integer.class).size());
    assertEquals(Integer.valueOf("2"), docB.getList("bagOfInts", Integer.class).get(0));
    assertEquals(Integer.valueOf("3"), docB.getList("bagOfInts", Integer.class).get(1));
  }

  @Test
  public void testSetValueAdditive() throws DocumentMapperException {
    Document docB = new Document();
    DocumentMapper documentMapper = new DocumentMapper(true, ArrayIndexMode.ADDITIVE);

    documentMapper.setValue(docB, "objectList.0.subDude", "fred");
    documentMapper.setValue(docB, "objectList.0.subDude", "fred2");
    documentMapper.setValue(docB, "objectList.0.subDude", "fred3");
    // overwrite -> possible only in explicit -> will add here
    documentMapper.setValue(docB, "objectList.0.subDude", "fred1");

    documentMapper.setValue(docB, "bagOfInts.0", 1);
    documentMapper.setValue(docB, "bagOfInts.0", 2);
    documentMapper.setValue(docB, "bagOfInts.0", 3);

    assertEquals(4, docB.getList("objectList", Document.class).size());
    assertEquals(docB.getList("objectList", Document.class).get(0), new Document("subDude", "fred"));
    assertEquals(docB.getList("objectList", Document.class).get(1), new Document("subDude", "fred2"));
    assertEquals(docB.getList("objectList", Document.class).get(2), new Document("subDude", "fred3"));
    assertEquals(docB.getList("objectList", Document.class).get(3), new Document("subDude", "fred1"));

    assertEquals(3, docB.getList("bagOfInts", Integer.class).size());
    assertEquals(Integer.valueOf("1"), docB.getList("bagOfInts", Integer.class).get(0));
    assertEquals(Integer.valueOf("2"), docB.getList("bagOfInts", Integer.class).get(1));
    assertEquals(Integer.valueOf("3"), docB.getList("bagOfInts", Integer.class).get(2));
  }

  @Test
  public void testSetValueExpressions() throws DocumentMapperException {
    Document docB = new Document();
    DocumentMapper documentMapper = new DocumentMapper();

    documentMapper.setValue(docB, "someProp.subkey.subkey2.subkey3.subkey4.0.subDude", "fred");
    documentMapper.setValue(docB, "anotherProp", "fred");
    documentMapper.setValue(docB, "anotherPropMore.sub", "fred");
    documentMapper.setValue(docB, "nullValue", null);

    Document complex = new Document(
        "subkey", new Document(
            "subkey2", new Document(
              "subkey3", new Document(
                "subkey4", List.of(new Document("subDude", "fred"))
              )
            )
        )
    );

    assertEquals(docB.get("someProp"), complex);

    assertEquals("fred", docB.getString("anotherProp"));
    assertEquals("fred", docB.get("anotherPropMore", Document.class).getString("sub"));

    assertEquals(null, docB.get("nullValue"));
  }

  @Test
  public void testSetValueNoNulls() throws DocumentMapperException {
    Document docB = new Document();
    DocumentMapper documentMapper = new DocumentMapper(false);

    documentMapper.setValue(docB, "test1", null);

    assertFalse(docB.containsKey("test1"));
  }

  @Test
  public void testGetValue() throws DocumentMapperException {
    Document complex = new Document(
        "subkey", new Document(
            "subkey2", new Document(
              "subkey3", new Document(
                "subkey4", List.of(new Document("subDude", "fred"))
              )
            )
        )
    );

    Document docB = new Document("someProp", complex);
    docB.put("docList", List.of(new Document("fred", "hans")));

    DocumentMapper documentMapper = new DocumentMapper();

    assertEquals("hans", documentMapper.getValue(docB, "docList.0.fred"));
    assertEquals("fred", documentMapper.getValue(docB, "someProp.subkey.subkey2.subkey3.subkey4.0.subDude"));
  }

  @Test
  public void testGetValueFirstNotNull() throws DocumentMapperException {
    Document docB = new Document("first", "key");

    DocumentMapper documentMapper = new DocumentMapper();

    Object value = documentMapper.getValue(docB, "not-existing-key", "first.0.key.whatever", "first");
    Object nothingMatches = documentMapper.getValue(docB, "not-existing-key", "first.0.key.whatever");

    assertEquals("key", value);
    assertNull(nothingMatches);
  }

  @Test
  public void testMappingFromOtherDocument() throws DocumentMapperException {

    DocumentMapper documentMapper = new DocumentMapper(true, ArrayIndexMode.EXPLICIT);

    Document docA = new Document();
    docA.put("simpleVal", "value");
    docA.put("embedded", new Document().append("subKey", "subVal").append("subInt", 33));

    Document docB = new Document();

    documentMapper.map(docA, "simpleVal", docB, "objectList.0.subDude");
    documentMapper.map(docA, "simpleVal", docB, "objectList.0.subDude");
    documentMapper.map(docA, "simpleVal", docB, "objectList.0.subDude");

    documentMapper.setValue(docB, "objectList.0.subDude", "fred");
    documentMapper.setValue(docB, "objectList.1.subDude", "fred2");

    documentMapper.map(docA, "simpleVal", docB, "arrayList.0");
    documentMapper.map(docA, "simpleVal", docB, "arrayList.0");
    documentMapper.map(docA, "simpleVal", docB, "arrayList.0");

    documentMapper.map(docA, "embedded.subKey", docB,"dude.theKeeee");
    documentMapper.map(docA,"embedded.subInt", docB, "dude.theKeeeeInt");
    documentMapper.map(docA,"embedded.subInt", docB, "dude.anotherOne.theKeeeeInt");
    documentMapper.setValue(docB, "otherObject.subK", Integer.valueOf(33));

    Document newDoc = new Document().append("id", "hans");
    documentMapper.map(newDoc, "id", docB, "anotherDoc.id");
    documentMapper.map(newDoc, "not-existing-key", docB, "anotherDoc.id2");

    assertEquals("subVal", docB.get("dude", Document.class).get("theKeeee"));
    assertEquals(33, docB.get("dude", Document.class).get("theKeeeeInt"));
    assertEquals(33, docB.get("dude", Document.class).get("anotherOne", Document.class).get("theKeeeeInt"));

    assertEquals(Integer.valueOf(33), docB.get("otherObject", Document.class).get("subK"));

    assertEquals("hans", docB.get("anotherDoc", Document.class).get("id"));
    assertNull(docB.get("anotherDoc", Document.class).get("id2"));
  }
}
