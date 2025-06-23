package com.lucidworks.hardwickehouse;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LuceneDocumentTest {
    
    @Test
    void testDocumentCreation() {
        LuceneDocument doc = new LuceneDocument(123);
        assertEquals(123, doc.getDocId());
        assertTrue(doc.getFields().isEmpty());
    }
    
    @Test
    void testAddField() {
        LuceneDocument doc = new LuceneDocument(1);
        doc.addField("title", "Test Document");
        doc.addField("content", "This is test content");
        doc.addField("score", 95.5);
        
        assertEquals("Test Document", doc.getField("title"));
        assertEquals("This is test content", doc.getField("content"));
        assertEquals(95.5, doc.getField("score"));
        assertEquals(3, doc.getFields().size());
    }
    
    @Test
    void testHasField() {
        LuceneDocument doc = new LuceneDocument(1);
        doc.addField("title", "Test");
        
        assertTrue(doc.hasField("title"));
        assertFalse(doc.hasField("nonexistent"));
    }
    
    @Test
    void testGetNonExistentField() {
        LuceneDocument doc = new LuceneDocument(1);
        assertNull(doc.getField("nonexistent"));
    }
    
    @Test
    void testToString() {
        LuceneDocument doc = new LuceneDocument(42);
        doc.addField("test", "value");
        
        String result = doc.toString();
        assertTrue(result.contains("42"));
        assertTrue(result.contains("test"));
        assertTrue(result.contains("value"));
    }
}