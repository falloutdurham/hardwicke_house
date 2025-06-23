package com.lucidworks.hardwickehouse;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;

class JsonLSchemaTest {
    
    @Test
    void testSchemaCreation() {
        Map<String, FieldType> fieldTypes = new HashMap<>();
        fieldTypes.put("title", FieldType.STRING);
        fieldTypes.put("score", FieldType.DOUBLE);
        fieldTypes.put("count", FieldType.INTEGER);
        
        JsonLSchema schema = new JsonLSchema(fieldTypes);
        
        assertEquals(3, schema.getFieldCount());
        assertEquals(FieldType.STRING, schema.getFieldType("title"));
        assertEquals(FieldType.DOUBLE, schema.getFieldType("score"));
        assertEquals(FieldType.INTEGER, schema.getFieldType("count"));
    }
    
    @Test
    void testEmptySchema() {
        Map<String, FieldType> fieldTypes = new HashMap<>();
        JsonLSchema schema = new JsonLSchema(fieldTypes);
        
        assertEquals(0, schema.getFieldCount());
        assertNull(schema.getFieldType("nonexistent"));
    }
    
    @Test
    void testAllFieldTypes() {
        Map<String, FieldType> fieldTypes = new HashMap<>();
        fieldTypes.put("string_field", FieldType.STRING);
        fieldTypes.put("int_field", FieldType.INTEGER);
        fieldTypes.put("long_field", FieldType.LONG);
        fieldTypes.put("float_field", FieldType.FLOAT);
        fieldTypes.put("double_field", FieldType.DOUBLE);
        fieldTypes.put("binary_field", FieldType.BINARY);
        fieldTypes.put("boolean_field", FieldType.BOOLEAN);
        
        JsonLSchema schema = new JsonLSchema(fieldTypes);
        
        assertEquals(7, schema.getFieldCount());
        assertEquals(7, schema.getFieldNames().size());
        assertTrue(schema.getFieldNames().contains("string_field"));
        assertTrue(schema.getFieldNames().contains("boolean_field"));
    }
    
    @Test
    void testToString() {
        Map<String, FieldType> fieldTypes = new HashMap<>();
        fieldTypes.put("test", FieldType.STRING);
        
        JsonLSchema schema = new JsonLSchema(fieldTypes);
        String result = schema.toString();
        
        assertTrue(result.contains("fieldCount=1"));
        assertTrue(result.contains("test"));
    }
}