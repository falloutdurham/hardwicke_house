package com.lucidworks.hardwickehouse;

import java.util.Map;
import java.util.Set;

public class JsonLSchema {
    
    private final Map<String, FieldType> fieldTypes;
    
    public JsonLSchema(Map<String, FieldType> fieldTypes) {
        this.fieldTypes = fieldTypes;
    }
    
    public Map<String, FieldType> getFieldTypes() {
        return fieldTypes;
    }
    
    public int getFieldCount() {
        return fieldTypes.size();
    }
    
    public FieldType getFieldType(String fieldName) {
        return fieldTypes.get(fieldName);
    }
    
    public Set<String> getFieldNames() {
        return fieldTypes.keySet();
    }
    
    @Override
    public String toString() {
        return "JsonLSchema{" +
                "fieldCount=" + getFieldCount() +
                ", fields=" + fieldTypes.keySet() +
                '}';
    }
}