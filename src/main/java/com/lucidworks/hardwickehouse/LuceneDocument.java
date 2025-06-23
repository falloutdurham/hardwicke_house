package com.lucidworks.hardwickehouse;

import java.util.HashMap;
import java.util.Map;

public class LuceneDocument {
    
    private final Map<String, Object> fields;
    private final int docId;
    
    public LuceneDocument(int docId) {
        this.docId = docId;
        this.fields = new HashMap<>();
    }
    
    public void addField(String name, Object value) {
        fields.put(name, value);
    }
    
    public Map<String, Object> getFields() {
        return fields;
    }
    
    public int getDocId() {
        return docId;
    }
    
    public Object getField(String name) {
        return fields.get(name);
    }
    
    public boolean hasField(String name) {
        return fields.containsKey(name);
    }
    
    @Override
    public String toString() {
        return "LuceneDocument{docId=" + docId + ", fields=" + fields + "}";
    }
}