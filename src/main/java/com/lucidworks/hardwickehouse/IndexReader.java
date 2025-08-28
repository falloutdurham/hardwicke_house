package com.lucidworks.hardwickehouse;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexReader {
    
    private static final Logger logger = LoggerFactory.getLogger(IndexReader.class);
    
    private org.apache.lucene.index.IndexReader reader;
    private List<DirectoryReader> shardReaders;
    private int currentDocIndex = 0;
    private JsonLSchema schema;
    private boolean isMultiShard = false;
    
    public void initialize(Path indexPath) throws IOException {
        logger.info("Opening Lucene index at: {}", indexPath);
        NIOFSDirectory directory = new NIOFSDirectory(indexPath);
        reader = DirectoryReader.open(directory);
        isMultiShard = false;
        
        logger.info("Index opened successfully. Total documents: {}", reader.numDocs());
        
        schema = inferSchema();
        logger.info("Schema inference completed. Found {} fields", schema.getFieldCount());
    }
    
    public void initializeMultiShard(List<Path> indexPaths) throws IOException {
        logger.info("Opening {} shard indexes", indexPaths.size());
        
        shardReaders = new ArrayList<>();
        List<DirectoryReader> readers = new ArrayList<>();
        
        for (Path indexPath : indexPaths) {
            logger.info("Opening shard index at: {}", indexPath);
            NIOFSDirectory directory = new NIOFSDirectory(indexPath);
            DirectoryReader shardReader = DirectoryReader.open(directory);
            shardReaders.add(shardReader);
            readers.add(shardReader);
        }
        
        reader = new MultiReader(readers.toArray(new org.apache.lucene.index.IndexReader[0]));
        isMultiShard = true;
        
        logger.info("Multi-shard index opened successfully. Total documents: {}", reader.numDocs());
        
        schema = inferSchema();
        logger.info("Schema inference completed. Found {} fields", schema.getFieldCount());
    }
    
    public long getTotalDocuments() {
        return reader.numDocs();
    }
    
    public JsonLSchema getSchema() {
        return schema;
    }
    
    public List<LuceneDocument> readBatch(int batchSize) throws IOException {
        List<LuceneDocument> batch = new ArrayList<>();
        
        for (int i = 0; i < batchSize && currentDocIndex < reader.numDocs(); i++) {
            Document luceneDoc = reader.document(currentDocIndex);
            LuceneDocument doc = convertDocument(luceneDoc, currentDocIndex);
            batch.add(doc);
            currentDocIndex++;
        }
        
        return batch;
    }
    
    private LuceneDocument convertDocument(Document luceneDoc, int docId) {
        LuceneDocument doc = new LuceneDocument(docId);
        
        for (IndexableField field : luceneDoc.getFields()) {
            String fieldName = field.name();
            Object value = getFieldValue(field);
            doc.addField(fieldName, value);
        }
        
        return doc;
    }
    
    private Object getFieldValue(IndexableField field) {
        if (field.numericValue() != null) {
            return field.numericValue();
        }
        
        if (field.binaryValue() != null) {
            return field.binaryValue().bytes;
        }
        
        return field.stringValue();
    }
    
    private JsonLSchema inferSchema() throws IOException {
        logger.info("Inferring schema from Lucene index...");
        
        Map<String, FieldType> fieldTypes = new HashMap<>();
        Set<String> allFieldNames = new HashSet<>();
        
        int sampleSize = Math.min(1000, reader.numDocs());
        logger.info("Sampling {} documents for schema inference", sampleSize);
        
        for (int i = 0; i < sampleSize; i++) {
            Document doc = reader.document(i);
            
            for (IndexableField field : doc.getFields()) {
                String fieldName = field.name();
                allFieldNames.add(fieldName);
                
                FieldType currentType = inferFieldType(field);
                FieldType existingType = fieldTypes.get(fieldName);
                
                if (existingType == null) {
                    fieldTypes.put(fieldName, currentType);
                } else if (existingType != currentType) {
                    fieldTypes.put(fieldName, FieldType.STRING);
                }
            }
        }
        
        logger.info("Schema inference found {} unique fields", allFieldNames.size());
        return new JsonLSchema(fieldTypes);
    }
    
    private FieldType inferFieldType(IndexableField field) {
        if (field.numericValue() != null) {
            Number num = field.numericValue();
            if (num instanceof Integer) {
                return FieldType.INTEGER;
            } else if (num instanceof Long) {
                return FieldType.LONG;
            } else if (num instanceof Float) {
                return FieldType.FLOAT;
            } else if (num instanceof Double) {
                return FieldType.DOUBLE;
            }
        }
        
        if (field.binaryValue() != null) {
            return FieldType.BINARY;
        }
        
        return FieldType.STRING;
    }
    
    public void close() throws IOException {
        if (isMultiShard && shardReaders != null) {
            for (DirectoryReader shardReader : shardReaders) {
                shardReader.close();
            }
            logger.info("Multi-shard Lucene index readers closed");
        }
        
        if (reader != null) {
            reader.close();
            logger.info("Lucene index reader closed");
        }
    }
}