package com.lucidworks.hardwickehouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class BackupPropertiesParser {
    
    private static final Logger logger = LoggerFactory.getLogger(BackupPropertiesParser.class);
    
    public BackupMetadata parseBackupProperties(Path propertiesPath) throws IOException {
        logger.info("Parsing backup properties file: {}", propertiesPath);
        
        Properties props = new Properties();
        try (InputStream input = Files.newInputStream(propertiesPath)) {
            props.load(input);
        }
        
        String collection = props.getProperty("collection");
        String collectionAlias = props.getProperty("collectionAlias");
        String configName = props.getProperty("collection.configName");
        String backupName = props.getProperty("backupName");
        String indexVersion = props.getProperty("indexVersion");
        
        int indexFileCount = Integer.parseInt(props.getProperty("indexFileCount", "0"));
        double indexSizeMB = Double.parseDouble(props.getProperty("indexSizeMB", "0.0"));
        
        Instant startTime = parseTimestamp(props.getProperty("startTime"));
        Instant endTime = parseTimestamp(props.getProperty("endTime"));
        
        List<String> shardMetadataFiles = extractShardMetadataFiles(props);
        
        BackupMetadata metadata = new BackupMetadata(
            collection, collectionAlias, configName, backupName, indexVersion,
            indexFileCount, indexSizeMB, startTime, endTime, shardMetadataFiles
        );
        
        logger.info("Parsed backup metadata: collection={}, shards={}, indexSize={}MB", 
                   collection, shardMetadataFiles.size(), indexSizeMB);
        
        return metadata;
    }
    
    private Instant parseTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isEmpty()) {
            return null;
        }
        
        try {
            return Instant.parse(timestamp.replace("\\:", ":"));
        } catch (Exception e) {
            logger.warn("Failed to parse timestamp: {}", timestamp, e);
            return null;
        }
    }
    
    private List<String> extractShardMetadataFiles(Properties props) {
        List<String> shardFiles = new ArrayList<>();
        
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("shard") && key.endsWith(".md")) {
                String filename = props.getProperty(key);
                if (filename != null && !filename.isEmpty()) {
                    shardFiles.add(filename);
                }
            }
        }
        
        shardFiles.sort(String::compareTo);
        return shardFiles;
    }
}