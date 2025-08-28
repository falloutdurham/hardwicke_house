package com.lucidworks.hardwickehouse;

import java.time.Instant;
import java.util.List;

public class BackupMetadata {
    
    private final String collection;
    private final String collectionAlias;
    private final String configName;
    private final String backupName;
    private final String indexVersion;
    private final int indexFileCount;
    private final double indexSizeMB;
    private final Instant startTime;
    private final Instant endTime;
    private final List<String> shardMetadataFiles;
    
    public BackupMetadata(String collection, String collectionAlias, String configName, 
                         String backupName, String indexVersion, int indexFileCount, 
                         double indexSizeMB, Instant startTime, Instant endTime,
                         List<String> shardMetadataFiles) {
        this.collection = collection;
        this.collectionAlias = collectionAlias;
        this.configName = configName;
        this.backupName = backupName;
        this.indexVersion = indexVersion;
        this.indexFileCount = indexFileCount;
        this.indexSizeMB = indexSizeMB;
        this.startTime = startTime;
        this.endTime = endTime;
        this.shardMetadataFiles = shardMetadataFiles;
    }
    
    public String getCollection() {
        return collection;
    }
    
    public String getCollectionAlias() {
        return collectionAlias;
    }
    
    public String getConfigName() {
        return configName;
    }
    
    public String getBackupName() {
        return backupName;
    }
    
    public String getIndexVersion() {
        return indexVersion;
    }
    
    public int getIndexFileCount() {
        return indexFileCount;
    }
    
    public double getIndexSizeMB() {
        return indexSizeMB;
    }
    
    public Instant getStartTime() {
        return startTime;
    }
    
    public Instant getEndTime() {
        return endTime;
    }
    
    public List<String> getShardMetadataFiles() {
        return shardMetadataFiles;
    }
    
    public int getShardCount() {
        return shardMetadataFiles.size();
    }
}