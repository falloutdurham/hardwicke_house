package com.lucidworks.hardwickehouse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class BackupPropertiesParserTest {
    
    @TempDir
    Path tempDir;
    
    @Test
    void testParseValidBackupProperties() throws IOException {
        BackupPropertiesParser parser = new BackupPropertiesParser();
        
        String propertiesContent = """
            #Backup properties file
            #Sun Aug 24 00:08:31 UTC 2025
            indexFileCount=269
            shard2.md=md_shard2_606.json
            collection.configName=Amex_UK
            shard1.md=md_shard1_606.json
            indexVersion=9.10.0
            collection=Amex_UK
            indexSizeMB=170.64600000000002
            collectionAlias=Amex_UK
            startTime=2025-08-24T00\\:08\\:16.343706101Z
            endTime=2025-08-24T00\\:08\\:31.278536080Z
            backupName=prd/Amex_UK
            """;
        
        Path propertiesFile = tempDir.resolve("backup.properties");
        Files.writeString(propertiesFile, propertiesContent);
        
        BackupMetadata metadata = parser.parseBackupProperties(propertiesFile);
        
        assertEquals("Amex_UK", metadata.getCollection());
        assertEquals("Amex_UK", metadata.getCollectionAlias());
        assertEquals("Amex_UK", metadata.getConfigName());
        assertEquals("prd/Amex_UK", metadata.getBackupName());
        assertEquals("9.10.0", metadata.getIndexVersion());
        assertEquals(269, metadata.getIndexFileCount());
        assertEquals(170.64600000000002, metadata.getIndexSizeMB(), 0.001);
        assertEquals(2, metadata.getShardCount());
        
        assertTrue(metadata.getShardMetadataFiles().contains("md_shard1_606.json"));
        assertTrue(metadata.getShardMetadataFiles().contains("md_shard2_606.json"));
        
        assertNotNull(metadata.getStartTime());
        assertNotNull(metadata.getEndTime());
        assertTrue(metadata.getEndTime().isAfter(metadata.getStartTime()));
    }
    
    @Test
    void testParsePropertiesWithMissingFields() throws IOException {
        BackupPropertiesParser parser = new BackupPropertiesParser();
        
        String propertiesContent = """
            collection=TestCollection
            indexFileCount=100
            shard1.md=md_shard1.json
            """;
        
        Path propertiesFile = tempDir.resolve("incomplete.properties");
        Files.writeString(propertiesFile, propertiesContent);
        
        BackupMetadata metadata = parser.parseBackupProperties(propertiesFile);
        
        assertEquals("TestCollection", metadata.getCollection());
        assertEquals(100, metadata.getIndexFileCount());
        assertEquals(0.0, metadata.getIndexSizeMB());
        assertEquals(1, metadata.getShardCount());
        assertNull(metadata.getStartTime());
        assertNull(metadata.getEndTime());
    }
    
    @Test
    void testParseSingleShardBackup() throws IOException {
        BackupPropertiesParser parser = new BackupPropertiesParser();
        
        String propertiesContent = """
            collection=SingleShard
            shard1.md=md_shard1.json
            indexFileCount=50
            """;
        
        Path propertiesFile = tempDir.resolve("single.properties");
        Files.writeString(propertiesFile, propertiesContent);
        
        BackupMetadata metadata = parser.parseBackupProperties(propertiesFile);
        
        assertEquals(1, metadata.getShardCount());
        assertEquals("md_shard1.json", metadata.getShardMetadataFiles().get(0));
    }
}