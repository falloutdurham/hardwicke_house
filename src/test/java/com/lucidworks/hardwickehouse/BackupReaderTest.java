package com.lucidworks.hardwickehouse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BackupReaderTest {
    
    @TempDir
    Path tempDir;
    
    @Test
    void testValidateBackupStructureValid() throws IOException {
        BackupReader reader = new BackupReader();
        
        Path metadataFile = tempDir.resolve("md_shard1.json");
        Files.writeString(metadataFile, "{\"indexDir\": \"shard1_data\"}");
        
        List<String> shardFiles = List.of("md_shard1.json");
        BackupMetadata metadata = new BackupMetadata("test", "test", "test", "test", 
                                                     "9.10.0", 100, 50.0, null, null, shardFiles);
        
        assertTrue(reader.validateBackupStructure(tempDir, metadata));
    }
    
    @Test
    void testValidateBackupStructureInvalidDirectory() {
        BackupReader reader = new BackupReader();
        Path nonExistentPath = tempDir.resolve("nonexistent");
        
        List<String> shardFiles = List.of("md_shard1.json");
        BackupMetadata metadata = new BackupMetadata("test", "test", "test", "test", 
                                                     "9.10.0", 100, 50.0, null, null, shardFiles);
        
        assertFalse(reader.validateBackupStructure(nonExistentPath, metadata));
    }
    
    @Test
    void testFindLuceneIndexPathsWithValidStructure() throws IOException {
        BackupReader reader = new BackupReader();
        
        Path shardDir = tempDir.resolve("shard1_data");
        Files.createDirectories(shardDir);
        Files.createFile(shardDir.resolve("segments_1"));
        
        String metadataContent = "{\"indexDir\": \"shard1_data\"}";
        Path metadataFile = tempDir.resolve("md_shard1.json");
        Files.writeString(metadataFile, metadataContent);
        
        List<String> shardFiles = List.of("md_shard1.json");
        BackupMetadata metadata = new BackupMetadata("test", "test", "test", "test", 
                                                     "9.10.0", 100, 50.0, null, null, shardFiles);
        
        List<Path> indexPaths = reader.findLuceneIndexPaths(tempDir, metadata);
        
        assertEquals(1, indexPaths.size());
        assertEquals(shardDir, indexPaths.get(0));
    }
    
    @Test
    void testFindLuceneIndexPathsWithFallback() throws IOException {
        BackupReader reader = new BackupReader();
        
        Path shardDir = tempDir.resolve("hidden_index");
        Files.createDirectories(shardDir);
        Files.createFile(shardDir.resolve("segments.gen"));
        
        String metadataContent = "{\"invalidField\": \"value\"}";
        Path metadataFile = tempDir.resolve("md_shard1.json");
        Files.writeString(metadataFile, metadataContent);
        
        List<String> shardFiles = List.of("md_shard1.json");
        BackupMetadata metadata = new BackupMetadata("test", "test", "test", "test", 
                                                     "9.10.0", 100, 50.0, null, null, shardFiles);
        
        List<Path> indexPaths = reader.findLuceneIndexPaths(tempDir, metadata);
        
        assertEquals(1, indexPaths.size());
        assertEquals(shardDir, indexPaths.get(0));
    }
    
    @Test
    void testFindLuceneIndexPathsEmpty() throws IOException {
        BackupReader reader = new BackupReader();
        
        List<String> shardFiles = List.of("nonexistent.json");
        BackupMetadata metadata = new BackupMetadata("test", "test", "test", "test", 
                                                     "9.10.0", 100, 50.0, null, null, shardFiles);
        
        List<Path> indexPaths = reader.findLuceneIndexPaths(tempDir, metadata);
        
        assertTrue(indexPaths.isEmpty());
    }
}