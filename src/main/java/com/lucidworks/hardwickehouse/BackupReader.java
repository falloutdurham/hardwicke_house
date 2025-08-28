package com.lucidworks.hardwickehouse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BackupReader {
    
    private static final Logger logger = LoggerFactory.getLogger(BackupReader.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    public List<Path> findLuceneIndexPaths(Path backupDirectory, BackupMetadata metadata) throws IOException {
        logger.info("Searching for Lucene indexes in backup directory: {}", backupDirectory);
        
        List<Path> indexPaths = new ArrayList<>();
        
        for (String shardMetadataFile : metadata.getShardMetadataFiles()) {
            Path metadataPath = backupDirectory.resolve(shardMetadataFile);
            
            if (!Files.exists(metadataPath)) {
                logger.warn("Shard metadata file not found: {}", metadataPath);
                continue;
            }
            
            Path shardIndexPath = findShardIndexPath(backupDirectory, metadataPath);
            if (shardIndexPath != null) {
                indexPaths.add(shardIndexPath);
                logger.info("Found shard index at: {}", shardIndexPath);
            } else {
                logger.info("Attempting to restore index from UUID files for: {}", shardMetadataFile);
                try {
                    Path restoredIndexPath = restoreIndexFromBackup(backupDirectory, shardMetadataFile);
                    indexPaths.add(restoredIndexPath);
                    logger.info("Successfully restored shard index to: {}", restoredIndexPath);
                } catch (IOException e) {
                    logger.warn("Failed to restore index from backup for {}: {}", shardMetadataFile, e.getMessage());
                }
            }
        }
        
        if (indexPaths.isEmpty()) {
            logger.warn("No Lucene index directories found in backup");
            Path fallbackPath = findFallbackIndexPath(backupDirectory);
            if (fallbackPath != null) {
                indexPaths.add(fallbackPath);
                logger.info("Using fallback index path: {}", fallbackPath);
            }
        }
        
        return indexPaths;
    }
    
    private Path findShardIndexPath(Path backupDirectory, Path metadataPath) throws IOException {
        try {
            JsonNode metadata = objectMapper.readTree(metadataPath.toFile());
            
            String indexDirName = extractIndexDirectoryName(metadata);
            if (indexDirName != null) {
                Path indexPath = backupDirectory.resolve(indexDirName);
                if (Files.exists(indexPath) && isValidLuceneIndex(indexPath)) {
                    return indexPath;
                }
            }
            
        } catch (Exception e) {
            logger.warn("Failed to parse shard metadata file: {}", metadataPath, e);
        }
        
        return null;
    }
    
    private String extractIndexDirectoryName(JsonNode metadata) {
        JsonNode indexDirNode = metadata.path("indexDir");
        if (!indexDirNode.isMissingNode()) {
            return indexDirNode.asText();
        }
        
        JsonNode dataNode = metadata.path("data");
        if (!dataNode.isMissingNode()) {
            return dataNode.asText();
        }
        
        JsonNode pathNode = metadata.path("path");
        if (!pathNode.isMissingNode()) {
            String fullPath = pathNode.asText();
            return Paths.get(fullPath).getFileName().toString();
        }
        
        return null;
    }
    
    private Path findFallbackIndexPath(Path backupDirectory) throws IOException {
        return Files.walk(backupDirectory, 3)
            .filter(path -> Files.isDirectory(path))
            .filter(this::isValidLuceneIndex)
            .findFirst()
            .orElse(null);
    }
    
    private boolean isValidLuceneIndex(Path path) {
        try {
            return Files.exists(path.resolve("segments_1")) || 
                   Files.exists(path.resolve("segments.gen")) ||
                   Files.list(path).anyMatch(file -> 
                       file.getFileName().toString().startsWith("segments_"));
        } catch (IOException e) {
            return false;
        }
    }
    
    public boolean validateBackupStructure(Path backupDirectory, BackupMetadata metadata) {
        logger.info("Validating backup structure for collection: {}", metadata.getCollection());
        
        if (!Files.exists(backupDirectory) || !Files.isDirectory(backupDirectory)) {
            logger.error("Backup directory does not exist: {}", backupDirectory);
            return false;
        }
        
        boolean allShardsFound = true;
        for (String shardMetadataFile : metadata.getShardMetadataFiles()) {
            Path metadataPath = backupDirectory.resolve(shardMetadataFile);
            if (!Files.exists(metadataPath)) {
                logger.warn("Missing shard metadata file: {}", shardMetadataFile);
                allShardsFound = false;
            }
        }
        
        if (!allShardsFound) {
            logger.warn("Some shard metadata files are missing, but continuing with available shards");
        }
        
        return true;
    }
    
    public Path restoreIndexFromBackup(Path backupDirectory, String shardMetadataFile) throws IOException {
        logger.info("Restoring index from backup using metadata: {}", shardMetadataFile);
        
        Path metadataPath = backupDirectory.resolve(shardMetadataFile);
        if (!Files.exists(metadataPath)) {
            throw new IOException("Shard metadata file not found: " + metadataPath);
        }
        
        JsonNode metadata = objectMapper.readTree(metadataPath.toFile());
        Map<String, String> fileMapping = parseFileMapping(metadata);
        
        if (fileMapping.isEmpty()) {
            throw new IOException("No file mappings found in metadata: " + metadataPath);
        }
        
        Path tempIndexDir = Files.createTempDirectory("restored-index-");
        logger.info("Created temporary index directory: {}", tempIndexDir);
        
        for (Map.Entry<String, String> entry : fileMapping.entrySet()) {
            String uuid = entry.getKey();
            String originalFilename = entry.getValue();
            
            Path sourceFile = backupDirectory.resolve("index").resolve(uuid);
            Path targetFile = tempIndexDir.resolve(originalFilename);
            
            if (Files.exists(sourceFile)) {
                Files.copy(sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
                logger.debug("Restored file: {} -> {}", uuid, originalFilename);
            } else {
                logger.warn("Source file not found: {}", sourceFile);
            }
        }
        
        if (!isValidLuceneIndex(tempIndexDir)) {
            throw new IOException("Restored index is not valid: " + tempIndexDir);
        }
        
        logger.info("Successfully restored index to: {}", tempIndexDir);
        return tempIndexDir;
    }
    
    private Map<String, String> parseFileMapping(JsonNode metadata) {
        Map<String, String> fileMapping = new HashMap<>();
        
        metadata.fields().forEachRemaining(entry -> {
            String uuid = entry.getKey();
            JsonNode fileInfo = entry.getValue();
            JsonNode fileNameNode = fileInfo.get("fileName");
            
            if (fileNameNode != null && !fileNameNode.isMissingNode()) {
                String fileName = fileNameNode.asText();
                fileMapping.put(uuid, fileName);
            }
        });
        
        logger.info("Parsed {} file mappings from metadata", fileMapping.size());
        return fileMapping;
    }
}