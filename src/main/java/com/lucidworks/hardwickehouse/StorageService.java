package com.lucidworks.hardwickehouse;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class StorageService {
    
    private static final Logger logger = LoggerFactory.getLogger(StorageService.class);
    
    private final Storage storage;
    
    public StorageService(String credentialsPath) {
        if (credentialsPath != null && !credentialsPath.isEmpty()) {
            logger.info("Initializing GCS client with credentials from: {}", credentialsPath);
            System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", credentialsPath);
        } else {
            logger.info("Initializing GCS client with default credentials");
        }
        
        this.storage = StorageOptions.getDefaultInstance().getService();
    }
    
    public Path downloadFromGcs(String gcsPath) throws IOException {
        logger.info("Downloading from GCS: {}", gcsPath);
        
        if (!gcsPath.startsWith("gs://")) {
            throw new IllegalArgumentException("GCS path must start with gs://");
        }
        
        String pathWithoutPrefix = gcsPath.substring(5);
        int firstSlash = pathWithoutPrefix.indexOf('/');
        
        if (firstSlash == -1) {
            throw new IllegalArgumentException("Invalid GCS path format: " + gcsPath);
        }
        
        String bucketName = pathWithoutPrefix.substring(0, firstSlash);
        String objectName = pathWithoutPrefix.substring(firstSlash + 1);
        
        logger.info("Bucket: {}, Object: {}", bucketName, objectName);
        
        Path tempDir = Files.createTempDirectory("lucene_index_");
        Path downloadPath = tempDir.resolve("index.zip");
        
        BlobId blobId = BlobId.of(bucketName, objectName);
        Blob blob = storage.get(blobId);
        
        if (blob == null) {
            throw new IOException("Object not found: " + gcsPath);
        }
        
        blob.downloadTo(downloadPath);
        logger.info("Downloaded {} bytes to {}", blob.getSize(), downloadPath);
        
        if (objectName.endsWith(".zip")) {
            Path extractedPath = tempDir.resolve("extracted");
            extractZip(downloadPath, extractedPath);
            Files.delete(downloadPath);
            return extractedPath;
        }
        
        return downloadPath;
    }
    
    public void uploadToGcs(Path localPath, String gcsPath) throws IOException {
        logger.info("Uploading {} to GCS: {}", localPath, gcsPath);
        
        if (!gcsPath.startsWith("gs://")) {
            throw new IllegalArgumentException("GCS path must start with gs://");
        }
        
        String pathWithoutPrefix = gcsPath.substring(5);
        int firstSlash = pathWithoutPrefix.indexOf('/');
        
        if (firstSlash == -1) {
            throw new IllegalArgumentException("Invalid GCS path format: " + gcsPath);
        }
        
        String bucketName = pathWithoutPrefix.substring(0, firstSlash);
        String objectName = pathWithoutPrefix.substring(firstSlash + 1);
        
        logger.info("Uploading to bucket: {}, object: {}", bucketName, objectName);
        
        BlobId blobId = BlobId.of(bucketName, objectName);
        String contentType = "application/octet-stream";
        if (localPath.toString().endsWith(".jsonl")) {
            contentType = "application/x-jsonlines";
        } else if (localPath.toString().endsWith(".jsonl.gz")) {
            contentType = "application/x-jsonlines";
        }
        
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                .setContentType(contentType)
                .build();
        
        byte[] content = Files.readAllBytes(localPath);
        storage.create(blobInfo, content);
        
        logger.info("Successfully uploaded {} bytes", content.length);
    }
    
    public void moveLocalFile(Path source, Path destination) throws IOException {
        logger.info("Moving file from {} to {}", source, destination);
        
        Files.createDirectories(destination.getParent());
        Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING);
        
        logger.info("File moved successfully");
    }
    
    public void deleteLocalFile(Path path) throws IOException {
        logger.info("Deleting local file/directory: {}", path);
        
        if (Files.isDirectory(path)) {
            deleteDirectory(path);
        } else {
            Files.deleteIfExists(path);
        }
        
        logger.info("Local file/directory deleted");
    }
    
    private void deleteDirectory(Path directory) throws IOException {
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
            
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
    
    private void extractZip(Path zipPath, Path extractPath) throws IOException {
        logger.info("Extracting ZIP file {} to {}", zipPath, extractPath);
        
        Files.createDirectories(extractPath);
        
        try (ZipInputStream zipIn = new ZipInputStream(Files.newInputStream(zipPath))) {
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                Path filePath = extractPath.resolve(entry.getName());
                
                if (entry.isDirectory()) {
                    Files.createDirectories(filePath);
                } else {
                    Files.createDirectories(filePath.getParent());
                    Files.copy(zipIn, filePath, StandardCopyOption.REPLACE_EXISTING);
                }
                
                zipIn.closeEntry();
            }
        }
        
        logger.info("ZIP extraction completed");
    }
}