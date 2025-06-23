package com.lucidworks.hardwickehouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class ConversionService {
    
    private static final Logger logger = LoggerFactory.getLogger(ConversionService.class);
    
    private final StorageService storageService;
    private final ProgressReporter progressReporter;
    private final IndexReader indexReader;
    private final JsonLWriter jsonLWriter;
    
    public ConversionService(StorageService storageService, 
                           ProgressReporter progressReporter,
                           IndexReader indexReader, 
                           JsonLWriter jsonLWriter) {
        this.storageService = storageService;
        this.progressReporter = progressReporter;
        this.indexReader = indexReader;
        this.jsonLWriter = jsonLWriter;
    }
    
    public void convert(String sourceLocation, String outputLocation, 
                       boolean useGcsSource, boolean useGcsOutput, int batchSize) throws Exception {
        convert(sourceLocation, outputLocation, useGcsSource, useGcsOutput, batchSize, false);
    }
    
    public void convert(String sourceLocation, String outputLocation, 
                       boolean useGcsSource, boolean useGcsOutput, int batchSize, boolean compress) throws Exception {
        
        logger.info("Starting conversion from {} to {}", sourceLocation, outputLocation);
        
        Path localSourcePath;
        if (useGcsSource) {
            logger.info("Downloading Lucene index from GCS: {}", sourceLocation);
            localSourcePath = storageService.downloadFromGcs(sourceLocation);
        } else {
            localSourcePath = Paths.get(sourceLocation);
        }
        
        logger.info("Reading Lucene index from: {}", localSourcePath);
        
        progressReporter.start();
        
        try {
            indexReader.initialize(localSourcePath);
            long totalDocuments = indexReader.getTotalDocuments();
            
            logger.info("Total documents to process: {}", totalDocuments);
            progressReporter.setTotalDocuments(totalDocuments);
            
            Path tempJsonLPath = jsonLWriter.initialize(useGcsOutput ? null : outputLocation, compress);
            logger.info("Initialized JSONL writer with output file: {}", tempJsonLPath);
            
            long processedDocuments = 0;
            List<LuceneDocument> batch;
            
            while ((batch = indexReader.readBatch(batchSize)).size() > 0) {
                jsonLWriter.writeBatch(batch);
                processedDocuments += batch.size();
                progressReporter.updateProgress(processedDocuments);
                
                if (batch.size() < batchSize) {
                    break;
                }
            }
            
            jsonLWriter.close();
            indexReader.close();
            
            if (useGcsOutput) {
                logger.info("Uploading JSONL file to GCS: {}", outputLocation);
                storageService.uploadToGcs(tempJsonLPath, outputLocation);
                
                logger.info("Cleaning up temporary JSONL file");
                storageService.deleteLocalFile(tempJsonLPath);
            } else {
                logger.info("JSONL file created at: {}", tempJsonLPath);
            }
            
            if (useGcsSource) {
                logger.info("Cleaning up temporary Lucene index files");
                storageService.deleteLocalFile(localSourcePath);
            }
            
            progressReporter.complete();
            logger.info("Conversion completed successfully. Processed {} documents", processedDocuments);
            
        } catch (Exception e) {
            progressReporter.error("Conversion failed: " + e.getMessage());
            throw e;
        } finally {
            progressReporter.stop();
        }
    }
}