package com.lucidworks.hardwickehouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ProgressReporter {
    
    private static final Logger logger = LoggerFactory.getLogger(ProgressReporter.class);
    
    private final int reportingIntervalSeconds;
    private final AtomicLong processedDocuments = new AtomicLong(0);
    private volatile long totalDocuments = 0;
    private volatile Instant startTime;
    private volatile Instant lastReportTime;
    private ScheduledExecutorService scheduler;
    
    public ProgressReporter(int reportingIntervalSeconds) {
        this.reportingIntervalSeconds = reportingIntervalSeconds;
    }
    
    public void start() {
        startTime = Instant.now();
        lastReportTime = startTime;
        processedDocuments.set(0);
        
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "progress-reporter");
            t.setDaemon(true);
            return t;
        });
        
        scheduler.scheduleAtFixedRate(this::reportProgress, 
                                    reportingIntervalSeconds, 
                                    reportingIntervalSeconds, 
                                    TimeUnit.SECONDS);
        
        logger.info("Progress reporting started with interval {} seconds", reportingIntervalSeconds);
    }
    
    public void setTotalDocuments(long totalDocuments) {
        this.totalDocuments = totalDocuments;
        logger.info("Total documents set to: {}", totalDocuments);
    }
    
    public void updateProgress(long processedCount) {
        processedDocuments.set(processedCount);
    }
    
    public void complete() {
        if (scheduler != null && !scheduler.isShutdown()) {
            reportProgress();
            stop();
        }
        
        Instant endTime = Instant.now();
        long totalProcessingTime = ChronoUnit.SECONDS.between(startTime, endTime);
        long finalProcessedCount = processedDocuments.get();
        
        logger.info("Processing completed successfully. " +
                   "Processed {} documents in {} seconds. " +
                   "Average rate: {:.2f} docs/sec",
                   finalProcessedCount,
                   totalProcessingTime,
                   totalProcessingTime > 0 ? (double) finalProcessedCount / totalProcessingTime : 0.0);
    }
    
    public void error(String errorMessage) {
        logger.error("Processing failed: {}", errorMessage);
        stop();
    }
    
    public void stop() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
    
    private void reportProgress() {
        Instant now = Instant.now();
        long currentProcessed = processedDocuments.get();
        
        if (totalDocuments > 0) {
            double percentComplete = (double) currentProcessed / totalDocuments * 100;
            long elapsedSeconds = ChronoUnit.SECONDS.between(startTime, now);
            long intervalSeconds = ChronoUnit.SECONDS.between(lastReportTime, now);
            
            double overallRate = elapsedSeconds > 0 ? (double) currentProcessed / elapsedSeconds : 0;
            
            long remaining = totalDocuments - currentProcessed;
            long estimatedTimeRemaining = overallRate > 0 ? (long) (remaining / overallRate) : 0;
            
            logger.info("Progress: {}/{} documents ({:.1f}%) - " +
                       "Rate: {:.1f} docs/sec - " +
                       "Elapsed: {}s - " +
                       "ETA: {}s",
                       currentProcessed,
                       totalDocuments,
                       percentComplete,
                       overallRate,
                       elapsedSeconds,
                       estimatedTimeRemaining);
        } else {
            long elapsedSeconds = ChronoUnit.SECONDS.between(startTime, now);
            double overallRate = elapsedSeconds > 0 ? (double) currentProcessed / elapsedSeconds : 0;
            
            logger.info("Progress: {} documents processed - " +
                       "Rate: {:.1f} docs/sec - " +
                       "Elapsed: {}s",
                       currentProcessed,
                       overallRate,
                       elapsedSeconds);
        }
        
        lastReportTime = now;
    }
    
    public long getProcessedDocuments() {
        return processedDocuments.get();
    }
    
    public long getTotalDocuments() {
        return totalDocuments;
    }
}