package com.lucidworks.hardwickehouse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

class ProgressReporterTest {
    
    private ProgressReporter progressReporter;
    
    @AfterEach
    void tearDown() {
        if (progressReporter != null) {
            progressReporter.stop();
        }
    }
    
    @Test
    void testProgressReporterCreation() {
        progressReporter = new ProgressReporter(5);
        assertNotNull(progressReporter);
        assertEquals(0, progressReporter.getProcessedDocuments());
        assertEquals(0, progressReporter.getTotalDocuments());
    }
    
    @Test
    void testSetTotalDocuments() {
        progressReporter = new ProgressReporter(1);
        progressReporter.setTotalDocuments(1000);
        assertEquals(1000, progressReporter.getTotalDocuments());
    }
    
    @Test
    void testUpdateProgress() {
        progressReporter = new ProgressReporter(1);
        progressReporter.updateProgress(50);
        assertEquals(50, progressReporter.getProcessedDocuments());
        
        progressReporter.updateProgress(150);
        assertEquals(150, progressReporter.getProcessedDocuments());
    }
    
    @Test
    void testStartAndStop() {
        progressReporter = new ProgressReporter(10);
        
        assertDoesNotThrow(() -> {
            progressReporter.start();
            progressReporter.setTotalDocuments(100);
            progressReporter.updateProgress(25);
            Thread.sleep(100);
            progressReporter.complete();
        });
    }
    
    @Test
    void testErrorHandling() {
        progressReporter = new ProgressReporter(1);
        
        assertDoesNotThrow(() -> {
            progressReporter.start();
            progressReporter.error("Test error message");
        });
    }
}