package com.lucidworks.hardwickehouse;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class JsonLWriter {
    
    private static final Logger logger = LoggerFactory.getLogger(JsonLWriter.class);
    
    private BufferedWriter writer;
    private Path outputPath;
    private ObjectMapper objectMapper;
    private long documentsWritten = 0;
    private boolean compressed = false;
    
    public JsonLWriter() {
        this.objectMapper = new ObjectMapper();
    }
    
    public Path initialize(String outputFilename) throws IOException {
        return initialize(outputFilename, false);
    }
    
    public Path initialize(String outputFilename, boolean compress) throws IOException {
        this.compressed = compress;
        
        if (compress) {
            this.outputPath = Files.createTempFile("lucene_to_jsonl_", ".jsonl.gz");
        } else {
            this.outputPath = Files.createTempFile("lucene_to_jsonl_", ".jsonl");
        }
        
        if (outputFilename != null) {
            Path targetPath = Path.of(outputFilename);
            if (targetPath.getParent() != null) {
                Files.createDirectories(targetPath.getParent());
            }
            this.outputPath = targetPath;
        }
        
        logger.info("Initializing JSONL writer with output path: {} (compressed: {})", outputPath, compress);
        
        if (compress) {
            this.writer = new BufferedWriter(
                new OutputStreamWriter(
                    new GZIPOutputStream(Files.newOutputStream(outputPath))));
        } else {
            this.writer = Files.newBufferedWriter(outputPath);
        }
        
        logger.info("JSONL writer initialized successfully");
        return outputPath;
    }
    
    public void writeBatch(List<LuceneDocument> documents) throws IOException {
        logger.debug("Writing batch of {} documents", documents.size());
        
        for (LuceneDocument document : documents) {
            writeDocument(document);
        }
        
        writer.flush();
    }
    
    private void writeDocument(LuceneDocument document) throws IOException {
        // Create a JSON object with all the document fields
        var jsonDoc = document.getFields();
        jsonDoc.put("_docId", document.getDocId());
        
        // Convert to JSON and write as a line
        String jsonLine = objectMapper.writeValueAsString(jsonDoc);
        writer.write(jsonLine);
        writer.newLine();
        
        documentsWritten++;
    }
    
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
            logger.info("JSONL writer closed successfully. Wrote {} documents (compressed: {})", documentsWritten, compressed);
        }
    }
    
    public long getDocumentsWritten() {
        return documentsWritten;
    }
}