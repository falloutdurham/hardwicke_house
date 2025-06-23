package com.lucidworks.hardwickehouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

@Command(name = "hardwicke-house", 
         description = "Convert Lucene indexes to JSONL format",
         mixinStandardHelpOptions = true,
         version = "1.0.0")
public class Application implements Callable<Integer> {
    
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    @Command(name = "convert", description = "Convert Lucene index to JSONL")
    static class ConvertCommand implements Callable<Integer> {
        
        @Option(names = {"-s", "--source"}, 
                description = "Source Lucene index path (local)")
        private String sourcePath;
        
        @Option(names = {"--gcs-source"}, 
                description = "Source Lucene index GCS path (gs://bucket/path)")
        private String gcsSourcePath;
        
        @Option(names = {"-o", "--output"}, 
                description = "Output JSONL file path (local)")
        private String outputPath;
        
        @Option(names = {"--gcs-output"}, 
                description = "Output JSONL file GCS path (gs://bucket/path)")
        private String gcsOutputPath;
        
        @Option(names = {"--batch-size"}, 
                description = "Batch size for processing documents", 
                defaultValue = "1000")
        private int batchSize;
        
        @Option(names = {"--progress-interval"}, 
                description = "Progress reporting interval in seconds", 
                defaultValue = "10")
        private int progressInterval;
        
        @Option(names = {"--gcs-credentials"}, 
                description = "Path to GCS service account credentials JSON file")
        private String gcsCredentialsPath;
        
        @Option(names = {"--compress"}, 
                description = "Compress output JSONL file using gzip", 
                defaultValue = "false")
        private boolean compress;
        
        @Override
        public Integer call() throws Exception {
            logger.info("Starting Lucene to JSONL conversion");
            
            if ((sourcePath == null && gcsSourcePath == null) || 
                (sourcePath != null && gcsSourcePath != null)) {
                logger.error("Must specify exactly one of --source or --gcs-source");
                return 1;
            }
            
            if ((outputPath == null && gcsOutputPath == null) || 
                (outputPath != null && gcsOutputPath != null)) {
                logger.error("Must specify exactly one of --output or --gcs-output");
                return 1;
            }
            
            try {
                StorageService storageService = new StorageService(gcsCredentialsPath);
                ProgressReporter progressReporter = new ProgressReporter(progressInterval);
                IndexReader indexReader = new IndexReader();
                JsonLWriter jsonLWriter = new JsonLWriter();
                
                ConversionService conversionService = new ConversionService(
                    storageService, progressReporter, indexReader, jsonLWriter);
                
                String sourceLocation = sourcePath != null ? sourcePath : gcsSourcePath;
                String outputLocation = outputPath != null ? outputPath : gcsOutputPath;
                boolean useGcsSource = gcsSourcePath != null;
                boolean useGcsOutput = gcsOutputPath != null;
                
                conversionService.convert(sourceLocation, outputLocation, 
                                        useGcsSource, useGcsOutput, batchSize, compress);
                
                logger.info("Conversion completed successfully");
                return 0;
                
            } catch (Exception e) {
                logger.error("Conversion failed", e);
                return 1;
            }
        }
    }
    
    @Override
    public Integer call() throws Exception {
        CommandLine.usage(this, System.out);
        return 0;
    }
    
    public static void main(String[] args) {
        CommandLine commandLine = new CommandLine(new Application());
        commandLine.addSubcommand("convert", new ConvertCommand());
        
        int exitCode = commandLine.execute(args);
        System.exit(exitCode);
    }
}