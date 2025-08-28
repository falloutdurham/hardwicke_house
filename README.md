# Hardwicke House

Hardwicke House is a Java CLI application that converts Apache Lucene indexes and Solr backup collections to JSONL (JSON Lines) format. The application supports both local file operations and Google Cloud Storage (GCS) integration for downloading source indexes and uploading converted JSONL files.

## Features

- **Lucene Index Conversion**: Convert Apache Lucene indexes directly to JSONL format
- **Solr Backup Processing**: Parse and convert Solr collection backups to JSONL
- **Multi-shard Support**: Handle Solr backups with multiple shards automatically  
- **GCS Integration**: Download indexes from and upload results to Google Cloud Storage
- **Compression Support**: Optional gzip compression for JSONL output files
- **Progress Reporting**: Real-time progress updates and conversion metrics
- **Backup Restoration**: Automatically restore Lucene indexes from Solr backup files

## Build and Development Commands

### Prerequisites
This project requires Java 21. Use SDKMAN to manage Java versions:

```bash
# Initialize SDKMAN in current shell
source ~/.sdkman/bin/sdkman-init.sh

# Check current Java version
sdk current java

# Install Java 21 if needed
sdk install java 21.0.7-tem
sdk use java 21.0.7-tem
```

### Maven Commands
**IMPORTANT**: Always source SDKMAN before running Maven commands:
- `source ~/.sdkman/bin/sdkman-init.sh && mvn clean compile` - Clean and compile the project
- `source ~/.sdkman/bin/sdkman-init.sh && mvn test` - Run all unit tests
- `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ClassName` - Run a specific test class
- `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ClassName#methodName` - Run a specific test method
- `source ~/.sdkman/bin/sdkman-init.sh && mvn package` - Build the executable JAR
- `source ~/.sdkman/bin/sdkman-init.sh && mvn verify` - Run tests and integration tests
- `source ~/.sdkman/bin/sdkman-init.sh && mvn clean install` - Full build with installation to local repository

### Running the Application

#### Basic Usage
- `java -jar target/hardwicke-house-*.jar --help` - Show CLI help

#### Converting Lucene Indexes
- `java -jar target/hardwicke-house-*.jar convert --source <path> --output <path>` - Basic conversion
- `java -jar target/hardwicke-house-*.jar convert --source <path> --output <path> --compress` - Compressed JSONL output
- `java -jar target/hardwicke-house-*.jar convert --gcs-source gs://bucket/path --gcs-output gs://bucket/output.jsonl.gz --compress` - GCS operations with compression

#### Converting Solr Backups
- `java -jar target/hardwicke-house-*.jar convert --backup-properties backup.properties --backup-directory /path/to/backup --output output.jsonl` - Convert Solr backup to JSONL
- `java -jar target/hardwicke-house-*.jar convert --backup-properties backup.properties --backup-directory /path/to/backup --gcs-output gs://bucket/output.jsonl.gz --compress` - Convert backup and upload to GCS

## Architecture

The application follows a modular CLI architecture with these key components:

### Core Processing Flow
1. **CLI Layer** (`Application.java`) - Handles command-line arguments and orchestrates operations
2. **Conversion Service** (`ConversionService.java`) - Orchestrates the conversion process for both Lucene indexes and Solr backups
3. **Index Reader** (`IndexReader.java`) - Opens and reads Lucene indexes, extracts document fields and metadata
4. **Backup Components** - Parse and process Solr backup files:
   - `BackupPropertiesParser.java` - Parses Solr backup properties files
   - `BackupReader.java` - Locates and restores Lucene indexes from backup directories
   - `BackupMetadata.java` - Stores backup collection metadata
5. **JSONL Writer** (`JsonLWriter.java`) - Converts Lucene documents to JSONL format and writes files
6. **Storage Service** (`StorageService.java`) - Abstracts local filesystem and GCS operations
7. **Progress Reporter** (`ProgressReporter.java`) - Provides real-time progress updates and metrics

### Data Flow Architecture
- **Lucene Index Processing**: Indexes are read document-by-document to manage memory usage
- **Solr Backup Processing**: Backup metadata is parsed to locate shard indexes, which are then restored if needed
- **Multi-shard Support**: Multiple shard indexes are automatically merged during processing
- **Document Batching**: Documents are batched for efficient JSONL writing
- **Schema Inference**: Schema inference occurs during the first pass of document processing
- **Progress Reporting**: Progress is reported at configurable intervals during processing

### Configuration and Logging
- JSON structured logging using Logback with contextual information
- Configuration supports both command-line arguments and external config files
- GCS authentication uses service account keys or default credentials
- Error handling includes retry logic for transient failures

### Key Dependencies
- Apache Lucene 9.7.0 for index reading
- Jackson for JSON processing and JSONL output format
- Google Cloud Storage client libraries
- Picocli for CLI interface
- Logback with JSON encoder for structured logging

## Solr Backup Format Support

The application supports Solr collection backups with the following structure:

### Backup Properties File
Contains collection metadata including:
- Collection name and configuration
- Shard metadata file references
- Index size and file count information
- Backup timestamps

### Shard Metadata Files
JSON files that map backup UUIDs to original Lucene index filenames, enabling restoration of the original index structure.

### Index Directory Structure
Backup directories can contain:
- Direct Lucene index directories (when available)
- UUID-based backup files that are restored on-demand
- Multi-shard collections automatically detected and processed

## Testing Strategy

- Unit tests mock external dependencies (GCS, file system)
- Integration tests use temporary directories and test indexes
- Backup processing tests validate restoration and multi-shard handling
- Performance tests validate memory usage with large indexes
- Test coverage target is 80%+ for core business logic