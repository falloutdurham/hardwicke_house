# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Hardwicke House is a Java CLI application that reads Apache Lucene indexes and converts them to JSONL (JSON Lines) format. The application supports both local file operations and Google Cloud Storage (GCS) integration for downloading source indexes and uploading converted JSONL files.

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
- `java -jar target/hardwicke-house-*.jar --help` - Show CLI help
- `java -jar target/hardwicke-house-*.jar convert --source <path> --output <path>` - Basic conversion
- `java -jar target/hardwicke-house-*.jar convert --source <path> --output <path> --compress` - Compressed JSONL output
- `java -jar target/hardwicke-house-*.jar convert --gcs-source gs://bucket/path --gcs-output gs://bucket/output.jsonl.gz --compress` - GCS operations with compression

## Architecture

The application follows a modular CLI architecture with these key components:

### Core Processing Flow
1. **CLI Layer** (`Application.java`) - Handles command-line arguments and orchestrates operations
2. **Index Reader** (`IndexReader.java`) - Opens and reads Lucene indexes, extracts document fields and metadata
3. **JSONL Writer** (`JsonLWriter.java`) - Converts Lucene documents to JSONL format and writes files
4. **Storage Service** (`StorageService.java`) - Abstracts local filesystem and GCS operations
5. **Progress Reporter** (`ProgressReporter.java`) - Provides real-time progress updates and metrics

### Data Flow Architecture
- Lucene indexes are read document-by-document to manage memory usage
- Documents are batched for efficient JSONL writing
- Schema inference occurs during the first pass of document processing
- Progress is reported at configurable intervals during processing

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

## Testing Strategy

- Unit tests mock external dependencies (GCS, file system)
- Integration tests use temporary directories and test indexes
- Performance tests validate memory usage with large indexes
- Test coverage target is 80%+ for core business logic