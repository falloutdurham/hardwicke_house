package com.lucidworks.hardwickehouse;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.NIOFSDirectory;
import java.nio.file.Paths;

public class ManualIndexTest {
    public static void main(String[] args) {
        try {
            System.out.println("Attempting to read Lucene index...");
            NIOFSDirectory directory = new NIOFSDirectory(Paths.get("solr-test/solr-9.3.0/example/techproducts/solr/techproducts/data/index"));
            DirectoryReader reader = DirectoryReader.open(directory);
            System.out.println("✅ Successfully opened index!");
            System.out.println("Number of documents: " + reader.numDocs());
            System.out.println("Number of deleted docs: " + reader.numDeletedDocs());
            System.out.println("Max doc ID: " + reader.maxDoc());
            
            if (reader.numDocs() > 0) {
                System.out.println("\nFirst document fields:");
                var doc = reader.document(0);
                doc.getFields().forEach(field -> {
                    System.out.println("  " + field.name() + " = " + field.stringValue());
                });
            }
            
            reader.close();
            directory.close();
            
        } catch (Exception e) {
            System.err.println("❌ Error reading index: " + e.getMessage());
            e.printStackTrace();
        }
    }
}