package com.foundationdb.lucene;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SimpleTest
{
    @Test
    public void indexBasic() throws Exception {
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40, analyzer);
        // recreate the index on each execution
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setCodec(new FDBCodec());
        Directory luceneDir = new FDBTestDirectory();
        IndexWriter writer = new IndexWriter(luceneDir, config);
        try {
            writer.addDocument(Arrays.asList(
                    new TextField("title", "The title of my first document", Store.YES),
                    new TextField("content", "The content of the first document", Store.NO)));

            writer.addDocument(
                    Arrays.asList(
                            new TextField("title", "The title of the second document", Store.YES),
                            new TextField("content", "And this is the content", Store.NO)
                    )
            );
        } finally {
            writer.close();
        }
        assertDocumentsAreThere(luceneDir, 2);
    }

    private void assertDocumentsAreThere(Directory dir, int amount) throws IOException {
        IndexReader reader = DirectoryReader.open(dir);
        try {
            assertEquals(amount, reader.numDocs());
        } finally {
            reader.close();
        }
    }
}
