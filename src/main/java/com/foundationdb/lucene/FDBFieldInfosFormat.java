package com.foundationdb.lucene;

import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FieldInfosReader;
import org.apache.lucene.codecs.FieldInfosWriter;

import java.io.IOException;

public class FDBFieldInfosFormat extends FieldInfosFormat
{
    private final FieldInfosReader reader = new FDBFieldInfosReader();
    private final FieldInfosWriter writer = new FDBFieldInfosWriter();

    @Override
    public FieldInfosReader getFieldInfosReader() throws IOException {
        return reader;
    }

    @Override
    public FieldInfosWriter getFieldInfosWriter() throws IOException {
        return writer;
    }
}
