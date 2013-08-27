package com.foundationdb.lucene;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class FDBDocValuesFormat extends DocValuesFormat
{
    public FDBDocValuesFormat() {
        super(FDBDocValuesFormat.class.getSimpleName());
    }


    //
    // DocValuesFormat
    //

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new FDBDocValuesWriter(state, "dat");
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new FDBDocValuesReader(state, "dat");
    }
}
