package com.foundationdb.lucene;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class FDBNormsFormat extends NormsFormat
{
    private static final String NORMS_SEG_EXTENSION = "len";

    @Override
    public DocValuesConsumer normsConsumer(SegmentWriteState state) throws IOException {
        return new FDBNormsConsumer(state);
    }

    @Override
    public DocValuesProducer normsProducer(SegmentReadState state) throws IOException {
        return new FDBNormsProducer(state);
    }

    public static class FDBNormsProducer extends FDBDocValuesReader
    {
        public FDBNormsProducer(SegmentReadState state) throws IOException {
            // All we do is change the extension from .dat -> .len;
            // otherwise this is a normal simple doc values file:
            super(state, NORMS_SEG_EXTENSION);
        }
    }

    public static class FDBNormsConsumer extends FDBDocValuesWriter
    {
        public FDBNormsConsumer(SegmentWriteState state) throws IOException {
            // All we do is change the extension from .dat -> .len;
            // otherwise this is a normal simple doc values file:
            super(state, NORMS_SEG_EXTENSION);
        }
    }
}
