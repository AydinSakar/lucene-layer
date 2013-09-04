package com.foundationdb.lucene;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class FDBNormsFormat extends NormsFormat
{
    private static final String NORMS_EXT = "len";

    @Override
    public DocValuesConsumer normsConsumer(SegmentWriteState state) throws IOException {
        return new FDBNormsConsumer(state);
    }

    @Override
    public DocValuesProducer normsProducer(SegmentReadState state) throws IOException {
        return new FDBNormsProducer(state);
    }

    public static class FDBNormsProducer extends FDBDocValuesFormat.FDBDocValuesProducer
    {
        public FDBNormsProducer(SegmentReadState state) throws IOException {
            super(state, NORMS_EXT);
        }
    }

    public static class FDBNormsConsumer extends FDBDocValuesFormat.FDBDocValuesConsumer
    {
        public FDBNormsConsumer(SegmentWriteState state) throws IOException {
            super(state, NORMS_EXT);
        }
    }
}
