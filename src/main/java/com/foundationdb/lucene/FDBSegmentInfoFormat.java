package com.foundationdb.lucene;

import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.SegmentInfoReader;
import org.apache.lucene.codecs.SegmentInfoWriter;

public class FDBSegmentInfoFormat extends SegmentInfoFormat
{
    private final SegmentInfoReader reader = new FDBSegmentInfoReader();
    private final SegmentInfoWriter writer = new FDBSegmentInfoWriter();

    public static final String SI_EXTENSION = "si";

    @Override
    public SegmentInfoReader getSegmentInfoReader() {
        return reader;
    }

    @Override
    public SegmentInfoWriter getSegmentInfoWriter() {
        return writer;
    }
}
