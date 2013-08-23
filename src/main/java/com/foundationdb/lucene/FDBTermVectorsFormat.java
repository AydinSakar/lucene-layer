package com.foundationdb.lucene;

import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

public class FDBTermVectorsFormat extends TermVectorsFormat
{

    @Override
    public TermVectorsReader vectorsReader(Directory directory,
                                           SegmentInfo segmentInfo,
                                           FieldInfos fieldInfos,
                                           IOContext context) throws IOException {
        return new FDBTermVectorsReader(directory, segmentInfo, context);
    }

    @Override
    public TermVectorsWriter vectorsWriter(Directory directory,
                                           SegmentInfo segmentInfo,
                                           IOContext context) throws IOException {
        return new FDBTermVectorsWriter(directory, segmentInfo.name, context);
    }
}
