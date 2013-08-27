package com.foundationdb.lucene;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;

public final class FDBCodec extends FilterCodec
{
    static final String CODEC_NAME = "FDBCodec";

    private final PostingsFormat postings = new FDBPostings();
    private final StoredFieldsFormat storedFields = new FDBStoredFieldsFormat();
    private final SegmentInfoFormat segmentInfos = new FDBSegmentInfoFormat();
    private final FieldInfosFormat fieldInfosFormat = new FDBFieldInfos();
    private final TermVectorsFormat vectorsFormat = new FDBTermVectorsFormat();
    private final NormsFormat normsFormat = new FDBNormsFormat();
    private final LiveDocsFormat liveDocs = new FDBLiveDocsFormat();
    private final DocValuesFormat dvFormat = new FDBDocValuesFormat();

    public FDBCodec() {
        super(CODEC_NAME, new Lucene42Codec());
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postings;
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFields;
    }

    //@Override
    //public TermVectorsFormat termVectorsFormat() {
    //  return vectorsFormat;
    //}

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    //@Override
    //public SegmentInfoFormat segmentInfoFormat() {
    //  return segmentInfos;
    //}
    //
    //@Override
    //public NormsFormat normsFormat() {
    //  return normsFormat;
    //}
    //
    //@Override
    //public LiveDocsFormat liveDocsFormat() {
    //  return liveDocs;
    //}

    @Override
    public DocValuesFormat docValuesFormat() {
        return dvFormat;
    }
}
