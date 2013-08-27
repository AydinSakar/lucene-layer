package com.foundationdb.lucene;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;

public final class FDBCodec extends FilterCodec
{
    static final String CODEC_NAME = FDBCodec.class.getSimpleName();

    private final FDBPostings postings = new FDBPostings();
    private final FDBDocValuesFormat dvFormat = new FDBDocValuesFormat();
    private final FDBStoredFields storedFields = new FDBStoredFields();
    private final FDBTermVectorsFormat vectorsFormat = new FDBTermVectorsFormat();
    private final FDBFieldInfos fieldInfosFormat = new FDBFieldInfos();
    private final FDBSegmentInfoFormat segmentInfos = new FDBSegmentInfoFormat();
    private final FDBNormsFormat normsFormat = new FDBNormsFormat();
    private final FDBLiveDocsFormat liveDocs = new FDBLiveDocsFormat();


    public FDBCodec() {
        super(CODEC_NAME, new Lucene42Codec());
    }

    @Override
    public FDBPostings postingsFormat() {
        return postings;
    }

    @Override
    public FDBDocValuesFormat docValuesFormat() {
        return dvFormat;
    }

    @Override
    public FDBStoredFields storedFieldsFormat() {
        return storedFields;
    }

    //@Override
    //public TermVectorsFormat termVectorsFormat() {
    //  return vectorsFormat;
    //}

    @Override
    public FDBFieldInfos fieldInfosFormat() {
        return fieldInfosFormat;
    }

    //@Override
    //public FDBSegmentInfoFormat segmentInfoFormat() {
    //  return segmentInfos;
    //}
    //
    //@Override
    //public FDBNormsFormat normsFormat() {
    //  return normsFormat;
    //}
    //
    //@Override
    //public FDBLiveDocsFormat liveDocsFormat() {
    //  return liveDocs;
    //}

}
