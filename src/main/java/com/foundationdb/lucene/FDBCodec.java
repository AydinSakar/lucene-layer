package com.foundationdb.lucene;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;

public final class FDBCodec extends FilterCodec
{
    static final String CODEC_NAME = FDBCodec.class.getSimpleName();

    private final FDBPostingsFormat postings = new FDBPostingsFormat();
    private final FDBDocValuesFormat docValues = new FDBDocValuesFormat();
    private final FDBStoredFieldsFormat storedFields = new FDBStoredFieldsFormat();
    private final FDBTermVectorsFormat termVectors = new FDBTermVectorsFormat();
    private final FDBFieldInfos fieldInfos = new FDBFieldInfos();
    private final FDBSegmentInfoFormat segmentInfos = new FDBSegmentInfoFormat();
    private final FDBNormsFormat normsFormat = new FDBNormsFormat();
    private final FDBLiveDocsFormat liveDocs = new FDBLiveDocsFormat();


    public FDBCodec() {
        super(CODEC_NAME, new Lucene42Codec());
    }

    @Override
    public FDBPostingsFormat postingsFormat() {
        return postings;
    }

    //@Override
    //public FDBDocValues docValuesFormat() {
    //    return docValues;
    //}

    @Override
    public FDBStoredFieldsFormat storedFieldsFormat() {
        return storedFields;
    }

    //@Override
    //public TermVectorsFormat termVectorsFormat() {
    //  return termVectors;
    //}

    @Override
    public FDBFieldInfos fieldInfosFormat() {
        return fieldInfos;
    }

    //@Override
    //public FDBSegmentInfoFormat segmentInfoFormat() {
    //  return segmentInfos;
    //}
    //
    //@Override
    //public FDBNormsFormat normsFormat() {
    //  return norms;
    //}
    //
    //@Override
    //public FDBLiveDocsFormat liveDocsFormat() {
    //  return liveDocs;
    //}

}
