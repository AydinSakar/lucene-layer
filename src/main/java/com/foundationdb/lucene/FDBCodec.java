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

import java.util.EnumSet;

public final class FDBCodec extends FilterCodec
{
    static final String CONFIG_PROP_NAME = "fdbcodec.formats";
    static final String CONFIG_VALUE_ALL = "ALL";
    static final String CONFIG_VALUE_NONE = "NONE";
    static final String CONFIG_VALUE_DEFAULT = "DEFAULT";


    private final PostingsFormat postings;
    private final DocValuesFormat docValues;
    private final StoredFieldsFormat storedFields;
    private final TermVectorsFormat termVectors;
    private final FieldInfosFormat fieldInfos;
    private final SegmentInfoFormat segmentInfo;
    private final NormsFormat norms;
    private final LiveDocsFormat liveDocs;


    public FDBCodec() {
        this(System.getProperty(CONFIG_PROP_NAME, CONFIG_VALUE_DEFAULT));
    }

    public FDBCodec(String formatOptStr) {
        this(deriveFormatOpts(formatOptStr));
    }

    public FDBCodec(EnumSet<FormatOpts> opts) {
        super(FDBCodec.class.getSimpleName(), new Lucene42Codec());
        this.postings = opts.contains(FormatOpts.POSTINGS) ? new FDBPostingsFormat() : super.postingsFormat();
        this.docValues = opts.contains(FormatOpts.DOC_VALUES) ? new FDBDocValuesFormat() : super.docValuesFormat();
        this.storedFields = opts.contains(FormatOpts.STORED_FIELDS) ? new FDBStoredFieldsFormat() : super.storedFieldsFormat();
        this.termVectors = opts.contains(FormatOpts.TERM_VECTORS) ? new FDBTermVectorsFormat() : super.termVectorsFormat();
        this.fieldInfos = opts.contains(FormatOpts.FIELD_INFOS) ? new FDBFieldInfosFormat() : super.fieldInfosFormat();
        this.segmentInfo = opts.contains(FormatOpts.SEGMENT_INFO) ? new FDBSegmentInfoFormat() : super.segmentInfoFormat();
        this.norms = opts.contains(FormatOpts.NORMS) ? new FDBNormsFormat() : super.normsFormat();
        this.liveDocs = opts.contains(FormatOpts.LIVE_DOCS) ? new FDBLiveDocsFormat() : super.liveDocsFormat();
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postings;
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return docValues;
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFields;
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
      return termVectors;
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return fieldInfos;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return segmentInfo;
    }

    @Override
    public NormsFormat normsFormat() {
      return norms;
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
      return liveDocs;
    }


    //
    // Helpers
    //

    private static enum FormatOpts
    {
        POSTINGS,
        DOC_VALUES,
        STORED_FIELDS,
        TERM_VECTORS,
        FIELD_INFOS,
        SEGMENT_INFO,
        NORMS,
        LIVE_DOCS
    }

    public static EnumSet<FormatOpts> deriveFormatOpts(String configStr) {
        if(CONFIG_VALUE_ALL.equals(configStr.toUpperCase())) {
            return EnumSet.allOf(FormatOpts.class);
        }
        if(CONFIG_VALUE_NONE.equals(configStr.toUpperCase())) {
            return EnumSet.noneOf(FormatOpts.class);
        }
        if(CONFIG_VALUE_DEFAULT.equals(configStr.toUpperCase())) {
            return EnumSet.of(FormatOpts.POSTINGS, FormatOpts.STORED_FIELDS,
                              FormatOpts.FIELD_INFOS, FormatOpts.SEGMENT_INFO,
                              FormatOpts.TERM_VECTORS);
        }
        EnumSet<FormatOpts> enumSet = EnumSet.noneOf(FormatOpts.class);
        String[] optNames = configStr.split(",");
        for(String name : optNames) {
            FormatOpts opt = FormatOpts.valueOf(name.toUpperCase());
            enumSet.add(opt);
        }
        return enumSet;
    }
}
