package com.foundationdb.lucene;

import com.foundationdb.KeyValue;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FieldInfosReader;
import org.apache.lucene.codecs.FieldInfosWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.foundationdb.lucene.Util.getBool;
import static com.foundationdb.lucene.Util.set;

public class FDBFieldInfosFormat extends FieldInfosFormat
{
    private static final String FIELD_INFOS_EXT = "inf";
    private static final String NAME = "name";
    private static final String HAS_INDEX = "has_index";
    private static final String HAS_PAYLOADS = "has_payloads";
    private static final String HAS_NORMS = "has_norms";
    private static final String HAS_VECTORS = "has_vectors";
    private static final String DOC_VALUES_TYPE = "doc_values_type";
    private static final String NORMS_TYPE = "norms_type";
    private static final String INDEX_OPTIONS = "index_options";
    private static final String ATTR = "attr";


    private final FieldInfosReader reader = new Reader();
    private final FieldInfosWriter writer = new Writer();


    //
    // FieldInfosFormat
    //

    @Override
    public FieldInfosReader getFieldInfosReader() {
        return reader;
    }

    @Override
    public FieldInfosWriter getFieldInfosWriter() {
        return writer;
    }


    //
    // FieldInfosReader
    //

    private static class Reader extends FieldInfosReader
    {
        @Override
        public FieldInfos read(Directory dirIn, String segmentName, IOContext iocontext) {
            final FDBDirectory dir = Util.unwrapDirectory(dirIn);
            final Tuple segmentTuple = dir.subspace.add(segmentName).add(FIELD_INFOS_EXT);

            List<FieldInfo> fieldInfos = new ArrayList<FieldInfo>();
            int lastFieldNum = -1;
            InfoBuilder info = null;
            for(KeyValue kv : dir.txn.getRange(segmentTuple.range())) {
                Tuple fieldTuple = Tuple.fromBytes(kv.getKey());
                int fieldNum = (int)fieldTuple.getLong(segmentTuple.size());
                if(fieldNum != lastFieldNum) {
                    if(info != null) {
                        fieldInfos.add(info.build(lastFieldNum));
                    }
                    info = new InfoBuilder();
                    lastFieldNum = fieldNum;
                }
                String key = fieldTuple.getString(segmentTuple.size() + 1);
                Tuple value = Tuple.fromBytes(kv.getValue());
                if(key.equals(NAME)) {
                    info.name = value.getString(0);
                } else if(key.equals(HAS_INDEX)) {
                    info.isIndexed = getBool(value, 0);
                } else if(key.equals(HAS_VECTORS)) {
                    info.storeTermVector = getBool(value, 0);
                } else if(key.equals(HAS_PAYLOADS)) {
                    info.storePayloads = getBool(value, 0);
                } else if(key.equals(HAS_NORMS)) {
                    info.omitNorms = !getBool(value, 0);
                } else if(key.equals(NORMS_TYPE)) {
                    String normType = value.getString(0);
                    info.normsType = stringToDocValuesType(normType);
                } else if(key.equals(DOC_VALUES_TYPE)) {
                    String docValueType = value.getString(0);
                    info.docValuesType = stringToDocValuesType(docValueType);
                } else if(key.equals(INDEX_OPTIONS)) {
                    info.indexOptions = IndexOptions.valueOf(value.getString(0));
                } else if(key.equals(ATTR)) {
                    info.attrs.put(fieldTuple.getString(segmentTuple.size() + 2), value.getString(0));
                } else {
                    throw new IllegalStateException("Unknown key: " + key);
                }
            }

            if(info != null) {
                fieldInfos.add(info.build(lastFieldNum));
            }

            return new FieldInfos(fieldInfos.toArray(new FieldInfo[fieldInfos.size()]));
        }
    }


    //
    // FieldInfosWriter
    //

    private static class Writer extends FieldInfosWriter
    {
        @Override
        public void write(Directory dirIn, String segmentName, FieldInfos infos, IOContext context) {
            final FDBDirectory dir = Util.unwrapDirectory(dirIn);
            final Tuple segmentTuple = dir.subspace.add(segmentName).add(FIELD_INFOS_EXT);

            for(FieldInfo fi : infos) {
                final Tuple fieldTuple = segmentTuple.add(fi.number);
                set(dir.txn, fieldTuple, NAME, fi.name);
                set(dir.txn, fieldTuple, HAS_INDEX, fi.isIndexed());

                if(fi.isIndexed()) {
                    assert fi.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 ||
                           !fi.hasPayloads();
                    set(dir.txn, fieldTuple, INDEX_OPTIONS, fi.getIndexOptions().toString());
                }

                set(dir.txn, fieldTuple, HAS_NORMS, !fi.omitsNorms());
                set(dir.txn, fieldTuple, HAS_PAYLOADS, fi.hasPayloads());
                set(dir.txn, fieldTuple, HAS_VECTORS, fi.hasVectors());
                set(dir.txn, fieldTuple, DOC_VALUES_TYPE, docValuesTypeToString(fi.getDocValuesType()));
                set(dir.txn, fieldTuple, NORMS_TYPE, docValuesTypeToString(fi.getNormType()));

                if(fi.attributes() != null) {
                    Tuple attrTuple = fieldTuple.add(ATTR);
                    for(Map.Entry<String, String> entry : fi.attributes().entrySet()) {
                        set(dir.txn, attrTuple, entry.getKey(), entry.getValue());
                    }
                }
            }
        }
    }


    //
    // Internal
    //

    private static class InfoBuilder
    {
        String name = null;
        boolean isIndexed = false;
        IndexOptions indexOptions = null;
        boolean storeTermVector = false;
        boolean storePayloads = false;
        boolean omitNorms = false;
        DocValuesType normsType = null;
        DocValuesType docValuesType = null;
        Map<String, String> attrs = new HashMap<String, String>();

        public FieldInfo build(int fieldNum) {
            return new FieldInfo(
                    name,
                    isIndexed,
                    fieldNum,
                    storeTermVector,
                    omitNorms,
                    storePayloads,
                    indexOptions,
                    docValuesType,
                    normsType,
                    attrs
            );
        }
    }


    public static DocValuesType stringToDocValuesType(String dvType) {
        return (dvType == null) ? null : DocValuesType.valueOf(dvType);
    }

    public static String docValuesTypeToString(DocValuesType type) {
        return (type == null) ? null : type.name();
    }
}
