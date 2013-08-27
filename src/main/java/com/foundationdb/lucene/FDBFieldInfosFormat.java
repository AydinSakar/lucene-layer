package com.foundationdb.lucene;

import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
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

public class FDBFieldInfosFormat extends FieldInfosFormat
{
    private static final String FIELD_INFOS_EXTENSION = "inf";
    private static final String NUMFIELDS = "number of fields";
    private static final String NAME = "name";
    private static final String NUMBER = "number";
    private static final String ISINDEXED = "indexed";
    private static final String STORETV = "term vectors";
    private static final String STORETVPOS = "term vector positions";
    private static final String STORETVOFF = "term vector offsets";
    private static final String PAYLOADS = "payloads";
    private static final String NORMS = "norms";
    private static final String NORMS_TYPE = "norms type";
    private static final String DOCVALUES = "doc values";
    private static final String INDEXOPTIONS = "index options";
    private static final String ATTRS = "attributes";
    private static final String ATT_KEY = "key";
    private static final String ATT_VALUE = "value";


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
        public FieldInfos read(Directory directory, String segmentName, IOContext iocontext) {
            FDBDirectory fdbDir = FDBDirectory.unwrapFDBDirectory(directory);
            Transaction txn = fdbDir.txn;
            Tuple tuple = fdbDir.subspace.add(segmentName).add(FIELD_INFOS_EXTENSION);

            List<FieldInfo> fieldInfos = new ArrayList<FieldInfo>();

            int lastFieldNum = -1;
            InfoBuilder info = null;
            for(KeyValue kv : txn.getRange(tuple.range())) {
                Tuple fieldTuple = Tuple.fromBytes(kv.getKey());
                int fieldNum = (int) fieldTuple.getLong(tuple.size());
                if(fieldNum != lastFieldNum) {
                    if(info != null) {
                        fieldInfos.add(info.build(lastFieldNum));
                    }
                    info = new InfoBuilder();
                    lastFieldNum = fieldNum;
                }
                String key = fieldTuple.getString(tuple.size() + 1);
                Tuple value = Tuple.fromBytes(kv.getValue());
                if(key.equals(NAME)) {
                    info.name = value.getString(0);
                } else if(key.equals(ISINDEXED)) {
                    info.isIndexed = value.getLong(0) == 1;
                } else if(key.equals(STORETV)) {
                    info.storeTermVector = value.getLong(0) == 1;
                }
                //case FDBFieldInfosWriter.STORETVPOS:
                //case FDBFieldInfosWriter.STORETVOFF
                else if(key.equals(PAYLOADS)) {
                    info.storePayloads = value.getLong(0) == 1;
                } else if(key.equals(NORMS)) {
                    info.omitNorms = !(value.getLong(0) == 1);
                } else if(key.equals(NORMS_TYPE)) {
                    String nrmType = value.getString(0);
                    info.normsType = stringToDocValuesType(nrmType);
                } else if(key.equals(DOCVALUES)) {
                    String dvType = value.getString(0);
                    info.docValuesType = stringToDocValuesType(dvType);
                } else if(key.equals(INDEXOPTIONS)) {
                    info.indexOptions = IndexOptions.valueOf(value.getString(0));
                } else if(key.equals(ATTRS)) {
                    info.atts.put(fieldTuple.getString(tuple.size() + 2), value.getString(0));
                } else {
                    throw new IllegalStateException("Unknown key: " + key);
                }
            }
            fieldInfos.add(info.build(lastFieldNum));

            return new FieldInfos(fieldInfos.toArray(new FieldInfo[fieldInfos.size()]));
        }
    }


    //
    // FieldInfosWriter
    //

    private static class Writer extends FieldInfosWriter
    {
        @Override
        public void write(Directory directory, String segmentName, FieldInfos infos, IOContext context) {
            FDBDirectory fdbDir = FDBDirectory.unwrapFDBDirectory(directory);
            Transaction txn = fdbDir.txn;
            Tuple tuple = fdbDir.subspace.add(segmentName).add(FIELD_INFOS_EXTENSION);

            for(FieldInfo fi : infos) {
                final Tuple fieldTuple = tuple.add(fi.number);
                set(txn, fieldTuple, NAME, fi.name);
                set(txn, fieldTuple, ISINDEXED, fi.isIndexed());

                if(fi.isIndexed()) {
                    assert fi.getIndexOptions()
                             .compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 || !fi.hasPayloads();
                    set(txn, fieldTuple, INDEXOPTIONS, fi.getIndexOptions().toString());
                }

                set(txn, fieldTuple, STORETV, fi.hasVectors());
                set(txn, fieldTuple, PAYLOADS, fi.hasPayloads());
                set(txn, fieldTuple, NORMS, !fi.omitsNorms());
                set(txn, fieldTuple, NORMS_TYPE, docValuesTypeToString(fi.getNormType()));
                set(txn, fieldTuple, DOCVALUES, docValuesTypeToString(fi.getDocValuesType()));

                Tuple attrTuple = fieldTuple.add(ATTRS);
                if(fi.attributes() != null) {
                    for(Map.Entry<String, String> entry : fi.attributes().entrySet()) {
                        set(txn, attrTuple, entry.getKey(), entry.getValue());
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
        Map<String, String> atts = new HashMap<String, String>();

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
                    atts
            );
        }
    }

    private static void set(Transaction txn, Tuple tuple, String name, String value) {
        txn.set(tuple.add(name).pack(), Tuple.from(value).pack());
    }

    private static void set(Transaction txn, Tuple tuple, String name, boolean value) {
        txn.set(tuple.add(name).pack(), Tuple.from(value ? 1 : 0).pack());
    }

    private static DocValuesType stringToDocValuesType(String dvType) {
        return "false".equals(dvType) ? null : DocValuesType.valueOf(dvType);
    }

    private static String docValuesTypeToString(DocValuesType type) {
        return type == null ? "false" : type.toString();
    }
}
