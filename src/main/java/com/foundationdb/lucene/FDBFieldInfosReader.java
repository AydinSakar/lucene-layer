package com.foundationdb.lucene;

import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.FieldInfosReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.foundationdb.lucene.FDBFieldInfosWriter.ATTRS;
import static com.foundationdb.lucene.FDBFieldInfosWriter.DOCVALUES;
import static com.foundationdb.lucene.FDBFieldInfosWriter.INDEXOPTIONS;
import static com.foundationdb.lucene.FDBFieldInfosWriter.ISINDEXED;
import static com.foundationdb.lucene.FDBFieldInfosWriter.NAME;
import static com.foundationdb.lucene.FDBFieldInfosWriter.NORMS;
import static com.foundationdb.lucene.FDBFieldInfosWriter.NORMS_TYPE;
import static com.foundationdb.lucene.FDBFieldInfosWriter.PAYLOADS;
import static com.foundationdb.lucene.FDBFieldInfosWriter.STORETV;

public class FDBFieldInfosReader extends FieldInfosReader
{

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

    @Override
    public FieldInfos read(Directory directory, String segmentName, IOContext iocontext) throws IOException {
        FDBDirectory fdbDir = FDBDirectory.unwrapFDBDirectory(directory);
        Transaction txn = fdbDir.txn;
        Tuple tuple = fdbDir.subspace.add(segmentName).add("inf");

        List<FieldInfo> fieldInfos = new ArrayList<FieldInfo>();

        int lastFieldNum = -1;
        InfoBuilder info = null;
        for(KeyValue kv : txn.getRange(tuple.range())) {
            Tuple fieldTuple = Tuple.fromBytes(kv.getKey());
            int fieldNum = (int) fieldTuple.getLong(tuple.size());
            if(fieldNum != lastFieldNum) {
                if(info != null) {
                    fieldInfos.add(info.build((int) lastFieldNum));
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
                info.normsType = docValuesType(nrmType);
            } else if(key.equals(DOCVALUES)) {
                String dvType = value.getString(0);
                info.docValuesType = docValuesType(dvType);
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

    public DocValuesType docValuesType(String dvType) {
        if("false".equals(dvType)) {
            return null;
        } else {
            return DocValuesType.valueOf(dvType);
        }
    }

    private String readString(int offset, BytesRef scratch) {
        return new String(scratch.bytes, scratch.offset + offset, scratch.length - offset, IOUtils.CHARSET_UTF_8);
    }
}
