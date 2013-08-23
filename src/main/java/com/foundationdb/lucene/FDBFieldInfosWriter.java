package com.foundationdb.lucene;

import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.FieldInfosWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.Map;

public class FDBFieldInfosWriter extends FieldInfosWriter
{

    /** Extension of field infos */
    static final String FIELD_INFOS_EXTENSION = "inf";


    static final String NUMFIELDS = "number of fields";
    static final String NAME = "name";
    static final String NUMBER = "number";
    static final String ISINDEXED = "indexed";
    static final String STORETV = "term vectors";
    static final String STORETVPOS = "term vector positions";
    static final String STORETVOFF = "term vector offsets";
    static final String PAYLOADS = "payloads";
    static final String NORMS = "norms";
    static final String NORMS_TYPE = "norms type";
    static final String DOCVALUES = "doc values";
    static final String INDEXOPTIONS = "index options";
    static final String ATTRS = "attributes";
    final static String ATT_KEY = "key";
    final static String ATT_VALUE = "value";


    private static void set(Transaction txn, Tuple tuple, String name, String value) {
        txn.set(tuple.add(name).pack(), Tuple.from(value).pack());
    }

    private static void set(Transaction txn, Tuple tuple, String name, boolean value) {
        txn.set(tuple.add(name).pack(), Tuple.from(value ? 1 : 0).pack());
    }


    @Override
    public void write(Directory directory, String segmentName, FieldInfos infos, IOContext context) throws IOException {
        FDBDirectory fdbDir = FDBDirectory.unwrapFDBDirectory(directory);
        Transaction txn = fdbDir.txn;
        Tuple tuple = fdbDir.subspace.add(segmentName).add("inf");

        //System.out.println("segmentName: " + segmentName);
        for(FieldInfo fi : infos) {
            final Tuple fieldTuple = tuple.add(fi.number);
            //System.out.println("writing: " + fi.name + ", number: " + fi.number);

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
            set(txn, fieldTuple, NORMS_TYPE, getDocValuesType(fi.getNormType()));
            set(txn, fieldTuple, DOCVALUES, getDocValuesType(fi.getDocValuesType()));

            Tuple attrTuple = fieldTuple.add(ATTRS);
            if(fi.attributes() != null) {
                for(Map.Entry<String, String> entry : fi.attributes().entrySet()) {
                    set(txn, attrTuple, entry.getKey(), entry.getValue());
                }
            }
        }
    }

    private static String getDocValuesType(DocValuesType type) {
        return type == null ? "false" : type.toString();
    }
}
