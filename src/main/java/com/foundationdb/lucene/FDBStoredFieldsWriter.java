package com.foundationdb.lucene;

import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class FDBStoredFieldsWriter extends StoredFieldsWriter
{
    private final FDBDirectory dir;
    private final Tuple segmentTuple;
    private Tuple docTuple;

    private int numDocsWritten = 0;

    final static String FIELDS_EXTENSION = "fld";

    final static String TYPE_STRING = "string";
    final static String TYPE_BINARY = "binary";
    final static String TYPE_INT = "int";
    final static String TYPE_LONG = "long";
    final static String TYPE_FLOAT = "float";
    final static String TYPE_DOUBLE = "double";


    public FDBStoredFieldsWriter(Directory dirIn, String segment, IOContext context) throws IOException {
        this.dir = FDBDirectory.unwrapFDBDirectory(dirIn);
        this.segmentTuple = dir.subspace.add(segment).add("fld");
    }

    // tuple(<prefix>, docNum, 0, field_num) => tuple(type, index)
    // tuple(<prefix>, docNum, 1, field_num, index, 0) => tuple(value0)
    // tuple(<prefix>, docNum, 1, field_num, index, 1) => tuple(value1)
    // ...

    static Tuple makeFieldTypeTuple(Tuple docTuple, int fieldNum) {
        return docTuple.add(0).add(fieldNum);
    }

    static Tuple makeFieldDataTuple(Tuple docTuple, int fieldNum, long index) {
        return docTuple.add(1).add(fieldNum).add(index);
    }


    @Override
    public void startDocument(int numStoredFields) throws IOException {
        docTuple = segmentTuple.add(numDocsWritten++);
    }

    @Override
    public void writeField(FieldInfo info, IndexableField field) throws IOException {
        final String type;
        final Object value;
        if(field.numericValue() != null) {
            Number n = field.numericValue();
            if(n instanceof Byte || n instanceof Short || n instanceof Integer) {
                type = TYPE_INT;
                value = n.intValue();
            } else if(n instanceof Long) {
                type = TYPE_LONG;
                value = n.longValue();
            } else if(n instanceof Float) {
                type = TYPE_FLOAT;
                value = Float.floatToRawIntBits(n.floatValue());
            } else if(n instanceof Double) {
                type = TYPE_DOUBLE;
                value = Double.doubleToRawLongBits(n.doubleValue());
            } else {
                throw new IllegalArgumentException("cannot store numeric type " + n.getClass());
            }
        } else if(field.binaryValue() != null) {
            type = TYPE_BINARY;
            BytesRef bytesRef = field.binaryValue();
            value = FDBDirectory.copyRange(bytesRef);
        } else if(field.stringValue() != null) {
            type = TYPE_STRING;
            value = field.stringValue();
        } else {
            throw new IllegalArgumentException(
                    "unsupported type for IndexableField " + field.name() + ": " + field.fieldType()
                                                                                        .docValueType()
            );
        }


        byte[] typeKey = makeFieldTypeTuple(docTuple, info.number).pack();
        byte[] typeValue = dir.txn.get(typeKey).get();
        long index = 1;
        if(typeValue != null) {
            index = Tuple.fromBytes(typeValue).getLong(1);
        }

        dir.txn.set(typeKey, Tuple.from(type, index).pack());

        Tuple dataTuple = makeFieldDataTuple(docTuple, info.number, index);
        FDBDirectory.writeLargeValue(dir.txn, dataTuple, 10000, Tuple.from(value).pack());
    }


    @Override
    public void abort() {
        // TODO?
        close();
    }

    @Override
    public void finish(FieldInfos fis, int numDocs) {
        if(numDocsWritten != numDocs) {
            throw new RuntimeException(
                    "mergeFields produced an invalid result: docCount is " + numDocs + " but only saw " + numDocsWritten + "; now aborting this merge to prevent index corruption"
            );
        }
    }

    @Override
    public void close() {
        // None
    }
}
