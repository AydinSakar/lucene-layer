package com.foundationdb.lucene;

import com.foundationdb.KeyValue;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


//
// (dirTuple, segmentName, "fld", docID, 0, fieldNum) => ("typeName", dataIndex)    # "field type" key
// ...
// (dirTuple, segmentName, "fld", docID, 1, fieldNum, dataIndex, off0) => (data0)   # "field data" key, part 0
// (dirTuple, segmentName, "fld", docID, 1, fieldNum, dataIndex, off1) => (data1)   # "field data" key, part 1
// ..
//
public class FDBStoredFieldsFormat extends StoredFieldsFormat
{
    private final static String STORED_FIELDS_EXT = "fld";
    private final static int FIELD_TYPE_SUBSPACE = 0;
    private final static int FIELD_DATA_SUBSPACE = 1;
    private final static String TYPE_STRING = "string";
    private final static String TYPE_BINARY = "binary";
    private final static String TYPE_INT = "int";
    private final static String TYPE_LONG = "long";
    private final static String TYPE_FLOAT = "float";
    private final static String TYPE_DOUBLE = "double";

    private static final Set<String> KNOWN_TYPES = new HashSet<String>(
            Arrays.asList(
                    TYPE_INT, TYPE_LONG, TYPE_FLOAT, TYPE_DOUBLE, TYPE_BINARY, TYPE_STRING
            )
    );


    private final static int LARGE_VALUE_BLOCK_SIZE = 10000;


    //
    // StoredFieldsFormat
    //

    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) {
        return new FDBStoredFieldsReader(directory, si, fn);
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) {
        return new FDBStoredFieldsWriter(directory, si.name);
    }


    //
    // StoredFieldsReader
    //

    private static class FDBStoredFieldsReader extends StoredFieldsReader
    {
        private final FDBDirectory dir;
        private final Tuple segmentTuple;
        private final FieldInfos fieldInfos;

        public FDBStoredFieldsReader(Directory dirIn, SegmentInfo si, FieldInfos fn) {
            this.dir = Util.unwrapDirectory(dirIn);
            this.segmentTuple = dir.subspace.add(si.name).add(STORED_FIELDS_EXT);
            this.fieldInfos = fn;
        }

        // used by clone
        private FDBStoredFieldsReader(FDBDirectory dir, Tuple segmentTuple, FieldInfos fieldInfos) {
            this.dir = dir;
            this.segmentTuple = segmentTuple;
            this.fieldInfos = fieldInfos;
        }


        @Override
        public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {
            final Tuple docTuple = segmentTuple.add(docID);
            final Tuple docTypesTuple = segmentTuple.add(docID).add(FIELD_TYPE_SUBSPACE);

            for(KeyValue kv : dir.txn.getRange(docTypesTuple.range())) {
                Tuple keyTuple = Tuple.fromBytes(kv.getKey());
                Tuple valueTuple = Tuple.fromBytes(kv.getValue());

                int fieldNumber = (int)keyTuple.getLong(docTypesTuple.size());
                String type = valueTuple.getString(0);
                long dataIndex = valueTuple.getLong(1);

                FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);
                if(!KNOWN_TYPES.contains(type)) {
                    throw new RuntimeException("unknown field type for field " + fieldInfo.name + ": " + type);
                }

                switch(visitor.needsField(fieldInfo)) {
                    case YES:
                        readField(makeFieldDataTuple(docTuple, fieldNumber, dataIndex), type, fieldInfo, visitor);
                        break;
                    case NO:
                        break;
                    case STOP:
                        return;
                }
            }
        }

        private void readField(Tuple fieldTuple,
                               String type,
                               FieldInfo fieldInfo,
                               StoredFieldVisitor visitor) throws IOException {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            for(KeyValue kv : dir.txn.getRange(fieldTuple.range())) {
                byte[] value = kv.getValue();
                os.write(value);
            }

            assert os.size() > 0;

            byte[] fullValue = os.toByteArray();
            Tuple tupleValue = Tuple.fromBytes(fullValue);
            if(type.equals(TYPE_STRING)) {
                visitor.stringField(fieldInfo, tupleValue.getString(0));
            } else if(type.equals(TYPE_BINARY)) {
                visitor.binaryField(fieldInfo, tupleValue.getBytes(0).clone());
            } else if(type.equals(TYPE_INT)) {
                visitor.intField(fieldInfo, (int)tupleValue.getLong(0));
            } else if(type.equals(TYPE_LONG)) {
                visitor.longField(fieldInfo, tupleValue.getLong(0));
            } else if(type.equals(TYPE_FLOAT)) {
                visitor.floatField(fieldInfo, Float.intBitsToFloat((int)tupleValue.getLong(0)));
            } else if(type.equals(TYPE_DOUBLE)) {
                visitor.doubleField(fieldInfo, Double.longBitsToDouble(tupleValue.getLong(0)));
            }
        }

        @Override
        @SuppressWarnings("CloneDoesntCallSuperClone")
        public FDBStoredFieldsReader clone() {
            // TODO: Needed?
            //if (in == null) {
            //  throw new AlreadyClosedException("this FieldsReader is closed");
            //}
            return new FDBStoredFieldsReader(dir, segmentTuple, fieldInfos);
        }

        @Override
        public void close() {
        }
    }


    //
    // StoredFieldsWriter
    //

    private static class FDBStoredFieldsWriter extends StoredFieldsWriter
    {
        private final FDBDirectory dir;
        private final Tuple segmentTuple;
        private Tuple docTuple;
        private int docCount;


        public FDBStoredFieldsWriter(Directory dirIn, String segment) {
            this.dir = Util.unwrapDirectory(dirIn);
            this.segmentTuple = dir.subspace.add(segment).add(STORED_FIELDS_EXT);
        }

        @Override
        public void startDocument(int numStoredFields) {
            docTuple = segmentTuple.add(docCount++);
        }

        @Override
        public void writeField(FieldInfo info, IndexableField field) {
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
                value = Util.copyRange(bytesRef);
            } else if(field.stringValue() != null) {
                type = TYPE_STRING;
                value = field.stringValue();
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported type for IndexableField %s: %s",
                                field.name(),
                                field.fieldType().docValueType()
                        )
                );
            }

            byte[] typeKey = makeFieldTypeTuple(docTuple, info.number).pack();
            byte[] typeValue = dir.txn.get(typeKey).get();
            long index = 0;
            if(typeValue != null) {
                index = Tuple.fromBytes(typeValue).getLong(1);
            }
            dir.txn.set(typeKey, Tuple.from(type, index).pack());

            Tuple dataTuple = makeFieldDataTuple(docTuple, info.number, index);
            // Clear any old data
            dir.txn.clear(dataTuple.range());
            Util.writeLargeValue(dir.txn, dataTuple, LARGE_VALUE_BLOCK_SIZE, Tuple.from(value).pack());
        }

        @Override
        public void abort() {
            // TODO?
            close();
        }

        @Override
        public void finish(FieldInfos fis, int numDocs) {
            if(docCount != numDocs) {
                throw new RuntimeException(
                        "mergeFields produced an invalid result: docCount is " + numDocs + " but only saw " + docCount + "; now aborting this merge to prevent index corruption"
                );
            }
        }

        @Override
        public void close() {
            // None
        }
    }


    //
    // Helpers
    //

    private static Tuple makeFieldTypeTuple(Tuple docTuple, int fieldNum) {
        return docTuple.add(FIELD_TYPE_SUBSPACE).add(fieldNum);
    }

    private static Tuple makeFieldDataTuple(Tuple docTuple, int fieldNum, long index) {
        return docTuple.add(FIELD_DATA_SUBSPACE).add(fieldNum).add(index);
    }
}
