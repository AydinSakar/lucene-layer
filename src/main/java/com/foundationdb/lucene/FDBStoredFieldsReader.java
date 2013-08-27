package com.foundationdb.lucene;

import com.foundationdb.KeyValue;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.foundationdb.lucene.FDBStoredFieldsWriter.TYPE_BINARY;
import static com.foundationdb.lucene.FDBStoredFieldsWriter.TYPE_DOUBLE;
import static com.foundationdb.lucene.FDBStoredFieldsWriter.TYPE_FLOAT;
import static com.foundationdb.lucene.FDBStoredFieldsWriter.TYPE_INT;
import static com.foundationdb.lucene.FDBStoredFieldsWriter.TYPE_LONG;
import static com.foundationdb.lucene.FDBStoredFieldsWriter.TYPE_STRING;
import static com.foundationdb.lucene.FDBStoredFieldsWriter.makeFieldDataTuple;
import static com.foundationdb.lucene.FDBStoredFieldsWriter.makeFieldTypeTuple;

public class FDBStoredFieldsReader extends StoredFieldsReader
{
    private static final Set<String> KNOWN_TYPES = new HashSet<String>(
            Arrays.asList(
                    TYPE_INT, TYPE_LONG, TYPE_FLOAT, TYPE_DOUBLE, TYPE_BINARY, TYPE_STRING
            )
    );

    private final FDBDirectory dir;
    private final Tuple segmentTuple;
    private final FieldInfos fieldInfos;

    public FDBStoredFieldsReader(Directory dirIn, SegmentInfo si, FieldInfos fn, IOContext context) {
        this.dir = FDBDirectory.unwrapFDBDirectory(dirIn);
        this.segmentTuple = dir.subspace.add(si.name).add("fld");
        this.fieldInfos = fn;
    }

    // used by clone
    private FDBStoredFieldsReader(FDBDirectory dir, Tuple segmentTuple, FieldInfos fieldInfos) {
        this.dir = dir;
        this.segmentTuple = segmentTuple;
        this.fieldInfos = fieldInfos;
    }


    @Override
    public void visitDocument(int n, StoredFieldVisitor visitor) throws IOException {
        final Tuple docTuple = segmentTuple.add(n);
        final Tuple docTypeTuple = makeFieldTypeTuple(docTuple, n);

        for(KeyValue kv : dir.txn.getRange(docTypeTuple.range())) {
            int fieldNumber = (int) Tuple.fromBytes(kv.getKey()).getLong(docTypeTuple.size());
            Tuple typeTuple = Tuple.fromBytes(kv.getValue());
            String type = typeTuple.getString(0);
            long index = typeTuple.getLong(1);

            FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);
            if(!KNOWN_TYPES.contains(type)) {
                throw new RuntimeException("unknown field type for field " + fieldInfo.name + ": " + type);
            }

            switch(visitor.needsField(fieldInfo)) {
                case YES:
                    readField(makeFieldDataTuple(docTuple, n, index), type, fieldInfo, visitor);
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
            visitor.binaryField(fieldInfo, tupleValue.getBytes(0));
        } else if(type.equals(TYPE_INT)) {
            visitor.intField(fieldInfo, (int) tupleValue.getLong(0));
        } else if(type.equals(TYPE_LONG)) {
            visitor.longField(fieldInfo, tupleValue.getLong(0));
        } else if(type.equals(TYPE_FLOAT)) {
            visitor.floatField(fieldInfo, Float.intBitsToFloat((int) tupleValue.getLong(0)));
        } else if(type.equals(TYPE_DOUBLE)) {
            visitor.doubleField(fieldInfo, Double.longBitsToDouble(tupleValue.getLong(0)));
        }
    }

    @Override
    public StoredFieldsReader clone() {
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
