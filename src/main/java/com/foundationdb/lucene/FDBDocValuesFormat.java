package com.foundationdb.lucene;

import com.foundationdb.KeyValue;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static com.foundationdb.lucene.Util.set;

public class FDBDocValuesFormat extends DocValuesFormat
{
    private static final String DOC_VALUES_EXT = "dat";
    private static final String BYTES = "bytes";
    private static final String ORD = "ord";
    private static final String DOC_TO_ORD = "doc_ord";


    public FDBDocValuesFormat() {
        super(FDBDocValuesFormat.class.getSimpleName());
    }


    //
    // DocValuesFormat
    //

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new FDBDocValuesProducer(state, DOC_VALUES_EXT);
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new FDBDocValuesConsumer(state, DOC_VALUES_EXT);
    }


    //
    // DocValuesProducer (Reader)
    //

    static class FDBDocValuesProducer extends DocValuesProducer
    {
        private final FDBDirectory dir;
        private final Tuple segmentTuple;


        public FDBDocValuesProducer(SegmentReadState state, String ext) {
            this.dir = Util.unwrapDirectory(state.directory);
            this.segmentTuple = dir.subspace.add(state.segmentInfo.name).add(state.segmentSuffix).add(ext);
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo fieldInfo) {
            return new FDBNumericDocValues(fieldInfo.name);
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo fieldInfo) {
            return new FDBBinaryDocValues(fieldInfo.name);
        }

        @Override
        public SortedDocValues getSorted(FieldInfo fieldInfo) {
            return new FDBSortedDocValues(fieldInfo.name);
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo fieldInfo) {
            return new FDBSortedSetDocValues(fieldInfo.name);
        }

        @Override
        public void close() {
            // None
        }


        //
        // Helpers
        //

        private class FDBNumericDocValues extends NumericDocValues
        {
            private final Tuple numericTuple;

            public FDBNumericDocValues(String fieldName) {
                this.numericTuple = segmentTuple.add(fieldName).add(DocValuesType.NUMERIC.ordinal());
            }

            @Override
            public long get(int docID) {
                byte[] bytes = dir.txn.get(numericTuple.add(docID).pack()).get();
                assert bytes != null : "No numeric for docID: " + docID;
                return Tuple.fromBytes(bytes).getLong(0);
            }
        }

        private class FDBBinaryDocValues extends BinaryDocValues
        {
            private final Tuple binaryTuple;

            public FDBBinaryDocValues(String fieldName) {
                this.binaryTuple = segmentTuple.add(fieldName).add(DocValuesType.BINARY.ordinal());
            }

            @Override
            public void get(int docID, BytesRef result) {
                byte[] bytes = dir.txn.get(binaryTuple.add(docID).pack()).get();
                assert bytes != null : "No bytes for docID: " + docID;
                result.bytes = Tuple.fromBytes(bytes).getBytes(0).clone();
                result.offset = 0;
                result.length = result.bytes.length;
            }
        }

        private class FDBSortedDocValues extends SortedDocValues
        {
            private final Tuple sortedTuple;

            public FDBSortedDocValues(String fieldName) {
                this.sortedTuple = segmentTuple.add(fieldName).add(DocValuesType.SORTED.ordinal());
            }

            @Override
            public int getOrd(int docID) {
                byte[] bytes = dir.txn.get(sortedTuple.add(ORD).add(docID).pack()).get();
                assert bytes != null : "No ord for docID: " + docID;
                return (int)Tuple.fromBytes(bytes).getLong(0);
            }

            @Override
            public void lookupOrd(int ord, BytesRef result) {
                byte[] bytes = dir.txn.get(sortedTuple.add(BYTES).add(ord).pack()).get();
                assert bytes != null : "No bytes for ord: " + ord;
                result.bytes = Tuple.fromBytes(bytes).getBytes(0).clone();
                result.offset = 0;
                result.length = result.bytes.length;
            }

            @Override
            public int getValueCount() {
                int valueCount = 0;
                Tuple bytesTuple = sortedTuple.add(BYTES);
                List<KeyValue> lastValue = dir.txn.getRange(bytesTuple.range(), 1, true).asList().get();
                if(!lastValue.isEmpty()) {
                    if(!lastValue.isEmpty()) {
                        KeyValue kv = lastValue.get(0);
                        int maxOrdinal = (int)Tuple.fromBytes(kv.getKey()).getLong(bytesTuple.size());
                        valueCount = maxOrdinal + 1;
                    }
                }
                return valueCount;
            }
        }

        private class FDBSortedSetDocValues extends SortedSetDocValues
        {
            private final Tuple sortedSetTuple;
            private Iterator<KeyValue> ordIt = null;

            public FDBSortedSetDocValues(String fieldName) {
                this.sortedSetTuple = segmentTuple.add(fieldName).add(DocValuesType.SORTED_SET.ordinal());
            }

            @Override
            public long nextOrd() {
                if(!ordIt.hasNext()) {
                    return NO_MORE_ORDS;
                }
                KeyValue kv = ordIt.next();
                Tuple keyTuple = Tuple.fromBytes(kv.getKey());
                return (int)keyTuple.getLong(keyTuple.size() - 1);
            }

            @Override
            public void setDocument(int docID) {
                ordIt = dir.txn.getRange(sortedSetTuple.add(DOC_TO_ORD).add(docID).range()).iterator();
            }

            @Override
            public void lookupOrd(long ord, BytesRef result) {
                byte[] bytes = dir.txn.get(sortedSetTuple.add(BYTES).add(ord).pack()).get();
                assert bytes != null : "No bytes for ord: " + ord;
                result.bytes = Tuple.fromBytes(bytes).getBytes(0).clone();
                result.offset = 0;
                result.length = result.bytes.length;
            }

            @Override
            public long getValueCount() {
                Tuple bytesTuple = sortedSetTuple.add(BYTES);
                List<KeyValue> lastValue = dir.txn.getRange(bytesTuple.range(), 1, true).asList().get();
                int valueCount = 0;
                if(!lastValue.isEmpty()) {
                    KeyValue kv = lastValue.get(0);
                    int maxOrdinal = (int)Tuple.fromBytes(kv.getKey()).getLong(bytesTuple.size());
                    valueCount = maxOrdinal + 1;
                }
                return valueCount;
            }
        }
    }


    //
    // DocValuesConsumer (Writer)
    //

    static class FDBDocValuesConsumer extends DocValuesConsumer
    {
        private final FDBDirectory dir;
        private final Tuple segmentTuple;
        private final int expectedDocs;


        public FDBDocValuesConsumer(SegmentWriteState state, String ext) throws IOException {
            this.dir = Util.unwrapDirectory(state.directory);
            this.segmentTuple = dir.subspace.add(state.segmentInfo.name).add(state.segmentSuffix).add(ext);
            this.expectedDocs = state.segmentInfo.getDocCount();
        }

        @Override
        public void addNumericField(FieldInfo field, Iterable<Number> values) {
            assert (field.getDocValuesType() == DocValuesType.NUMERIC || field.getNormType() == DocValuesType.NUMERIC);
            Tuple fieldTuple = segmentTuple.add(field.name).add(DocValuesType.NUMERIC.ordinal());
            int docNum = 0;
            for(Number n : values) {
                assert n instanceof Long : n.getClass();
                set(dir.txn, fieldTuple, docNum, n.longValue());
                ++docNum;
            }
            checkWritten(docNum);
        }

        @Override
        public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) {
            assert field.getDocValuesType() == DocValuesType.BINARY;
            Tuple fieldTuple = segmentTuple.add(field.name).add(DocValuesType.BINARY.ordinal());
            int docNum = 0;
            for(BytesRef value : values) {
                set(dir.txn, fieldTuple, docNum, value);
                ++docNum;
            }
            checkWritten(docNum);
        }

        @Override
        public void addSortedField(FieldInfo field,
                                   Iterable<BytesRef> values,
                                   Iterable<Number> docToOrd) throws IOException {
            assert field.getDocValuesType() == DocValuesType.SORTED;
            Tuple fieldTuple = segmentTuple.add(field.name).add(field.getDocValuesType().ordinal());

            Tuple bytesTuple = fieldTuple.add(BYTES);
            int ordNum = 0;
            for(BytesRef value : values) {
                set(dir.txn, bytesTuple, ordNum, value);
                ++ordNum;
            }

            Tuple ordTuple = fieldTuple.add(ORD);
            int docNum = 0;
            for(Number ord : docToOrd) {
                set(dir.txn, ordTuple, docNum, ord.longValue());
                ++docNum;
            }

            checkWritten(docNum);
        }

        @Override
        public void addSortedSetField(FieldInfo field,
                                      Iterable<BytesRef> values,
                                      Iterable<Number> docToOrdCount,
                                      Iterable<Number> ords) {
            assert field.getDocValuesType() == DocValuesType.SORTED_SET;

            Tuple fieldTuple = segmentTuple.add(field.name).add(field.getDocValuesType().ordinal());
            Tuple bytesTuple = fieldTuple.add(BYTES);
            int ordNum = 0;
            for(BytesRef value : values) {
                set(dir.txn, bytesTuple, ordNum, value);
                ++ordNum;
            }

            Tuple docOrdTuple = fieldTuple.add(DOC_TO_ORD);
            int docNum = 0;
            Iterator<Number> ordIt = ords.iterator();
            for(Number ordCount : docToOrdCount) {
                Tuple docOrdDocTuple = docOrdTuple.add(docNum);
                for(int i = 0; i < ordCount.longValue(); ++i) {
                    long ordinal = ordIt.next().longValue();
                    set(dir.txn, docOrdDocTuple, ordinal);
                }
                ++docNum;
            }

            checkWritten(docNum);
        }

        @Override
        public void close() throws IOException {
            // None
        }

        private void checkWritten(int numWritten) {
            if(numWritten != expectedDocs) {
                throw new IllegalStateException("Expected " + expectedDocs + " docs to be written but saw " + numWritten);
            }
        }
    }
}
