package com.foundationdb.lucene;

import com.foundationdb.KeyValue;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import java.util.Iterator;
import java.util.List;

import static com.foundationdb.lucene.FDBDocValuesConsumer.*;

//
// Reader
//
class FDBDocValuesProducer extends DocValuesProducer
{
    private final FDBDirectory dir;
    private final Tuple segmentTuple;


    public FDBDocValuesProducer(SegmentReadState state, String ext) {
        //System.out.println("Producer: " + state.segmentInfo.name +", " + state.segmentSuffix + ", " + ext); System.out.flush();
        this.dir = Util.unwrapDirectory(state.directory);
        this.segmentTuple = dir.subspace.add(state.segmentInfo.name).add(state.segmentSuffix).add(ext);
    }


    //
    // DocValuesProducer
    //

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
            result.bytes = Tuple.fromBytes(bytes).getBytes(0);
            result.offset = 0;
            result.length = result.bytes.length;
        }
    }

    private class FDBSortedDocValues extends SortedDocValues
    {
        private final Tuple sortedTuple;

        public FDBSortedDocValues(String fieldName) {
            //System.out.println("FDBSortedDocValues: " + fieldName); System.out.flush();
            this.sortedTuple = segmentTuple.add(fieldName).add(DocValuesType.SORTED.ordinal());
        }

        @Override
        public int getOrd(int docID) {
            // (dirTuple, segName, segSuffix, ext, fieldName, SORTED, "ord", docID) => (ordNum)
            byte[] bytes = dir.txn.get(sortedTuple.add(ORD).add(docID).pack()).get();
            assert bytes != null : "No ord for docID: " + docID;
            int ord = (int)Tuple.fromBytes(bytes).getLong(0);
            //System.out.println("  getOrd: docID: " + docID + ", ord: " + ord); System.out.flush();
            return ord;
        }

        @Override
        public void lookupOrd(int ord, BytesRef result) {
            byte[] bytes = dir.txn.get(sortedTuple.add(BYTES).add(ord).pack()).get();
            assert bytes != null : "No bytes for ord: " + ord;
            result.bytes = Tuple.fromBytes(bytes).getBytes(0);
            result.offset = 0;
            result.length = result.bytes.length;
            //System.out.println("  lookupOrd: ord: " + ord + ", result: " + new String(result.bytes)); System.out.flush();
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
            //System.out.println("  getValueCount: " + valueCount); System.out.flush();
            return valueCount;
        }
    }

    private class FDBSortedSetDocValues extends SortedSetDocValues
    {
        private final Tuple sortedSetTuple;
        private Iterator<KeyValue> ordIt = null;

        public FDBSortedSetDocValues(String fieldName) {
            //System.out.println("FDBSortedSetDocValues: " + fieldName); System.out.flush();
            this.sortedSetTuple = segmentTuple.add(fieldName).add(DocValuesType.SORTED_SET.ordinal());
        }

        @Override
        public long nextOrd() {
            if(!ordIt.hasNext()) {
                //System.out.println("  nextOrd: NO_MORE"); System.out.flush();
                return NO_MORE_ORDS;
            }
            KeyValue kv = ordIt.next();
            Tuple keyTuple = Tuple.fromBytes(kv.getKey());
            int ord = (int)keyTuple.getLong(keyTuple.size() - 1);
            //System.out.println("  nextOrd: " + ord); System.out.flush();
            return ord;
        }

        @Override
        public void setDocument(int docID) {
            //System.out.println("  setDocument: " + docID); System.out.flush();
            ordIt = dir.txn.getRange(sortedSetTuple.add(DOC_TO_ORD).add(docID).range()).iterator();
        }

        @Override
        public void lookupOrd(long ord, BytesRef result) {
            byte[] bytes = dir.txn.get(sortedSetTuple.add(BYTES).add(ord).pack()).get();
            assert bytes != null : "No bytes for ord: " + ord;
            result.bytes = Tuple.fromBytes(bytes).getBytes(0);
            result.offset = 0;
            result.length = result.bytes.length;
            //System.out.println("  lookupOrd: ord: " + ord + ", result: " + new String(result.bytes, result.offset, result.length)); System.out.flush();
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
            //System.out.println("  getValueCount: " + valueCount); System.out.flush();
            return valueCount;
        }
    }
}
