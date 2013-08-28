package com.foundationdb.lucene;

import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.foundationdb.lucene.FDBTermVectorsWriter.*;

public class FDBTermVectorsReader extends TermVectorsReader
{
    private final FDBDirectory dir;
    private final Tuple segmentTuple;

    public FDBTermVectorsReader(Directory dirIn, SegmentInfo si) {
        this.dir = FDBDirectory.unwrapFDBDirectory(dirIn);
        this.segmentTuple = dir.subspace.add(si.name).add(VECTORS_EXTENSION);
    }

    private FDBTermVectorsReader(FDBDirectory dir, Tuple segmentTuple) {
        this.dir = dir;
        this.segmentTuple = segmentTuple;
    }

    private static class FieldBuilder
    {
        int fieldNum;
        String name;
        Boolean hasPositions;
        Boolean hasOffsets;
        Boolean hasPayloads;
        FDBTermVectorTerms terms;

        public FieldBuilder(int fieldNum) {
            this.fieldNum = fieldNum;
        }
    }

    @Override
    public Fields get(int doc) throws IOException {
        final Tuple docTuple = segmentTuple.add(doc);

        SortedMap<String, FDBTermVectorTerms> fields = new TreeMap<String, FDBTermVectorTerms>();

        Transaction txn = dir.txn;
        Iterator<KeyValue> it = txn.getRange(docTuple.range()).iterator();

        FieldBuilder builder = null;

        KeyValue kv = null;
        for(; ; ) {
            if(kv == null) {
                if(!it.hasNext()) {
                    break;
                }
                kv = it.next();
            }
            Tuple keyTuple = Tuple.fromBytes(kv.getKey());
            Tuple valueTuple = Tuple.fromBytes(kv.getValue());

            int fieldNum = (int)keyTuple.getLong(docTuple.size());

            if(builder == null || fieldNum != builder.fieldNum) {
                if(builder != null) {
                    fields.put(builder.name, builder.terms);
                }
                builder = new FieldBuilder(fieldNum);
            }

            if(keyTuple.size() == (docTuple.size() + 1)) {
                // Field start
                builder.name = valueTuple.getString(0);
                builder.hasPositions = valueTuple.getLong(1) == 1;
                builder.hasOffsets = valueTuple.getLong(2) == 1;
                builder.hasPayloads = valueTuple.getLong(3) == 1;
            } else if(keyTuple.size() == (docTuple.size() + 2)) {
                // Term data
                kv = loadTerms(it, docTuple.size(), builder, kv);
                continue;
            } else {
                throw new IllegalStateException("Unexpected tuple: " + FDBDirectory.tupleStr(keyTuple));
            }
            kv = null;
        }

        if(builder != null) {
            fields.put(builder.name, builder.terms);
        }

        return new FDBTermVectorFields(fields);
    }

    private static KeyValue loadTerms(Iterator<KeyValue> it, int fieldNumIndex, FieldBuilder builder, KeyValue initKV) {
        final int TERM_NUM_INDEX = fieldNumIndex + 1;
        final int TERM_POS_INDEX = fieldNumIndex + 2;

        final KeyValue outKV;

        int lastTermNum = -1;
        int posIndex = -1;
        FDBTermVectorPostings postings = null;
        final SortedMap<BytesRef, FDBTermVectorPostings> terms = new TreeMap<BytesRef, FDBTermVectorPostings>();

        boolean first = true;
        for(;;) {
            final KeyValue kv;
            if(first) {
                kv = initKV;
                first = false;
            } else {
                if(!it.hasNext()) {
                    outKV = null;
                    break;
                }
                kv = it.next();
            }

            Tuple keyTuple = Tuple.fromBytes(kv.getKey());
            Tuple valueTuple = Tuple.fromBytes(kv.getValue());

            int fieldNum = (int)keyTuple.getLong(fieldNumIndex);
            if(fieldNum != builder.fieldNum) {
                outKV = kv;
                break;
            }

            int termNum = (int)keyTuple.getLong(TERM_NUM_INDEX);
            if(termNum != lastTermNum) {
                // Should always see metadata key first
                assert (lastTermNum == -1) == (postings == null);
                assert keyTuple.size() == (TERM_NUM_INDEX + 1);

                lastTermNum = termNum;
                posIndex = -1;

                BytesRef term = new BytesRef(valueTuple.getBytes(0));
                int freq = (int)valueTuple.getLong(1);
                postings = new FDBTermVectorPostings(freq, builder.hasPositions, builder.hasPayloads, builder.hasOffsets);
                terms.put(term, postings);
            } else if(keyTuple.size() == (TERM_POS_INDEX + 1)) {
                // Position data
                assert builder.hasPositions || builder.hasOffsets || builder.hasPayloads;

                ++posIndex;
                if(builder.hasPositions) {
                    postings.positions[posIndex] = (int)keyTuple.getLong(TERM_POS_INDEX);
                }
                if(builder.hasOffsets) {
                    postings.startOffsets[posIndex] = (int)valueTuple.getLong(0);
                    postings.endOffsets[posIndex] = (int)valueTuple.getLong(1);
                }
                if(builder.hasPayloads) {
                    postings.payloads[posIndex] = new BytesRef(valueTuple.getBytes(2));
                }
            } else {
                throw new IllegalStateException("Unexpected keyTuple size: " + keyTuple.size());
            }
        }

        builder.terms = new FDBTermVectorTerms(builder.hasOffsets, builder.hasPositions, builder.hasPayloads, terms);
        return outKV;
    }



    @Override @SuppressWarnings("CloneDoesntCallSuperClone")
    public TermVectorsReader clone() {
        // TODO: needed?
        //if(in == null) {
        //    throw new AlreadyClosedException("this TermVectorsReader is closed");
        //}
        return new FDBTermVectorsReader(dir, segmentTuple);
    }

    @Override
    public void close() {
        // None
    }

    private class FDBTermVectorFields extends Fields
    {
        private final SortedMap<String, FDBTermVectorTerms> fields;

        FDBTermVectorFields(SortedMap<String, FDBTermVectorTerms> fields) {
            this.fields = fields;
        }

        @Override
        public Iterator<String> iterator() {
            return Collections.unmodifiableSet(fields.keySet()).iterator();
        }

        @Override
        public Terms terms(String field) throws IOException {
            return fields.get(field);
        }

        @Override
        public int size() {
            return fields.size();
        }
    }

    private static class FDBTermVectorTerms extends FDBTermsBase
    {
        final SortedMap<BytesRef, FDBTermVectorPostings> terms;

        public FDBTermVectorTerms(boolean hasOffsets,
                                  boolean hasPositions,
                                  boolean hasPayloads,
                                  SortedMap<BytesRef, FDBTermVectorPostings> terms) {
            super(hasOffsets, hasPositions, hasPayloads, terms.size(), -1, 1);
            this.terms = terms;
        }

        @Override
        public TermsEnum iterator(TermsEnum reuse) throws IOException {
            return new FDBTermVectorTermsEnum(terms);
        }
    }

    private static class FDBTermVectorPostings
    {
        public final int freq;
        public final int[] positions;
        public final int[] startOffsets;
        public final int[] endOffsets;
        public final BytesRef[] payloads;

        public FDBTermVectorPostings(int freq, boolean withPositions, boolean withPayload, boolean withOffsets) {
            this.freq = freq;
            this.positions = withPositions ? new int[freq] : null;
            this.payloads = withPayload ? new BytesRef[freq] : null;
            this.startOffsets = withOffsets ? new int[freq] : null;
            this.endOffsets = withOffsets ? new int[freq] : null;
        }
    }

    private static class FDBTermVectorTermsEnum extends TermsEnum
    {
        private final SortedMap<BytesRef, FDBTermVectorPostings> terms;
        private Iterator<Map.Entry<BytesRef, FDBTermVectorPostings>> iterator;
        private Map.Entry<BytesRef, FDBTermVectorPostings> current;

        FDBTermVectorTermsEnum(SortedMap<BytesRef, FDBTermVectorPostings> terms) {
            this.terms = terms;
            this.iterator = terms.entrySet().iterator();
        }

        @Override
        public SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException {
            iterator = terms.tailMap(text).entrySet().iterator();
            if(!iterator.hasNext()) {
                return SeekStatus.END;
            }
            return next().equals(text) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
        }

        @Override
        public void seekExact(long ord) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef next() throws IOException {
            if(!iterator.hasNext()) {
                return null;
            }
            current = iterator.next();
            return current.getKey();
        }

        @Override
        public BytesRef term() throws IOException {
            return current.getKey();
        }

        @Override
        public long ord() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docFreq() throws IOException {
            return 1;
        }

        @Override
        public long totalTermFreq() throws IOException {
            return current.getValue().freq;
        }

        @Override
        public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) {
            // TODO: reuse?
            int freq = (flags & DocsEnum.FLAG_FREQS) == 0 ? 1 : current.getValue().freq;
            FDBTermVectorDocsAndPositionsEnum e = new FDBTermVectorDocsAndPositionsEnum();
            e.reset(freq, liveDocs, null, null, null, null);
            return e;
        }

        @Override
        public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) {
            FDBTermVectorPostings postings = current.getValue();
            if(postings.positions == null && postings.startOffsets == null) {
                return null;
            }
            // TODO: reuse?
            FDBTermVectorDocsAndPositionsEnum e = new FDBTermVectorDocsAndPositionsEnum();
            e.reset(null, liveDocs, postings.positions, postings.startOffsets, postings.endOffsets, postings.payloads);
            return e;
        }

        @Override
        public Comparator<BytesRef> getComparator() {
            return BytesRef.getUTF8SortedAsUnicodeComparator();
        }
    }

    private static class FDBTermVectorDocsAndPositionsEnum extends DocsAndPositionsEnum
    {
        private Integer freq;
        private boolean didNext;
        private int doc = -1;
        private int nextPos;
        private Bits liveDocs;
        private int[] positions;
        private BytesRef[] payloads;
        private int[] startOffsets;
        private int[] endOffsets;

        @Override
        public int freq() throws IOException {
            if(freq != null) {
                return freq;
            }
            if(positions != null) {
                return positions.length;
            }
            return startOffsets.length;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            if(!didNext && (liveDocs == null || liveDocs.get(0))) {
                didNext = true;
                doc = 0;
            } else {
                doc = NO_MORE_DOCS;
            }
            return doc;
        }

        @Override
        public int advance(int target) throws IOException {
            return slowAdvance(target);
        }

        public void reset(Integer freq,
                          Bits liveDocs,
                          int[] positions,
                          int[] startOffsets,
                          int[] endOffsets,
                          BytesRef payloads[]) {
            this.freq = freq;
            this.liveDocs = liveDocs;
            this.positions = positions;
            this.startOffsets = startOffsets;
            this.endOffsets = endOffsets;
            this.payloads = payloads;
            this.doc = -1;
            didNext = false;
            nextPos = 0;
        }

        @Override
        public BytesRef getPayload() {
            return payloads == null ? null : payloads[nextPos - 1];
        }

        @Override
        public int nextPosition() {
            assert (positions != null && nextPos < positions.length) || startOffsets != null && nextPos < startOffsets.length;
            if(positions != null) {
                return positions[nextPos++];
            }
            nextPos++;
            return -1;
        }

        @Override
        public int startOffset() {
            return (startOffsets == null) ? -1 : startOffsets[nextPos - 1];
        }

        @Override
        public int endOffset() {
            return (endOffsets == null) ? -1 : endOffsets[nextPos - 1];
        }

        @Override
        public long cost() {
            return 1;
        }
    }
}
