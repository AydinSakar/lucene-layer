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

    // used by clone
    FDBTermVectorsReader(FDBDirectory dir, Tuple segmentTuple) {
        this.dir = dir;
        this.segmentTuple = segmentTuple;
    }

    private static class FieldBuilder
    {
        int num;
        String name;
        Boolean withPositions;
        Boolean withOffsets;
        Boolean withPayloads;
        TermVectorTerms terms;

        public FieldBuilder(int num) {
            this.num = num;
        }
    }

    @Override
    public Fields get(int doc) throws IOException {
        final Tuple docTuple = segmentTuple.add(doc);

        SortedMap<String, TermVectorTerms> fields = new TreeMap<String, TermVectorTerms>();

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
            String part = keyTuple.getString(docTuple.size() + 1);

            if(builder == null) {
                builder = new FieldBuilder(fieldNum);
            } else if(fieldNum != builder.num) {
                fields.put(builder.name, builder.terms);
                builder = new FieldBuilder(fieldNum);
            }
            if(FIELD_NAME.equals(part)) {
                builder.name = valueTuple.getString(0);
            } else if(FIELD_POSITIONS.equals(part)) {
                builder.withPositions = valueTuple.getLong(0) == 1;
            } else if(FIELD_OFFSETS.equals(part)) {
                builder.withOffsets = valueTuple.getLong(0) == 1;
            } else if(FIELD_PAYLOADS.equals(part)) {
                builder.withPayloads = valueTuple.getLong(0) == 1;
            } else if(TERM.equals(part)) {
                kv = loadTerms(it, docTuple.size(), builder, kv);
                continue;
            } else {
                throw new IllegalStateException("Unexpected data part: " + part);
            }
            kv = null;
        }

        if(builder != null) {
            fields.put(builder.name, builder.terms);
            builder = null;
        }

        return new TermVectorFields(fields);
    }

    private static KeyValue loadTerms(Iterator<KeyValue> it, int fieldNumIndex, FieldBuilder builder, KeyValue initKV) {
        final int FIELD_PART_INDEX = fieldNumIndex + 1;
        final int TERM_NUM_INDEX = fieldNumIndex + 2;
        final int TERM_POS_INDEX = fieldNumIndex + 3;
        final int TERM_POS_PART_INDEX = fieldNumIndex + 4;

        final KeyValue outKV;

        int lastTermNum = -1;
        int posIndex = -1;
        FDBTermVectorsPostings postings = null;
        final SortedMap<BytesRef, FDBTermVectorsPostings> terms = new TreeMap<BytesRef, FDBTermVectorsPostings>();

        boolean first = true;
        for(; ; ) {
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
            if(fieldNum != builder.num) {
                outKV = kv;
                break;
            }

            assert TERM.equals(keyTuple.getString(FIELD_PART_INDEX));

            int termNum = (int)keyTuple.getLong(TERM_NUM_INDEX);

            if(termNum != lastTermNum) {
                assert (lastTermNum == -1) == (postings == null);

                // Should always see metadata key first
                assert keyTuple.size() == (TERM_NUM_INDEX + 1);
                lastTermNum = termNum;
                posIndex = -1;

                BytesRef term = new BytesRef(valueTuple.getBytes(0));
                postings = new FDBTermVectorsPostings();
                postings.freq = (int)valueTuple.getLong(1);
                terms.put(term, postings);

                if(builder.withPositions) {
                    postings.positions = new int[postings.freq];
                    if(builder.withPayloads) {
                        postings.payloads = new BytesRef[postings.freq];
                    }
                }

                if(builder.withOffsets) {
                    postings.startOffsets = new int[postings.freq];
                    postings.endOffsets = new int[postings.freq];
                }
            } else {
                // Position data
                assert builder.withPositions || builder.withOffsets;

                int posNum = (int)keyTuple.getLong(TERM_POS_INDEX);
                if(keyTuple.size() == TERM_POS_INDEX + 1) {
                    ++posIndex;
                    postings.positions[posIndex] = posNum;
                } else if(keyTuple.size() == TERM_POS_PART_INDEX + 1) {
                    String part = keyTuple.getString(TERM_POS_PART_INDEX);
                    if(PAYLOAD.equals(part)) {
                        assert builder.withPayloads;
                        postings.payloads[posIndex] = valueTuple.size() == 0 ? null : new BytesRef(valueTuple.getBytes(0));
                    } else if(START_OFFSET.equals(part)) {
                        assert builder.withOffsets;
                        postings.startOffsets[posIndex] = (int)valueTuple.getLong(0);
                    } else if(END_OFFSET.equals(part)) {
                        assert builder.withOffsets;
                        postings.endOffsets[posIndex] = (int)valueTuple.getLong(0);
                    } else {
                        throw new IllegalStateException("Unexpected position info: " + part);
                    }
                } else {
                    throw new IllegalStateException("Unexpected keyTuple size: " + keyTuple.size());
                }
            }
        }

        builder.terms = new TermVectorTerms(builder.withOffsets, builder.withPositions, builder.withPayloads, terms);
        return outKV;
    }


    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override
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

    private class TermVectorFields extends Fields
    {
        private final SortedMap<String, TermVectorTerms> fields;

        TermVectorFields(SortedMap<String, TermVectorTerms> fields) {
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

    private static class TermVectorTerms extends FDBTermsBase
    {
        final SortedMap<BytesRef, FDBTermVectorsPostings> terms;

        public TermVectorTerms(boolean hasOffsets,
                               boolean hasPositions,
                               boolean hasPayloads,
                               SortedMap<BytesRef, FDBTermVectorsPostings> terms) {
            super(hasOffsets, hasPositions, hasPayloads, terms.size(), -1, 1);
            this.terms = terms;
        }

        @Override
        public TermsEnum iterator(TermsEnum reuse) throws IOException {
            return new FDBTermVectorsTermsEnum(terms);
        }
    }

    private static class FDBTermVectorsPostings
    {
        private int freq;
        private int positions[];
        private int startOffsets[];
        private int endOffsets[];
        private BytesRef payloads[];
    }

    private static class FDBTermVectorsTermsEnum extends TermsEnum
    {
        private final SortedMap<BytesRef, FDBTermVectorsPostings> terms;
        private Iterator<Map.Entry<BytesRef, FDBTermVectorsPostings>> iterator;
        private Map.Entry<BytesRef, FDBTermVectorsPostings> current;

        FDBTermVectorsTermsEnum(SortedMap<BytesRef, FDBTermVectorsPostings> terms) {
            this.terms = terms;
            this.iterator = terms.entrySet().iterator();
        }

        @Override
        public SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException {
            iterator = terms.tailMap(text).entrySet().iterator();
            if(!iterator.hasNext()) {
                return SeekStatus.END;
            } else {
                return next().equals(text) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
            }
        }

        @Override
        public void seekExact(long ord) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef next() throws IOException {
            if(!iterator.hasNext()) {
                return null;
            } else {
                current = iterator.next();
                return current.getKey();
            }
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
            FDBTermVectorsDocsAndPositionsEnum e = new FDBTermVectorsDocsAndPositionsEnum();
            e.reset(freq, liveDocs, null, null, null, null);
            return e;
        }

        @Override
        public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) {
            FDBTermVectorsPostings postings = current.getValue();
            if(postings.positions == null && postings.startOffsets == null) {
                return null;
            }
            // TODO: reuse?
            FDBTermVectorsDocsAndPositionsEnum e = new FDBTermVectorsDocsAndPositionsEnum();
            e.reset(null, liveDocs, postings.positions, postings.startOffsets, postings.endOffsets, postings.payloads);
            return e;
        }

        @Override
        public Comparator<BytesRef> getComparator() {
            return BytesRef.getUTF8SortedAsUnicodeComparator();
        }
    }

    private static class FDBTermVectorsDocsAndPositionsEnum extends DocsAndPositionsEnum
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
            assert startOffsets != null;
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
                return (doc = 0);
            } else {
                return (doc = NO_MORE_DOCS);
            }
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
            } else {
                nextPos++;
                return -1;
            }
        }

        @Override
        public int startOffset() {
            if(startOffsets == null) {
                return -1;
            } else {
                return startOffsets[nextPos - 1];
            }
        }

        @Override
        public int endOffset() {
            if(endOffsets == null) {
                return -1;
            } else {
                return endOffsets[nextPos - 1];
            }
        }

        @Override
        public long cost() {
            return 1;
        }
    }
}
