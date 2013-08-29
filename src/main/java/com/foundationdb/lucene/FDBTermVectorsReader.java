package com.foundationdb.lucene;

import com.foundationdb.KeyValue;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static com.foundationdb.lucene.FDBTermVectorsWriter.*;

public class FDBTermVectorsReader extends TermVectorsReader
{
    private final FDBDirectory dir;
    private final Tuple segmentTuple;
    private final FieldInfos fieldInfos;

    public FDBTermVectorsReader(Directory dirIn, SegmentInfo si, FieldInfos fi) {
        this.dir = FDBDirectory.unwrapFDBDirectory(dirIn);
        this.segmentTuple = dir.subspace.add(si.name).add(VECTORS_EXTENSION);
        this.fieldInfos = fi;
    }

    private FDBTermVectorsReader(FDBDirectory dir, Tuple segmentTuple, FieldInfos fi) {
        this.dir = dir;
        this.segmentTuple = segmentTuple;
        this.fieldInfos = fi;
    }

    @Override
    public Fields get(int doc) {
        List<TVField> fields = new ArrayList<TVField>();
        for(KeyValue kv : dir.txn.getRange(segmentTuple.add(doc).add(FIELDS).range())) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey());
            Tuple valueTuple = Tuple.fromBytes(kv.getValue());
            fields.add(
                    new TVField(
                            keyTuple.getString(keyTuple.size() - 1),
                            doc,
                            (int)valueTuple.getLong(1),
                            valueTuple.getLong(2) == 1,
                            valueTuple.getLong(3) == 1,
                            valueTuple.getLong(4) == 1
                    )
            );
        }
        return new TVFields(fields.toArray(new TVField[fields.size()]));
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public TermVectorsReader clone() {
        // TODO: needed?
        //if(in == null) {
        //    throw new AlreadyClosedException("this TermVectorsReader is closed");
        //}
        return new FDBTermVectorsReader(dir, segmentTuple, fieldInfos);
    }

    @Override
    public void close() {
        // None
    }


    //
    // Helpers
    //

    private TVPostings loadPostings(TVField field, int freq, Tuple termTuple) {
        TVPostings postings = new TVPostings(freq, field.hasPositions, field.hasPayloads, field.hasOffsets);
        int index = 0;
        for(KeyValue kv : dir.txn.getRange(termTuple.range())) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey());
            assert keyTuple.size() == termTuple.size() + 1 : "Unexpected key: " + FDBDirectory.tupleStr(keyTuple);

            Tuple valueTuple = Tuple.fromBytes(kv.getValue());
            if(field.hasPositions) {
                postings.positions[index] = (int)keyTuple.getLong(keyTuple.size() - 1);
            }
            if(field.hasOffsets) {
                postings.startOffsets[index] = (int)valueTuple.getLong(0);
                postings.endOffsets[index] = (int)valueTuple.getLong(1);
            }
            if(field.hasPayloads) {
                postings.payloads[0] = new BytesRef(valueTuple.getBytes(2));
            }
            ++index;
        }
        return postings;
    }

    private static class TVField
    {
        private final String name;
        private final int docNum;
        private final int numTerms;
        private final boolean hasPositions;
        private final boolean hasOffsets;
        private final boolean hasPayloads;

        private TVField(String name, int docNum, int numTerms, boolean hasPositions, boolean hasOffsets, boolean hasPayloads) {
            this.name = name;
            this.docNum = docNum;
            this.numTerms = numTerms;
            this.hasPositions = hasPositions;
            this.hasOffsets = hasOffsets;
            this.hasPayloads = hasPayloads;
        }
    }

    private class TVFields extends Fields
    {
        private final TVField[] fields;

        public TVFields(TVField[] fields) {
            this.fields = fields;
        }

        @Override
        public Iterator<String> iterator() {
            return new Iterator<String>()
            {
                int curIndex = 0;

                @Override
                public boolean hasNext() {
                    return curIndex < fields.length;
                }

                @Override
                public String next() {
                    return fields[curIndex++].name;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public Terms terms(String fieldName) {
            for(TVField field : fields) {
                if(field.name.equals(fieldName)) {
                    return new TVTerms(field);
                }
            }
            return null;
        }

        @Override
        public int size() {
            return fields.length;
        }
    }

    private class TVTerms extends FDBTermsBase
    {
        final TVField field;

        public TVTerms(TVField field) {
            super(field.hasPositions, field.hasOffsets, field.hasPayloads, field.numTerms, -1, 1);
            this.field = field;
        }

        @Override
        public TermsEnum iterator(TermsEnum reuse) throws IOException {
            return new TVTermsEnum(field);
        }
    }

    private class TVPostings
    {
        public final int freq;
        public final int[] positions;
        public final int[] startOffsets;
        public final int[] endOffsets;
        public final BytesRef[] payloads;

        public TVPostings(int freq, boolean withPositions, boolean withPayload, boolean withOffsets) {
            this.freq = freq;
            this.positions = withPositions ? new int[freq] : null;
            this.payloads = withPayload ? new BytesRef[freq] : null;
            this.startOffsets = withOffsets ? new int[freq] : null;
            this.endOffsets = withOffsets ? new int[freq] : null;
        }
    }

    /** Iterate in terms order over all terms for a single field. * */
    private class TVTermsEnum extends TermsEnum
    {
        private final TVField field;
        private final Tuple termsTuple;
        private Iterator<KeyValue> it;
        private BytesRef curTerm;
        private int curFreq;

        public TVTermsEnum(TVField field) {
            this.field = field;
            this.termsTuple = segmentTuple.add(field.docNum).add(TERMS).add(field.name);
        }

        private void advance() {
            if(it == null) {
                // next() immediately after constructed. Position to first term for this field.
                it = dir.txn.getRange(termsTuple.range()).iterator();
            }
            curTerm = null;
            curFreq = -1;
            while(it.hasNext()) {
                KeyValue kv = it.next();
                Tuple keyTuple = Tuple.fromBytes(kv.getKey());
                if(keyTuple.size() == (termsTuple.size() + 1)) {
                    curTerm = new BytesRef(keyTuple.getBytes(keyTuple.size() - 1));
                    curFreq = (int)Tuple.fromBytes(kv.getValue()).getLong(0);
                    break;
                }
            }
        }

        @Override
        public SeekStatus seekCeil(BytesRef text, boolean useCache) {
            byte[] begin = termsTuple.add(FDBDirectory.copyRange(text)).pack();
            byte[] end = termsTuple.range().end;
            it = dir.txn.getRange(begin, end).iterator();
            advance();
            if(curTerm == null) {
                return SeekStatus.END;
            }
            return curTerm.equals(text) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
        }

        @Override
        public void seekExact(long ord) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef next() {
            advance();
            return curTerm;
        }

        @Override
        public BytesRef term() {
            return curTerm;
        }

        @Override
        public long ord() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docFreq() {
            return 1;
        }

        @Override
        public long totalTermFreq() throws IOException {
            return curFreq;
        }

        @Override
        public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) {
            // TODO: reuse?
            int freq = (flags & DocsEnum.FLAG_FREQS) == 0 ? 1 : curFreq;
            return new TVDocsAndPositionsEnum(freq, liveDocs);
        }

        @Override
        public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) {
            // TODO: reuse?
            Tuple postingsTuple = termsTuple.add(FDBDirectory.copyRange(curTerm));
            TVPostings postings = loadPostings(field, curFreq, postingsTuple);
            if(postings.positions == null && postings.startOffsets == null) {
                return null;
            }
            return new TVDocsAndPositionsEnum(liveDocs, postings);
        }

        @Override
        public Comparator<BytesRef> getComparator() {
            return BytesRef.getUTF8SortedAsUnicodeComparator();
        }
    }

    private static class TVDocsAndPositionsEnum extends DocsAndPositionsEnum
    {
        private final Integer freq;
        private final Bits liveDocs;
        private final int[] positions;
        private final int[] startOffsets;
        private final int[] endOffsets;
        private final BytesRef[] payloads;
        private boolean didNext;
        private int nextPos;
        private int doc = -1;

        /** DocsEnum */
        public TVDocsAndPositionsEnum(int freq, Bits liveDocs) {
            this(freq, liveDocs, null);
        }

        /** DocsAndPositionsEnum * */
        public TVDocsAndPositionsEnum(Bits liveDocs, TVPostings postings) {
            this(null, liveDocs, postings);
        }

        private TVDocsAndPositionsEnum(Integer freq, Bits liveDocs, TVPostings postings) {
            this.freq = freq;
            this.liveDocs = liveDocs;
            this.positions = (postings != null) ? postings.positions : null;
            this.startOffsets = (postings != null) ? postings.startOffsets : null;
            this.endOffsets = (postings != null) ? postings.endOffsets : null;
            this.payloads = (postings != null) ? postings.payloads : null;
            this.doc = -1;
            this.didNext = false;
            this.nextPos = 0;
        }

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
