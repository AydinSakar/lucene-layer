package com.foundationdb.lucene;

import com.foundationdb.KeyValue;
import com.foundationdb.MutationType;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.foundationdb.lucene.FDBDirectory.copyRange;
import static com.foundationdb.lucene.FDBDirectory.unpackLongForAtomic;

public final class FDBPostingsFormat extends PostingsFormat
{
    private static final String POSTINGS_EXTENSION = "pst";
    private static final byte[] ATOMIC_OP_LONG_0 = { 0, 0, 0, 0, 0, 0, 0, 0 };
    private static final byte[] ATOMIC_OP_LONG_1 = { 1, 0, 0, 0, 0, 0, 0, 0 };


    public FDBPostingsFormat() {
        super(FDBPostingsFormat.class.getSimpleName());
    }


    //
    // PostingsFormat
    //

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) {
        return new FDBFieldsProducer(state);
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) {
        return new FDBFieldsConsumer(state);
    }


    //
    // FieldsProducer (reading)
    //

    private static class FDBFieldsProducer extends FieldsProducer
    {
        private final Map<String, FDBPostingsTerms> termsCache = new HashMap<String, FDBPostingsTerms>();

        private final FieldInfos fieldInfos;
        private final FDBDirectory dir;
        private final Tuple segmentTuple;


        public FDBFieldsProducer(SegmentReadState state) {
            this.fieldInfos = state.fieldInfos;
            this.dir = FDBDirectory.unwrapFDBDirectory(state.directory);
            this.segmentTuple = dir.subspace.add(state.segmentInfo.name).add(POSTINGS_EXTENSION);
        }

        @Override
        public Iterator<String> iterator() {
            // Note: (just?) Test requires these to be sorted by name
            Set<String> set = new TreeSet<String>();
            for(FieldInfo fi : fieldInfos) {
                if(fi.isIndexed()) {
                    set.add(fi.name);
                }
            }
            return set.iterator();
        }

        @Override
        synchronized public FDBPostingsTerms terms(String field) {
            FDBPostingsTerms terms = termsCache.get(field);
            if(terms == null) {
                FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
                if(fieldInfo != null) {
                    terms = new FDBPostingsTerms(dir.txn, fieldInfo, segmentTuple);
                    termsCache.put(field, terms);
                }
            }
            return terms;
        }

        @Override
        public int size() {
            return -1;
        }

        @Override
        public void close() {
        }
    }


    //
    // FieldsConsumer (writing)
    //

    private static class FDBFieldsConsumer extends FieldsConsumer
    {
        private final FDBDirectory dir;
        private final Tuple segmentTuple;

        public FDBFieldsConsumer(SegmentWriteState state) {
            dir = FDBDirectory.unwrapFDBDirectory(state.directory);
            segmentTuple = dir.subspace.add(state.segmentInfo.name).add(POSTINGS_EXTENSION);
        }

        @Override
        public FDBTermsConsumer addField(FieldInfo field) {
            return new FDBTermsConsumer(dir.txn, field, segmentTuple);
        }

        @Override
        public void close() {
        }
    }


    //
    // Reading helpers
    //

    private static class FDBPostingsTerms extends FDBTermsBase
    {
        private final Transaction txn;
        private final FieldInfo fieldInfo;
        private final Tuple fieldTuple;

        public FDBPostingsTerms(Transaction txn, FieldInfo fieldInfo, Tuple segmentTuple) {
            super(
                    fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0,
                    fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0,
                    fieldInfo.hasPayloads(),
                    -1,
                    -1,
                    -1
            );
            this.txn = txn;
            this.fieldInfo = fieldInfo;
            this.fieldTuple = segmentTuple.add(fieldInfo.number);
        }

        @Override
        public FDBTermsEnum iterator(TermsEnum reuse) {
            return new FDBTermsEnum(txn, fieldTuple, fieldInfo.getIndexOptions());
        }

        @Override
        public long getSumTotalTermFreq() {
            return -1;
            //return fieldInfo.getIndexOptions() == IndexOptions.DOCS_ONLY ? -1 : sumTotalTermFreq;
        }
    }

    private static class FDBTermsEnum extends TermsEnum
    {
        private final Transaction txn;
        private final IndexOptions indexOptions;
        private final Tuple fieldTuple;
        private BytesRef foundTerm = null;
        private int docFreq;

        public FDBTermsEnum(Transaction txn, Tuple fieldTuple, IndexOptions indexOptions) {
            this.txn = txn;
            this.indexOptions = indexOptions;
            this.fieldTuple = fieldTuple;
        }

        @Override
        public boolean seekExact(BytesRef text, boolean useCache) {
            return seekCeil(text, useCache) == SeekStatus.FOUND;
        }

        @Override
        public SeekStatus seekCeil(BytesRef text, boolean useCache) {
            List<KeyValue> range = txn.getRange(fieldTuple.add(copyRange(text)).pack(), fieldTuple.range().end, 1)
                                      .asList()
                                      .get();
            if(range.isEmpty()) {
                return SeekStatus.END;
            }

            Tuple t = Tuple.fromBytes(range.get(0).getKey());
            byte[] term = t.getBytes(fieldTuple.size());
            foundTerm = new BytesRef(term);

            // NOTE: "numDocs" delicate
            docFreq = (int)unpackLongForAtomic(range.get(0).getValue());

            if(foundTerm.equals(text)) {
                return SeekStatus.FOUND;
            } else {
                return SeekStatus.NOT_FOUND;
            }
        }

        @Override
        public BytesRef next() {
            if(foundTerm == null) {
                foundTerm = new BytesRef();
            } else {
                foundTerm.append(new BytesRef(new byte[]{ 0 }));
            }
            SeekStatus status = seekCeil(foundTerm, false);
            return (status == SeekStatus.END) ? null : term();
        }

        @Override
        public BytesRef term() {
            assert foundTerm != null;
            return foundTerm;
        }

        @Override
        public long ord() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seekExact(long ord) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docFreq() {
            return docFreq;
        }

        @Override
        public long totalTermFreq() {
            return -1;
            //return indexOptions == IndexOptions.DOCS_ONLY ? -1 : totalTermFreq;
        }

        @Override
        public FDBDocsAndPositionsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) {
            // TODO: Reuse?
            Tuple termTuple = fieldTuple.add(copyRange(foundTerm));
            return new FDBDocsAndPositionsEnum(txn, termTuple, liveDocs, indexOptions, docFreq);
        }

        @Override
        public FDBDocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) {
            if(indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                // Positions were not indexed
                return null;
            }
            return docs(liveDocs, reuse, flags);
        }

        @Override
        public Comparator<BytesRef> getComparator() {
            return BytesRef.getUTF8SortedAsUnicodeComparator();
        }
    }

    private static class FDBDocsAndPositionsEnum extends DocsAndPositionsEnum
    {
        private Tuple termTuple;
        private Bits liveDocs;
        private int docFreq;
        private int termFreq;
        private int docID;
        private Iterator<KeyValue> termIterator;

        private boolean readOffsets;
        private boolean readPositions;


        public FDBDocsAndPositionsEnum(Transaction txn,
                                       Tuple termTuple,
                                       Bits liveDocs,
                                       IndexOptions options,
                                       int docFreq) {
            this.termTuple = termTuple;
            this.liveDocs = liveDocs;
            this.docFreq = docFreq;
            this.docID = -1;
            this.termIterator = txn.getRange(termTuple.add(0).pack(), termTuple.range().end).iterator();

            this.readPositions = options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
            this.readOffsets = options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        }


        @Override
        public int docID() {
            return docID;
        }

        @Override
        public int freq() {
            return termFreq;
        }

        @Override
        public int nextDoc() {
            // Exhausted
            if(docID == NO_MORE_DOCS) {
                return docID;
            }

            while(termIterator.hasNext()) {
                KeyValue kv = termIterator.next();
                Tuple curTuple = Tuple.fromBytes(kv.getKey());
                if(curTuple.size() > (termTuple.size() + 1)) {
                    // This is a position
                    continue;
                }
                int curDocID = (int)curTuple.getLong(termTuple.size());
                if(liveDocs == null || liveDocs.get(curDocID)) {
                    docID = curDocID;
                    termFreq = (int)Tuple.fromBytes(kv.getValue()).getLong(0);
                    return docID;
                }
            }

            docID = NO_MORE_DOCS;
            return docID;
        }

        @Override
        public int advance(int docIDTarget) throws IOException {
            // Naive -- better to index skip data
            return slowAdvance(docIDTarget);
        }

        @Override
        public int nextPosition() {
            if(!readPositions) {
                return -1;
            }

            assert termIterator.hasNext();
            KeyValue kv = termIterator.next();
            Tuple curTuple = Tuple.fromBytes(kv.getKey());
            assert curTuple.size() == (termTuple.size() + 2);
            return (int)curTuple.getLong(termTuple.size() + 1);

            // TODO: offsets, payloads
        }

        @Override
        public int startOffset() {
            return -1;
        }

        @Override
        public int endOffset() {
            return -1;
        }

        @Override
        public BytesRef getPayload() {
            return null;
        }

        @Override
        public long cost() {
            return docFreq;
        }
    }


    //
    // Writing helpers
    //

    private static class FDBTermsConsumer extends TermsConsumer
    {
        private final FDBPostingsConsumer postingsConsumer;
        private final Tuple fieldTuple;

        public FDBTermsConsumer(Transaction txn, FieldInfo field, Tuple segmentTuple) {
            this.postingsConsumer = new FDBPostingsConsumer(txn, field);
            this.fieldTuple = segmentTuple.add(field.number);
        }

        @Override
        public FDBPostingsConsumer startTerm(BytesRef term) {
            return postingsConsumer.startTerm(term, fieldTuple);
        }

        @Override
        public void finishTerm(BytesRef term, TermStats stats) {
        }

        @Override
        public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) {
        }

        @Override
        public Comparator<BytesRef> getComparator() {
            return BytesRef.getUTF8SortedAsUnicodeComparator();
        }
    }

    private static class FDBPostingsConsumer extends PostingsConsumer
    {
        private final Transaction txn;
        private final IndexOptions indexOptions;
        private final boolean writePositions;
        private final boolean writeOffsets;
        private Tuple termTuple = null;
        private Tuple docTuple = null;


        public FDBPostingsConsumer(Transaction txn, FieldInfo field) {
            this.txn = txn;
            this.indexOptions = field.getIndexOptions();
            writePositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
            writeOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
            assert !writeOffsets;
        }

        public FDBPostingsConsumer startTerm(BytesRef term, Tuple fieldTuple) {
            this.termTuple = fieldTuple.add(Arrays.copyOfRange(term.bytes, term.offset, term.offset + term.length));
            txn.set(termTuple.add("numDocs").pack(), ATOMIC_OP_LONG_0);
            return this;
        }

        @Override
        public void startDoc(int docID, int termDocFreq) {
            docTuple = termTuple.add(docID);
            txn.mutate(MutationType.ADD, termTuple.add("numDocs").pack(), ATOMIC_OP_LONG_1);
            txn.set(docTuple.pack(), Tuple.from(termDocFreq).pack());
            // if(indexOptions != IndexOptions.DOCS_ONLY) {
        }

        @Override
        public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) {
            if(writePositions) {
                txn.set(docTuple.add(position).pack(), new byte[0]);
            }
            assert payload == null;
                /*
                if(writeOffsets) {
                    lastStartOffset = startOffset;
                    write(START_OFFSET);
                    write(Integer.toString(startOffset));
                    newline();
                    write(END_OFFSET);
                    write(Integer.toString(endOffset));
                    newline();
                }

                if(payload != null && payload.length > 0) {
                    assert payload.length != 0;
                    write(PAYLOAD);
                    write(payload);
                    newline();
                }
                */
        }

        @Override
        public void finishDoc() {
        }
    }
}
