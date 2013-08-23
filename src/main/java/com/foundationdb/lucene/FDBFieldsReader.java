package com.foundationdb.lucene;

import com.foundationdb.KeyValue;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static com.foundationdb.lucene.FDBDirectory.copyRange;
import static com.foundationdb.lucene.FDBDirectory.unpackLongForAtomic;

class FDBFieldsReader extends FieldsProducer
{
    private final FieldInfos fieldInfos;
    private final FDBDirectory dir;
    private final Tuple segmentTuple;


    public FDBFieldsReader(SegmentReadState state) throws IOException {
        fieldInfos = state.fieldInfos;
        dir = FDBDirectory.unwrapFDBDirectory(state.directory);
        segmentTuple = dir.subspace.add(state.segmentInfo.name).add("pst");
    }

    private class FDBTextTermsEnum extends TermsEnum
    {
        private final IndexOptions indexOptions;
        private final Tuple fieldTuple;
        private BytesRef foundTerm = null;
        private int docFreq;

        public FDBTextTermsEnum(Tuple fieldTuple, IndexOptions indexOptions) {
            this.indexOptions = indexOptions;
            this.fieldTuple = fieldTuple;
        }

        @Override
        public boolean seekExact(BytesRef text, boolean useCache /* ignored */) throws IOException {
            return seekCeil(text, useCache) == SeekStatus.FOUND;
        }

        @Override
        public SeekStatus seekCeil(BytesRef text, boolean useCache /* ignored */) throws IOException {
            List<KeyValue> range = dir.txn
                                      .getRange(fieldTuple.add(copyRange(text)).pack(), fieldTuple.range().end, 1)
                                      .asList()
                                      .get();
            if(range.isEmpty()) {
                return SeekStatus.END;
            }

            Tuple t = Tuple.fromBytes(range.get(0).getKey());
            byte[] term = t.getBytes(fieldTuple.size());
            foundTerm = new BytesRef(term);

            // NOTE: "numDocs" delicate
            docFreq = (int) unpackLongForAtomic(range.get(0).getValue());

            if(foundTerm.equals(text)) {
                return SeekStatus.FOUND;
            } else {
                return SeekStatus.NOT_FOUND;
            }
        }

        @Override
        public BytesRef next() throws IOException {
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
        public long ord() throws IOException {
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
        public DocsAndPositionsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
            FDBTextDocsAndPositionsEnum docsAndPositionsEnum;
            if(reuse != null && reuse instanceof FDBTextDocsAndPositionsEnum && ((FDBTextDocsAndPositionsEnum) reuse).canReuse(
                    FDBFieldsReader.this
            )) {
                docsAndPositionsEnum = (FDBTextDocsAndPositionsEnum) reuse;
            } else {
                docsAndPositionsEnum = new FDBTextDocsAndPositionsEnum();
            }
            Tuple termTuple = fieldTuple.add(copyRange(foundTerm));
            //System.out.println("reset term: " + foundTerm);
            return docsAndPositionsEnum.reset(termTuple, liveDocs, indexOptions, docFreq);
        }

        @Override
        public DocsAndPositionsEnum docsAndPositions(Bits liveDocs,
                                                     DocsAndPositionsEnum reuse,
                                                     int flags) throws IOException {
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

    private class FDBTextDocsAndPositionsEnum extends DocsAndPositionsEnum
    {
        private Tuple termTuple;
        private Bits liveDocs;
        private int docFreq;
        private Iterator<KeyValue> termIterator;
        private int docID = -1;
        private int termFreq;

        private boolean readOffsets;
        private boolean readPositions;


        public FDBTextDocsAndPositionsEnum() {
        }

        public boolean canReuse(FDBFieldsReader reader) {
            return FDBFieldsReader.this == reader;
        }

        public FDBTextDocsAndPositionsEnum reset(Tuple termTuple,
                                                 Bits liveDocs,
                                                 IndexOptions indexOptions,
                                                 int docFreq) {
            this.termTuple = termTuple;
            this.liveDocs = liveDocs;
            docID = -1;
            readPositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
            readOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
            this.docFreq = docFreq;

            //System.out.println("resetRange: " + niceTupleStr(termTuple));
            this.termIterator = dir.txn.getRange(termTuple.add(0).pack(), termTuple.range().end).iterator();
            return this;
        }


        @Override
        public int docID() {
            return docID;
        }

        @Override
        public int freq() throws IOException {
            return termFreq;
        }

        @Override
        public int nextDoc() throws IOException {
            // Exhausted
            if(docID == NO_MORE_DOCS) {
                return docID;
            }

            while(termIterator.hasNext()) {
                KeyValue kv = termIterator.next();
                Tuple curTuple = Tuple.fromBytes(kv.getKey());
                //System.out.println("  saw: " + niceTupleStr(curTuple));
                if(curTuple.size() > (termTuple.size() + 1)) {
                    continue; // This is a position
                }
                int curDocID = (int) curTuple.getLong(termTuple.size());
                if(liveDocs == null || liveDocs.get(docID)) {
                    docID = curDocID;
                    termFreq = (int) Tuple.fromBytes(kv.getValue()).getLong(0);
                }
                return docID;
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
        public int nextPosition() throws IOException {
            if(!readPositions) {
                return -1;
            }

            assert termIterator.hasNext();
            KeyValue kv = termIterator.next();
            Tuple curTuple = Tuple.fromBytes(kv.getKey());
            assert curTuple.size() == (termTuple.size() + 2);
            return (int) curTuple.getLong(termTuple.size() + 1);

            // TODO: offsets, payloads
        }

        @Override
        public int startOffset() throws IOException {
            return -1;
        }

        @Override
        public int endOffset() throws IOException {
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

    private class FDBTextTerms extends Terms
    {
        private final FieldInfo fieldInfo;
        final Tuple fieldTuple;

        public FDBTextTerms(FieldInfo fieldInfo) throws IOException {
            this.fieldInfo = fieldInfo;
            this.fieldTuple = segmentTuple.add(fieldInfo.number);
        }

        @Override
        public TermsEnum iterator(TermsEnum reuse) throws IOException {
            return new FDBTextTermsEnum(fieldTuple, fieldInfo.getIndexOptions());
        }

        @Override
        public Comparator<BytesRef> getComparator() {
            return BytesRef.getUTF8SortedAsUnicodeComparator();
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public long getSumTotalTermFreq() {
            return -1;
            //return fieldInfo.getIndexOptions() == IndexOptions.DOCS_ONLY ? -1 : sumTotalTermFreq;
        }

        @Override
        public long getSumDocFreq() throws IOException {
            return -1;
        }

        @Override
        public int getDocCount() throws IOException {
            return -1;
        }

        @Override
        public boolean hasOffsets() {
            return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        }

        @Override
        public boolean hasPositions() {
            return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        }

        @Override
        public boolean hasPayloads() {
            return fieldInfo.hasPayloads();
        }
    }

    @Override
    public Iterator<String> iterator() {
        // Note: (just?) Test requires these to be sorted by name
        TreeSet<String> set = new TreeSet<String>();
        for(FieldInfo fi : fieldInfos) {
            if(fi.isIndexed()) {
                set.add(fi.name);
            }
        }
        return set.iterator();
    }

    private final Map<String, Terms> termsCache = new HashMap<String, Terms>();

    @Override
    synchronized public Terms terms(String field) throws IOException {
        Terms terms = termsCache.get(field);
        if(terms == null) {
            FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
            if(fieldInfo != null) {
                terms = new FDBTextTerms(fieldInfo);
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
    public void close() throws IOException {
    }
}
