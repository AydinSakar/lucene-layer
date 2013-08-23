package com.foundationdb.lucene;

import com.foundationdb.MutationType;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

class FDBFieldsWriter extends FieldsConsumer
{
    private final FDBDirectory dir;
    private final Tuple segmentTuple;

    public FDBFieldsWriter(SegmentWriteState state) throws IOException {
        dir = FDBDirectory.unwrapFDBDirectory(state.directory);
        segmentTuple = dir.subspace.add(state.segmentInfo.name).add("pst");
    }

    @Override
    public TermsConsumer addField(FieldInfo field) throws IOException {
        return new FDBTextTermsWriter(field);
    }

    private class FDBTextTermsWriter extends TermsConsumer
    {
        private final FDBTextPostingsWriter postingsWriter;
        private final Tuple fieldTuple;

        public FDBTextTermsWriter(FieldInfo field) {
            postingsWriter = new FDBTextPostingsWriter(field);
            this.fieldTuple = segmentTuple.add(field.number);
        }

        @Override
        public PostingsConsumer startTerm(BytesRef term) throws IOException {
            return postingsWriter.startTerm(term, fieldTuple);
        }

        @Override
        public void finishTerm(BytesRef term, TermStats stats) throws IOException {
        }

        @Override
        public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
        }

        @Override
        public Comparator<BytesRef> getComparator() {
            return BytesRef.getUTF8SortedAsUnicodeComparator();
        }
    }

    private class FDBTextPostingsWriter extends PostingsConsumer
    {
        private BytesRef term;
        private boolean wroteTerm;
        private final IndexOptions indexOptions;
        private final boolean writePositions;
        private final boolean writeOffsets;
        private Tuple termTuple = null;
        private Tuple docTuple = null;

        // for assert:
        private int lastStartOffset = 0;

        public FDBTextPostingsWriter(FieldInfo field) {
            //System.out.println("Postings fieldName: " + field.name + ", number: " + field.number);
            this.indexOptions = field.getIndexOptions();
            writePositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
            writeOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
            assert !writeOffsets;

            //System.out.println("writeOffsets=" + writeOffsets);
            //System.out.println("writePos=" + writePositions);
        }

        public PostingsConsumer startTerm(BytesRef term, Tuple fieldTuple) {
            //System.out.println("startTerm: " + term);
            this.term = term;
            this.termTuple = fieldTuple.add(Arrays.copyOfRange(term.bytes, term.offset, term.offset + term.length));
            dir.txn.set(termTuple.add("numDocs").pack(), new byte[]{ 0, 0, 0, 0, 0, 0, 0, 0 });
            return this;
        }

        @Override
        public void startDoc(int docID, int termDocFreq) throws IOException {
            //System.out.println("startDoc: " + docID);
            docTuple = termTuple.add(docID);
            dir.txn.mutate(MutationType.ADD, termTuple.add("numDocs").pack(), new byte[]{ 1, 0, 0, 0, 0, 0, 0, 0 });
            dir.txn.set(docTuple.pack(), Tuple.from(termDocFreq).pack());
            // if(indexOptions != IndexOptions.DOCS_ONLY) {
            lastStartOffset = 0;
        }

        @Override
        public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
            if(writePositions) {
                dir.txn.set(docTuple.add(position).pack(), new byte[0]);
            }
            assert payload == null;
/*
            if(writeOffsets) {
                assert endOffset >= startOffset;
                assert startOffset >= lastStartOffset : "startOffset=" + startOffset + " lastStartOffset=" + lastStartOffset;
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

    @Override
    public void close() throws IOException {
    }
}
