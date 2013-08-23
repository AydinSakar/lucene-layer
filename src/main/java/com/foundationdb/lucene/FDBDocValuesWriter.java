package com.foundationdb.lucene;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;

class FDBDocValuesWriter extends DocValuesConsumer
{
    final static BytesRef END = new BytesRef("END");
    final static BytesRef FIELD = new BytesRef("field ");
    final static BytesRef TYPE = new BytesRef("  type ");
    // used for numerics
    final static BytesRef MINVALUE = new BytesRef("  minvalue ");
    final static BytesRef PATTERN = new BytesRef("  pattern ");
    // used for bytes
    final static BytesRef LENGTH = new BytesRef("length ");
    final static BytesRef MAXLENGTH = new BytesRef("  maxlength ");
    // used for sorted bytes
    final static BytesRef NUMVALUES = new BytesRef("  numvalues ");
    final static BytesRef ORDPATTERN = new BytesRef("  ordpattern ");

    final IndexOutput data;
    final BytesRef scratch = new BytesRef();
    final int numDocs;
    private final Set<String> fieldsSeen = new HashSet<String>(); // for asserting

    public FDBDocValuesWriter(SegmentWriteState state, String ext) throws IOException {
        //System.out.println("WRITE: " + IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ext) + " " + state.segmentInfo.getDocCount() + " docs");
        data = state.directory
                    .createOutput(
                            IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, ext),
                            state.context
                    );
        numDocs = state.segmentInfo.getDocCount();
    }

    // for asserting
    private boolean fieldSeen(String field) {
        assert !fieldsSeen.contains(field) : "field \"" + field + "\" was added more than once during flush";
        fieldsSeen.add(field);
        return true;
    }

    @Override
    public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
        assert fieldSeen(field.name);
        assert (field.getDocValuesType() == DocValuesType.NUMERIC || field.getNormType() == DocValuesType.NUMERIC);
        writeFieldEntry(field, DocValuesType.NUMERIC);

        // first pass to find min/max
        long minValue = Long.MAX_VALUE;
        long maxValue = Long.MIN_VALUE;
        for(Number n : values) {
            long v = n.longValue();
            minValue = Math.min(minValue, v);
            maxValue = Math.max(maxValue, v);
        }

        // write our minimum value to the .dat, all entries are deltas from that
        FDBUtil.write(data, MINVALUE);
        FDBUtil.write(data, Long.toString(minValue), scratch);
        FDBUtil.writeNewline(data);

        // build up our fixed-width "simple text packed ints"
        // format
        BigInteger maxBig = BigInteger.valueOf(maxValue);
        BigInteger minBig = BigInteger.valueOf(minValue);
        BigInteger diffBig = maxBig.subtract(minBig);
        int maxBytesPerValue = diffBig.toString().length();
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < maxBytesPerValue; i++) {
            sb.append('0');
        }

        // write our pattern to the .dat
        FDBUtil.write(data, PATTERN);
        FDBUtil.write(data, sb.toString(), scratch);
        FDBUtil.writeNewline(data);

        final String patternString = sb.toString();

        final DecimalFormat encoder = new DecimalFormat(patternString, new DecimalFormatSymbols(Locale.ROOT));

        int numDocsWritten = 0;

        // second pass to write the values
        for(Number n : values) {
            long value = n.longValue();
            assert value >= minValue;
            Number delta = BigInteger.valueOf(value).subtract(BigInteger.valueOf(minValue));
            String s = encoder.format(delta);
            assert s.length() == patternString.length();
            FDBUtil.write(data, s, scratch);
            FDBUtil.writeNewline(data);
            numDocsWritten++;
            assert numDocsWritten <= numDocs;
        }

        assert numDocs == numDocsWritten : "numDocs=" + numDocs + " numDocsWritten=" + numDocsWritten;
    }

    @Override
    public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
        assert fieldSeen(field.name);
        assert field.getDocValuesType() == DocValuesType.BINARY;
        int maxLength = 0;
        for(BytesRef value : values) {
            maxLength = Math.max(maxLength, value.length);
        }
        writeFieldEntry(field, DocValuesType.BINARY);

        // write maxLength
        FDBUtil.write(data, MAXLENGTH);
        FDBUtil.write(data, Integer.toString(maxLength), scratch);
        FDBUtil.writeNewline(data);

        int maxBytesLength = Long.toString(maxLength).length();
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < maxBytesLength; i++) {
            sb.append('0');
        }
        // write our pattern for encoding lengths
        FDBUtil.write(data, PATTERN);
        FDBUtil.write(data, sb.toString(), scratch);
        FDBUtil.writeNewline(data);
        final DecimalFormat encoder = new DecimalFormat(sb.toString(), new DecimalFormatSymbols(Locale.ROOT));

        int numDocsWritten = 0;
        for(BytesRef value : values) {
            // write length
            FDBUtil.write(data, LENGTH);
            FDBUtil.write(data, encoder.format(value.length), scratch);
            FDBUtil.writeNewline(data);

            // write bytes -- don't use FDBText.write
            // because it escapes:
            data.writeBytes(value.bytes, value.offset, value.length);

            // pad to fit
            for(int i = value.length; i < maxLength; i++) {
                data.writeByte((byte) ' ');
            }
            FDBUtil.writeNewline(data);
            numDocsWritten++;
        }

        assert numDocs == numDocsWritten;
    }

    @Override
    public void addSortedField(FieldInfo field,
                               Iterable<BytesRef> values,
                               Iterable<Number> docToOrd) throws IOException {
        assert fieldSeen(field.name);
        assert field.getDocValuesType() == DocValuesType.SORTED;
        writeFieldEntry(field, DocValuesType.SORTED);

        int valueCount = 0;
        int maxLength = -1;
        for(BytesRef value : values) {
            maxLength = Math.max(maxLength, value.length);
            valueCount++;
        }

        // write numValues
        FDBUtil.write(data, NUMVALUES);
        FDBUtil.write(data, Integer.toString(valueCount), scratch);
        FDBUtil.writeNewline(data);

        // write maxLength
        FDBUtil.write(data, MAXLENGTH);
        FDBUtil.write(data, Integer.toString(maxLength), scratch);
        FDBUtil.writeNewline(data);

        int maxBytesLength = Integer.toString(maxLength).length();
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < maxBytesLength; i++) {
            sb.append('0');
        }

        // write our pattern for encoding lengths
        FDBUtil.write(data, PATTERN);
        FDBUtil.write(data, sb.toString(), scratch);
        FDBUtil.writeNewline(data);
        final DecimalFormat encoder = new DecimalFormat(sb.toString(), new DecimalFormatSymbols(Locale.ROOT));

        int maxOrdBytes = Integer.toString(valueCount).length();
        sb.setLength(0);
        for(int i = 0; i < maxOrdBytes; i++) {
            sb.append('0');
        }

        // write our pattern for ords
        FDBUtil.write(data, ORDPATTERN);
        FDBUtil.write(data, sb.toString(), scratch);
        FDBUtil.writeNewline(data);
        final DecimalFormat ordEncoder = new DecimalFormat(sb.toString(), new DecimalFormatSymbols(Locale.ROOT));

        // for asserts:
        int valuesSeen = 0;

        for(BytesRef value : values) {
            // write length
            FDBUtil.write(data, LENGTH);
            FDBUtil.write(data, encoder.format(value.length), scratch);
            FDBUtil.writeNewline(data);

            // write bytes -- don't use FDBText.write
            // because it escapes:
            data.writeBytes(value.bytes, value.offset, value.length);

            // pad to fit
            for(int i = value.length; i < maxLength; i++) {
                data.writeByte((byte) ' ');
            }
            FDBUtil.writeNewline(data);
            valuesSeen++;
            assert valuesSeen <= valueCount;
        }

        assert valuesSeen == valueCount;

        for(Number ord : docToOrd) {
            FDBUtil.write(data, ordEncoder.format(ord.intValue()), scratch);
            FDBUtil.writeNewline(data);
        }
    }

    @Override
    public void addSortedSetField(FieldInfo field,
                                  Iterable<BytesRef> values,
                                  Iterable<Number> docToOrdCount,
                                  Iterable<Number> ords) throws IOException {
        assert fieldSeen(field.name);
        assert field.getDocValuesType() == DocValuesType.SORTED_SET;
        writeFieldEntry(field, DocValuesType.SORTED_SET);

        long valueCount = 0;
        int maxLength = 0;
        for(BytesRef value : values) {
            maxLength = Math.max(maxLength, value.length);
            valueCount++;
        }

        // write numValues
        FDBUtil.write(data, NUMVALUES);
        FDBUtil.write(data, Long.toString(valueCount), scratch);
        FDBUtil.writeNewline(data);

        // write maxLength
        FDBUtil.write(data, MAXLENGTH);
        FDBUtil.write(data, Integer.toString(maxLength), scratch);
        FDBUtil.writeNewline(data);

        int maxBytesLength = Integer.toString(maxLength).length();
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < maxBytesLength; i++) {
            sb.append('0');
        }

        // write our pattern for encoding lengths
        FDBUtil.write(data, PATTERN);
        FDBUtil.write(data, sb.toString(), scratch);
        FDBUtil.writeNewline(data);
        final DecimalFormat encoder = new DecimalFormat(sb.toString(), new DecimalFormatSymbols(Locale.ROOT));

        // compute ord pattern: this is funny, we encode all values for all docs to find the maximum length
        int maxOrdListLength = 0;
        StringBuilder sb2 = new StringBuilder();
        Iterator<Number> ordStream = ords.iterator();
        for(Number n : docToOrdCount) {
            sb2.setLength(0);
            int count = n.intValue();
            for(int i = 0; i < count; i++) {
                long ord = ordStream.next().longValue();
                if(sb2.length() > 0) {
                    sb2.append(",");
                }
                sb2.append(Long.toString(ord));
            }
            maxOrdListLength = Math.max(maxOrdListLength, sb2.length());
        }

        sb2.setLength(0);
        for(int i = 0; i < maxOrdListLength; i++) {
            sb2.append('X');
        }

        // write our pattern for ord lists
        FDBUtil.write(data, ORDPATTERN);
        FDBUtil.write(data, sb2.toString(), scratch);
        FDBUtil.writeNewline(data);

        // for asserts:
        long valuesSeen = 0;

        for(BytesRef value : values) {
            // write length
            FDBUtil.write(data, LENGTH);
            FDBUtil.write(data, encoder.format(value.length), scratch);
            FDBUtil.writeNewline(data);

            // write bytes -- don't use FDBText.write
            // because it escapes:
            data.writeBytes(value.bytes, value.offset, value.length);

            // pad to fit
            for(int i = value.length; i < maxLength; i++) {
                data.writeByte((byte) ' ');
            }
            FDBUtil.writeNewline(data);
            valuesSeen++;
            assert valuesSeen <= valueCount;
        }

        assert valuesSeen == valueCount;

        ordStream = ords.iterator();

        // write the ords for each doc comma-separated
        for(Number n : docToOrdCount) {
            sb2.setLength(0);
            int count = n.intValue();
            for(int i = 0; i < count; i++) {
                long ord = ordStream.next().longValue();
                if(sb2.length() > 0) {
                    sb2.append(",");
                }
                sb2.append(Long.toString(ord));
            }
            // now pad to fit: these are numbers so spaces work well. reader calls trim()
            int numPadding = maxOrdListLength - sb2.length();
            for(int i = 0; i < numPadding; i++) {
                sb2.append(' ');
            }
            FDBUtil.write(data, sb2.toString(), scratch);
            FDBUtil.writeNewline(data);
        }
    }

    /** write the header for this field */
    private void writeFieldEntry(FieldInfo field, DocValuesType type) throws IOException {
        FDBUtil.write(data, FIELD);
        FDBUtil.write(data, field.name, scratch);
        FDBUtil.writeNewline(data);

        FDBUtil.write(data, TYPE);
        FDBUtil.write(data, type.toString(), scratch);
        FDBUtil.writeNewline(data);
    }

    @Override
    public void close() throws IOException {
        boolean success = false;
        try {
            assert !fieldsSeen.isEmpty();
            // TODO: sheisty to do this here?
            FDBUtil.write(data, END);
            FDBUtil.writeNewline(data);
            success = true;
        } finally {
            if(success) {
                IOUtils.close(data);
            } else {
                IOUtils.closeWhileHandlingException(data);
            }
        }
    }
}
