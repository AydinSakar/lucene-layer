/**
 * FoundationDB Lucene Layer
 * Copyright (c) 2013 FoundationDB, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.foundationdb.lucene;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class FDBNormsFormat extends NormsFormat
{
    private static final String NORMS_EXT = "len";

    @Override
    public DocValuesConsumer normsConsumer(SegmentWriteState state) throws IOException {
        return new FDBNormsConsumer(state);
    }

    @Override
    public DocValuesProducer normsProducer(SegmentReadState state) throws IOException {
        return new FDBNormsProducer(state);
    }

    public static class FDBNormsProducer extends FDBDocValuesFormat.FDBDocValuesProducer
    {
        public FDBNormsProducer(SegmentReadState state) throws IOException {
            super(state, NORMS_EXT);
        }
    }

    public static class FDBNormsConsumer extends FDBDocValuesFormat.FDBDocValuesConsumer
    {
        public FDBNormsConsumer(SegmentWriteState state) throws IOException {
            super(state, NORMS_EXT);
        }
    }
}
