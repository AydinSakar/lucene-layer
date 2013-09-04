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

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.MethodRule;
import org.junit.rules.TestName;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

public class TestBase
{
    /** Rule that implements retry logic. Saves each method from having (Partial)Function boilerplate. */
    private static class RetryRule implements MethodRule
    {
        @Override
        public Statement apply(final Statement base, FrameworkMethod method, Object target) {
            return new Statement()
            {
                @Override
                public void evaluate() throws Throwable {
                    try {
                        base.evaluate();
                    } catch(Exception e) {
                        if(testBaseTxn == null) {
                            throw e;
                        }
                        testBaseTxn.onError(e).get();
                    }
                }
            };
        }
    }


    protected static final FDB testBaseFDB;
    protected static final Database testBaseDB;
    protected static Transaction testBaseTxn;

    @Rule
    public final RetryRule retryRule = new RetryRule();

    @Rule
    public final TestName testName = new TestName();


    static {
        testBaseFDB = FDB.selectAPIVersion(100);
        testBaseDB = testBaseFDB.open();
    }


    @Before
    public void clearRootPrefix() {
        testBaseDB.run(
                new Function<Transaction, Void>()
                {
                    @Override
                    public Void apply(Transaction txn) {
                        txn.clear(Tuple.from(Util.DEFAULT_TEST_ROOT_PREFIX).range());
                        return null;
                    }
                }
        );
        testBaseTxn = testBaseDB.createTransaction();
    }

    @After
    public void clearTransaction() {
        if(testBaseTxn != null) {
            testBaseTxn.reset();
            testBaseTxn = null;
        }
    }


    protected FDBDirectory createDirectoryForMethod() {
        return createDirectory(testName.getMethodName());
    }

    protected FDBDirectory createDirectory(String subDirName) {
        return new FDBDirectory(Tuple.from(Util.DEFAULT_TEST_ROOT_PREFIX, subDirName), testBaseTxn);
    }
}
