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
