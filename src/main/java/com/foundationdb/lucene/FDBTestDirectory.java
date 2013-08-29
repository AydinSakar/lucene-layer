/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.lucene;

import com.foundationdb.Cluster;
import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.Tuple;

import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class FDBTestDirectory extends FDBDirectory
{
  private static final Random RND = new Random();
  private static final byte[] DEFAULT_DB_NAME = "DB".getBytes(Charset.forName("UTF8"));

  private static FDB fdb;
  private static Database fdbDB;

  public FDBTestDirectory() {
    super(String.format("dir_%d", RND.nextInt(100000)), newTransactionForTest());
  }


  /** Find the system thread group. By default, threads in this group won't trigger failures in Lucene/Solr tests. **/
  private static ThreadGroup getSystemThreadGroup() {
    ThreadGroup tg = Thread.currentThread().getThreadGroup();
    while(tg != null && !"system".equals(tg.getName())) {
      tg = tg.getParent();
    }
    return tg;
  }

  private static synchronized void initFDB() {
    FDBTestDirectory.fdb = FDB.selectAPIVersion(100);

    final ThreadGroup threadGroup = getSystemThreadGroup();
    ExecutorService executor = Executors.newCachedThreadPool(
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            return new Thread(threadGroup, r);
          }
        }
    );

    fdb.startNetwork(executor);
    Cluster cluster = fdb.createCluster(null, executor);
    FDBTestDirectory.fdbDB = cluster.openDatabase(DEFAULT_DB_NAME);
    FDBTestDirectory.fdbDB.run(
        new Function<Transaction, Void>()
        {
          @Override
          public Void apply(Transaction transaction) {
            transaction.clear(Tuple.from(ROOT_PREFIX).range());
            return null;
          }
        }
    );
  }

  private static synchronized Transaction newTransactionForTest() {
    if(fdb == null) {
      initFDB();
    }
    return fdbDB.createTransaction();
  }
}
