/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.operation;

import org.junit.Test;
import org.mongodb.ReadPreference;

import java.util.EnumSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class QueryTest {
    @Test
    public void testNumberToReturn() {
        Query query = new Find();
        assertEquals(0, query.getNumberToReturn());

        query = new Find().limit(-10);
        assertEquals(-10, query.getNumberToReturn());

        query = new Find().batchSize(10);
        assertEquals(10, query.getNumberToReturn());

        query = new Find().limit(10);
        assertEquals(10, query.getNumberToReturn());

        query = new Find().limit(10).batchSize(15);
        assertEquals(10, query.getNumberToReturn());

        query = new Find().limit(10).batchSize(-15);
        assertEquals(10, query.getNumberToReturn());

        query = new Find().limit(10).batchSize(7);
        assertEquals(7, query.getNumberToReturn());

        query = new Find().limit(10).batchSize(-7);
        assertEquals(-7, query.getNumberToReturn());
    }

    @Test
    public void testOptions() {
        Query query = new Find();
        assertEquals(EnumSet.noneOf(QueryFlag.class), query.getOptions().getFlags());

        query.addFlags(EnumSet.of(QueryFlag.Tailable));
        assertEquals(EnumSet.of(QueryFlag.Tailable), query.getOptions().getFlags());

        query.addFlags(EnumSet.of(QueryFlag.SlaveOk));
        assertEquals(EnumSet.of(QueryFlag.Tailable, QueryFlag.SlaveOk), query.getOptions().getFlags());

        try {
            query.options(null);
            fail();
        } catch (IllegalArgumentException e) {  // NOPMD
            // all good
        }

        try {
            query.addFlags(null);
            fail();
        } catch (IllegalArgumentException e) { // NOPMD
            // all good
        }
    }

    @Test
    public void testCopyConstructor() {
        Find query = new Find();
        query.addFlags(EnumSet.allOf(QueryFlag.class)).readPreference(ReadPreference.primary()).batchSize(2).limit(5).skip(1);
        Query copy = new Find(query);
        assertEquals(EnumSet.allOf(QueryFlag.class), copy.getOptions().getFlags());
        assertEquals(ReadPreference.primary(), copy.getReadPreference());
        assertEquals(2, copy.getBatchSize());
        assertEquals(5, copy.getLimit());
        assertEquals(1, copy.getSkip());
    }
}
