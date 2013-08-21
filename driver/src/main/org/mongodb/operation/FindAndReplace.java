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

import org.mongodb.ConvertibleToDocument;
import org.mongodb.Document;

import static org.mongodb.operation.CommandDocumentTemplates.findAndModifyDocument;

public class FindAndReplace<T> extends FindAndModify implements ConvertibleToDocument {
    private final T replacement;
    private final String collectionName;

    public FindAndReplace(final T replacement, final String collectionName) {
        this.replacement = replacement;
        this.collectionName = collectionName;
    }

    @Override
    public FindAndReplace<T> sortBy(final Document sortCriteria) {
        super.sortBy(sortCriteria);
        return this;
    }

    @Override
    public FindAndReplace<T> returnNew(final boolean returnNew) {
        super.returnNew(returnNew);
        return this;
    }

    @Override
    public FindAndReplace<T> upsert(final boolean upsert) {
        super.upsert(upsert);
        return this;
    }

    @Override
    public FindAndReplace<T> where(final Document filter) {
        super.where(filter);
        return this;
    }

    @Override
    public FindAndReplace<T> select(final Document selector) {
        super.select(selector);
        return this;
    }

    @Override
    public Document toDocument() {
        final Document cmd = findAndModifyDocument(this, collectionName);
        // TODO: I don't think this will work, as we don't have a Class<T> to make sure that serialization works properly
        cmd.put("update", replacement);
        return cmd;
    }
}
