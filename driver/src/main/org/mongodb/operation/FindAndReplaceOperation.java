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

import org.mongodb.Decoder;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.command.FindAndModifyCommandResult;
import org.mongodb.command.FindAndModifyCommandResultCodec;
import org.mongodb.command.FindAndReplaceCommand;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.session.Session;

public class FindAndReplaceOperation<T> extends OperationBase<T> {
    private final FindAndReplace<T> findAndReplace;
    private final MongoNamespace namespace;
    private final PrimitiveCodecs primitiveCodecs;
    private final Decoder<T> decoder;
    private final ClusterDescription clusterDescription;

    public FindAndReplaceOperation(final BufferProvider bufferProvider, final Session session, final boolean closeSession,
                                   final ClusterDescription clusterDescription, final MongoNamespace namespace,
                                   final FindAndReplace<T> findAndReplace, final PrimitiveCodecs primitiveCodecs,
                                   final Decoder<T> decoder) {
        super(bufferProvider, session, closeSession);
        this.findAndReplace = findAndReplace;
        this.namespace = namespace;
        this.primitiveCodecs = primitiveCodecs;
        this.decoder = decoder;
        this.clusterDescription = clusterDescription;
    }

    @Override
    public T execute() {
        final FindAndReplaceCommand<T> command = new FindAndReplaceCommand<T>(findAndReplace, namespace.getCollectionName());
        return new FindAndModifyCommandResult<T>(new CommandOperation(namespace.getDatabaseName(), command,
                                                                      new FindAndModifyCommandResultCodec<T>(primitiveCodecs, decoder),
                                                                      clusterDescription, getBufferProvider(), getSession(), false)
                                                 .execute()).getValue();
    }
}
