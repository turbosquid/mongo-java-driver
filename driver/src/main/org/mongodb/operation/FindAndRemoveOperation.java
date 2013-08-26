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

import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.connection.BufferProvider;
import org.mongodb.operation.protocol.CommandProtocol;
import org.mongodb.session.PrimaryServerSelector;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

public class FindAndRemoveOperation<T> extends OperationBase<T> {
    private final MongoNamespace namespace;
    private final FindAndRemove<T> findAndRemove;
    private final CommandResultWithPayloadDecoder<T> commandResultWithPayloadDecoder;

    public FindAndRemoveOperation(final BufferProvider bufferProvider, final Session session, final boolean closeSession,
                                  final MongoNamespace namespace, final FindAndRemove<T> findAndRemove,
                                  final PrimitiveCodecs primitiveCodecs, final Decoder<T> decoder) {
        super(bufferProvider, session, closeSession);
        this.namespace = namespace;
        this.findAndRemove = findAndRemove;
        commandResultWithPayloadDecoder = new CommandResultWithPayloadDecoder<T>(decoder);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T execute() {
        final ServerConnectionProvider provider = getServerConnectionProvider();
        final CommandResult commandResult = new CommandProtocol(namespace.getDatabaseName(), findAndRemove.toDocument(),
                                                                new DocumentCodec(PrimitiveCodecs.createDefault()),
                                                                commandResultWithPayloadDecoder, getBufferProvider(),
                                                                provider.getServerDescription(), provider.getConnection(), true
        ).execute();
        return (T) commandResult.getResponse().get("value");
        // TODO: any way to remove the warning?  This could be a design flaw
    }

    private ServerConnectionProvider getServerConnectionProvider() {
        return getSession().createServerConnectionProvider(new ServerConnectionProviderOptions(false, new PrimaryServerSelector()));
    }

}
