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

import org.mongodb.CollectibleCodec;
import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.command.FindAndModifyCommandResultCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.operation.protocol.CommandWithPayloadProtocol;
import org.mongodb.session.PrimaryServerSelector;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

public class FindAndReplaceOperation<T> extends OperationBase<T> {
    private final FindAndReplace<T> findAndReplace;
    private final MongoNamespace namespace;
    private final FindAndModifyCommandResultCodec<T> findAndModifyCommandResultCodec;
    private final CollectibleCodec<T> payloadCodec;

    public FindAndReplaceOperation(final BufferProvider bufferProvider, final Session session, final boolean closeSession,
                                   final MongoNamespace namespace, final FindAndReplace<T> findAndReplace,
                                   final PrimitiveCodecs primitiveCodecs, final Decoder<T> decoder,
                                   final CollectibleCodec<T> payloadCodec) {
        super(bufferProvider, session, closeSession);
        this.findAndReplace = findAndReplace;
        this.namespace = namespace;
        this.payloadCodec = payloadCodec;
        //need to change this to be more inline with the payload codec
        findAndModifyCommandResultCodec = new FindAndModifyCommandResultCodec<T>(primitiveCodecs, decoder);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T execute() {
        final ServerConnectionProvider provider = getServerConnectionProvider();
        //TODO: CommandResult can be genericised?
        final CommandResult commandResult = new CommandWithPayloadProtocol<T>(namespace.getDatabaseName(), payloadCodec,
                                                                              findAndReplace.toDocument(),
                                                                              findAndModifyCommandResultCodec,
                                                                              getBufferProvider(), provider.getServerDescription(),
                                                                              provider.getConnection(), true).execute();
        return (T) commandResult.getResponse().get("value");
        // TODO: any way to remove the warning?  This could be a design flaw
    }

    private ServerConnectionProvider getServerConnectionProvider() {
        return getSession().createServerConnectionProvider(new ServerConnectionProviderOptions(false, new PrimaryServerSelector()));
    }


    //    private final FindAndReplace<T> findAndReplace;
    //    private final MongoNamespace namespace;
    //    private final PrimitiveCodecs primitiveCodecs;
    //    private final Decoder<T> decoder;
    //    private final ClusterDescription clusterDescription;
    //
    //    public FindAndReplaceOperation(final BufferProvider bufferProvider, final Session session, final boolean closeSession,
    //                                   final ClusterDescription clusterDescription, final MongoNamespace namespace,
    //                                   final FindAndReplace<T> findAndReplace, final PrimitiveCodecs primitiveCodecs,
    //                                   final Encoder<T> encoder, final Decoder<T> decoder) {
    //        super(bufferProvider, session, closeSession);
    //        this.findAndReplace = findAndReplace;
    //        this.namespace = namespace;
    //        //for encoding query
    //        this.primitiveCodecs = primitiveCodecs;
    //        this.decoder = decoder;
    //        this.clusterDescription = clusterDescription;
    //    }
    //
    //    @Override
    //    public T execute() {
    //        final FindAndReplaceCommand<T> command = new FindAndReplaceCommand<T>(findAndReplace, namespace.getCollectionName());
    //        return new FindAndModifyCommandResult<T>(new CommandOperation(namespace.getDatabaseName(), command,
    //                                                                      new FindAndModifyCommandResultCodec<T>(PrimitiveCodecs
    // .createDefault(), decoder),
    //                                                                      clusterDescription, getBufferProvider(), getSession(), false)
    //                                                 .execute()).getValue();
    //    }

}
