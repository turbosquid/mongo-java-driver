package org.mongodb.operation;

import org.mongodb.Decoder;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.command.FindAndModifyCommandResult;
import org.mongodb.command.FindAndModifyCommandResultCodec;
import org.mongodb.command.FindAndUpdateCommand;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.session.Session;

public class FindAndUpdateOperation<T> extends OperationBase<T> {
    private final FindAndUpdate<T> findAndUpdate;
    private final ClusterDescription clusterDescription;
    private final PrimitiveCodecs primitiveCodecs;
    private final Decoder<T> resultDecoder;
    private final MongoNamespace namespace;

    public FindAndUpdateOperation(final BufferProvider bufferProvider, final Session session, final boolean closeSession,
                                  final ClusterDescription clusterDescription, final MongoNamespace namespace,
                                  final FindAndUpdate<T> findAndUpdate, final PrimitiveCodecs primitiveCodecs,
                                  final Decoder<T> resultDecoder) {
        super(bufferProvider, session, closeSession);
        this.findAndUpdate = findAndUpdate;
        this.clusterDescription = clusterDescription;
        this.primitiveCodecs = primitiveCodecs;
        this.resultDecoder = resultDecoder;
        this.namespace = namespace;
    }

    @Override
    public T execute() {
        try {
            final FindAndUpdateCommand<T> findAndUpdateCommand = new FindAndUpdateCommand<T>(findAndUpdate, namespace.getCollectionName());

            final FindAndModifyCommandResultCodec<T> codec = new FindAndModifyCommandResultCodec<T>(primitiveCodecs, resultDecoder);

            return new FindAndModifyCommandResult<T>(new CommandOperation(namespace.getDatabaseName(), findAndUpdateCommand, codec,
                                                                          clusterDescription, getBufferProvider(), getSession(),
                                                                          isCloseSession()).execute()).getValue();
        } finally {
            if (isCloseSession()) {
                getSession().close();
            }
        }
    }

}
