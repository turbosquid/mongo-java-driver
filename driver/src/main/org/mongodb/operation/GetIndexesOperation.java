package org.mongodb.operation;

import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoCursor;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.connection.BufferProvider;
import org.mongodb.session.Session;

import java.util.ArrayList;
import java.util.List;

import static org.mongodb.ReadPreference.primary;
import static org.mongodb.assertions.Assertions.notNull;

public class GetIndexesOperation<T> extends BaseOperation<List<T>> {
    private static final String NAMESPACE_KEY_NAME = "ns";

    private final DocumentCodec simpleDocumentCodec = new DocumentCodec(PrimitiveCodecs.createDefault());
    private final MongoNamespace indexesNamespace;
    private final Find queryForCollectionNamespace;
    private final Decoder<T> resultDecoder;

    public GetIndexesOperation(final BufferProvider bufferProvider, final Session session, final MongoNamespace collectionNamespace,
                               final Decoder<T> resultDecoder) {
        super(bufferProvider, session, false);
        this.resultDecoder = notNull("resultDecoder", resultDecoder);
        notNull("collectionNamespace", collectionNamespace);
        indexesNamespace = new MongoNamespace(collectionNamespace.getDatabaseName(), "system.indexes");
        queryForCollectionNamespace = new Find(new Document(NAMESPACE_KEY_NAME, collectionNamespace.getFullName()))
                                      .readPreference(primary());
    }

    @Override
    public List<T> execute() {
        final List<T> retVal = new ArrayList<T>();
        final MongoCursor<T> cursor = new QueryOperation<T>(indexesNamespace, queryForCollectionNamespace,
                                                                          simpleDocumentCodec, resultDecoder, getBufferProvider(),
                                                                          getSession(), isCloseSession()).execute();
        while (cursor.hasNext()) {
            retVal.add(cursor.next());
        }
        return retVal;
    }
}
