package org.mongodb.operation;

import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoCursor;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.BufferProvider;
import org.mongodb.session.Session;

import java.util.ArrayList;
import java.util.List;

import static org.mongodb.ReadPreference.primary;

public class GetIndexesOperation<T> extends OperationBase<List<T>> {
    private static final String NAMESPACE_KEY_NAME = "ns";

    private final Encoder<Document> queryEncoder;
    private final Decoder<T> resultDecoder;
    private final MongoNamespace indexesNamespace;
    private final Find queryForCollectionNamespace;

    public GetIndexesOperation(final BufferProvider bufferProvider, final Session session, final boolean closeSession,
                               final MongoNamespace collectionNamespace, final Encoder<Document> queryEncoder,
                               final Decoder<T> resultDecoder) {
        super(bufferProvider, session, closeSession);
        this.queryEncoder = queryEncoder;
        this.resultDecoder = resultDecoder;
        indexesNamespace = new MongoNamespace(collectionNamespace.getDatabaseName(), "system.indexes");
        queryForCollectionNamespace = new Find(new Document(NAMESPACE_KEY_NAME, collectionNamespace.getFullName()))
                                      .readPreference(primary());
    }

    @Override
    public List<T> execute() {
        final List<T> retVal = new ArrayList<T>();
        final MongoCursor<T> cursor = new QueryOperation<T>(indexesNamespace, queryForCollectionNamespace, queryEncoder,
                                                                          resultDecoder, getBufferProvider(), getSession(),
                                                                          false).execute();
        while (cursor.hasNext()) {
            retVal.add(cursor.next());
        }
        return retVal;
    }
}
