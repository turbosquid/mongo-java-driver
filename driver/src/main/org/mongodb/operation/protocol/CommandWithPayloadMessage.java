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

package org.mongodb.operation.protocol;

import org.bson.BSONBinaryWriter;
import org.bson.io.OutputBuffer;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoInvalidDocumentException;
import org.mongodb.codecs.Codecs;
import org.mongodb.operation.FindAndModifyCommandEncoder;

import static org.mongodb.operation.protocol.RequestMessage.OpCode.OP_QUERY;

public class CommandWithPayloadMessage<T> extends RequestMessage {
    private final FindAndModifyCommandEncoder<T> findAndModifyCommandEncoder;
    private final Document command;

    public CommandWithPayloadMessage(final String collectionName, final Document command, final Encoder<T> payloadEncoder,
                                     final MessageSettings settings) {
        super(collectionName, OP_QUERY, settings);
        this.command = command;
        findAndModifyCommandEncoder = new FindAndModifyCommandEncoder<T>(payloadEncoder, Codecs.createDefault());
    }

    @Override
    protected RequestMessage encodeMessageBody(final OutputBuffer buffer, final int messageStartPosition) {
        buffer.writeInt(0);
        buffer.writeCString(getCollectionName());

        buffer.writeInt(0);
        buffer.writeInt(-1);
        encodeCommand(buffer);
        return null;
    }

    private void encodeCommand(final OutputBuffer buffer) {
        final BSONBinaryWriter writer = new BSONBinaryWriter(buffer, false);
        try {
            int startPosition = buffer.getPosition();
            findAndModifyCommandEncoder.encode(writer, command);
            int documentSize = buffer.getPosition() - startPosition;
            if (documentSize > getSettings().getMaxDocumentSize()) {
                buffer.truncateToPosition(startPosition);
                throw new MongoInvalidDocumentException(String.format("Document size of %d exceeds maximum of %d", documentSize,
                                                                      getSettings().getMaxDocumentSize()));
            }
        } finally {
            writer.close();
        }
    }
}