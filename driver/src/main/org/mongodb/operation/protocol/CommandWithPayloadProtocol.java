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

import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerDescription;

import static org.mongodb.operation.OperationHelpers.getMessageSettings;
import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public class CommandWithPayloadProtocol<T> implements Protocol<CommandResult> {
    private final Decoder<Document> responseDecoder;
    private final Encoder<T> payloadEncoder;
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private final boolean closeConnection;
    private final Document command;

    public CommandWithPayloadProtocol(final String database, final Encoder<T> payloadEncoder, final Document command,
                                      final Decoder<Document> responseDecoder, final BufferProvider bufferProvider,
                                      final ServerDescription serverDescription, final Connection connection,
                                      final boolean closeConnection) {
        this.payloadEncoder = payloadEncoder;
        this.serverDescription = serverDescription;
        this.connection = connection;
        this.closeConnection = closeConnection;
        this.namespace = new MongoNamespace(database, MongoNamespace.COMMAND_COLLECTION_NAME);
        this.bufferProvider = bufferProvider;
        this.responseDecoder = responseDecoder;
        this.command = command;
    }

    public CommandResult execute() {
        try {
            final CommandWithPayloadMessage<T> sentMessage = sendMessage();
            return receiveMessage(sentMessage.getId());
        } finally {
            if (closeConnection) {
                connection.close();
            }
        }
    }

    private CommandWithPayloadMessage<T> sendMessage() {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            final CommandWithPayloadMessage<T> message = new CommandWithPayloadMessage<T>(namespace.getFullName(), command, payloadEncoder,
                    getMessageSettings(serverDescription));
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers());
            return message;
        } finally {
            buffer.close();
        }
    }

    private CommandResult receiveMessage(final int messageId) {
        final ResponseBuffers responseBuffers = connection.receiveMessage(
                getResponseSettings(serverDescription, messageId));
        try {
            final ReplyMessage<Document> replyMessage = new ReplyMessage<Document>(responseBuffers, responseDecoder, messageId);
            return commandResult(replyMessage);
        } finally {
            responseBuffers.close();
        }
    }

    private CommandResult commandResult(final ReplyMessage<Document> replyMessage) {
        final CommandResult commandResult = new CommandResult(command, connection.getServerAddress(),
                replyMessage.getDocuments().get(0), replyMessage.getElapsedNanoseconds());
        if (!commandResult.isOk()) {
            throw new MongoCommandFailureException(commandResult);
        }

        return commandResult;
    }
}
