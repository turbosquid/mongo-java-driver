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

import org.mongodb.Codec;
import org.mongodb.CollectibleCodec;
import org.mongodb.CommandResult;
import org.mongodb.Document;
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
    private final Codec<Document> codec;
    private final CollectibleCodec<T> payloadCodec;
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private final boolean closeConnection;
    private final Document command;

    public CommandWithPayloadProtocol(final String database, final CollectibleCodec<T> payloadCodec, final Document command,
                                      final Codec<Document> codec,
                                      final BufferProvider bufferProvider, final ServerDescription serverDescription,
                                      final Connection connection, final boolean closeConnection) {
        this.payloadCodec = payloadCodec;
        this.serverDescription = serverDescription;
        this.connection = connection;
        this.closeConnection = closeConnection;
        this.namespace = new MongoNamespace(database, MongoNamespace.COMMAND_COLLECTION_NAME);
        this.bufferProvider = bufferProvider;
        this.codec = codec;
        this.command = command;
    }

    public CommandResult execute() {
        try {
            return receiveMessage(sendMessage());
        } finally {
            if (closeConnection) {
                connection.close();
            }
        }
    }

    private CommandWithPayloadMessage<T> sendMessage() {
        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        try {
            final CommandWithPayloadMessage<T> message = new CommandWithPayloadMessage<T>(namespace.getFullName(), command, payloadCodec,
                    getMessageSettings(serverDescription));
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers());
            return message;
        } finally {
            buffer.close();
        }
    }

    private CommandResult receiveMessage(final CommandWithPayloadMessage<T> message) {
        final ResponseBuffers responseBuffers = connection.receiveMessage(
                getResponseSettings(serverDescription, message.getId()));
        try {
            final ReplyMessage<Document> replyMessage = new ReplyMessage<Document>(responseBuffers, codec, message.getId());
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
