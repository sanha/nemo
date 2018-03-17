/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.runtime.executor.data.metadata;

import com.google.protobuf.ByteString;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * This class represents a metadata for a remote file block.
 * Because the data is stored in a remote file and globally accessed by multiple nodes,
 * each read, or deletion for a block needs one instance of this metadata.
 * The metadata is store in and read from a file (after a remote file block is committed).
 * @param <K> the key type of its partitions.
 */
@ThreadSafe
public final class RemoteFileMetadata<K extends Serializable> extends FileMetadata<K> {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteFileMetadata.class.getName());
  private final String blockId;
  private final PersistentConnectionToMasterMap connectionToMaster;

  /**
   * Constructor for creating a non-committed new file metadata.
   */
  private RemoteFileMetadata(final String blockId,
                             final PersistentConnectionToMasterMap connectionToMaster) {
    super();
    this.blockId = blockId;
    this.connectionToMaster = connectionToMaster;
  }

  /**
   * Constructor for opening a existing file metadata.
   *
   * @param partitionMetadataList the partition metadata list.
   */
  private RemoteFileMetadata(final String blockId,
                             final PersistentConnectionToMasterMap connectionToMaster,
                             final List<PartitionMetadata<K>> partitionMetadataList) {
    super(partitionMetadataList);
    this.blockId = blockId;
    this.connectionToMaster = connectionToMaster;
  }

  /**
   * @see FileMetadata#deleteMetadata()
   */
  @Override
  public void deleteMetadata() throws IOException {
    connectionToMaster.getMessageSender(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID).send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RemovePartitionMetadata)
            .setRemovePartitionMetadataMsg(
                ControlMessage.RemovePartitionMetadataMsg.newBuilder()
                    .setBlockId(blockId))
            .build());
  }

  /**
   * Write the collected {@link PartitionMetadata}s to the metadata file.
   * Notifies that all writes are finished for the block corresponding to this metadata.
   */
  @Override
  public synchronized void commitBlock() throws IOException {
    final Iterable<PartitionMetadata<K>> partitionMetadataItr = getPartitionMetadataIterable();
    // Convert block metadata to block metadata messages.
    final List<ControlMessage.PartitionMetadataMsg> partitionMetadataMsgs = new ArrayList<>();
    partitionMetadataItr.forEach(metadata -> {
      partitionMetadataMsgs.add(
          ControlMessage.PartitionMetadataMsg.newBuilder()
              .setKey(ByteString.copyFrom(SerializationUtils.serialize(metadata.getKey())))
              .setPartitionSize(metadata.getPartitionSize())
              .setOffset(metadata.getOffset())
              .setNumElements(metadata.getElementsTotal())
              .build());
    });

    connectionToMaster.getMessageSender(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID).send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.CommitPartition)
            .setCommitPartitionMsg(
                ControlMessage.CommitPartitionMsg.newBuilder()
                    .setBlockId(blockId)
                    .addAllPartitionMetadataMsg(partitionMetadataMsgs))
            .build());
    setCommitted(true);
  }

  /**
   * Creates a new block metadata.
   *
   * @param <T>          the key type of the block's partitions.
   * @return the created block metadata.
   */
  public static <T extends Serializable>
  RemoteFileMetadata<T> create(final String blockId,
                               final PersistentConnectionToMasterMap connectionToMaster) {
    return new RemoteFileMetadata<>(blockId, connectionToMaster);
  }

  /**
   * Opens a existing block metadata in file.
   *
   * @param <T>          the key type of the block's partitions.
   * @return the created block metadata.
   * @throws IOException if fail to open.
   */
  public static <T extends Serializable>
  RemoteFileMetadata<T> open(final String blockId,
                             final String executorId,
                             final PersistentConnectionToMasterMap connectionToMaster) throws IOException {
    final List<PartitionMetadata<T>> partitionMetadataList = new ArrayList<>();

    final long requestId = RuntimeIdGenerator.generateMessageId();
    // Ask the metadata server in the master for the metadata
    final CompletableFuture<ControlMessage.Message> metadataResponseFuture =
        connectionToMaster.getMessageSender(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID).request(
            ControlMessage.Message.newBuilder()
                .setId(requestId)
                .setListenerId(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.RequestPartitionMetadata)
                .setRequestPartitionMetadataMsg(
                    ControlMessage.RequestPartitionMetadataMsg.newBuilder()
                        .setExecutorId(executorId)
                        .setBlockId(blockId))
                .build());

    final ControlMessage.Message responseFromMaster;
    try {
      responseFromMaster = metadataResponseFuture.get();
    } catch (final InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }

    final ControlMessage.MetadataResponseMsg metadataResponseMsg = responseFromMaster.getMetadataResponseMsg();
    if (metadataResponseMsg.hasState()) {
      // Response has an exception state.
      throw new IOException(new Throwable(
          "Cannot get the metadata of block " + blockId + " from the metadata server: "
              + "The block state is " + metadataResponseMsg.getState()));
    }

    // Construct the metadata from the response.
    final List<ControlMessage.PartitionMetadataMsg> partitionMetadataMsgList =
        metadataResponseMsg.getPartitionMetadataList();
    for (int partitionIdx = 0; partitionIdx < partitionMetadataMsgList.size(); partitionIdx++) {
      final ControlMessage.PartitionMetadataMsg partitionMetadataMsg = partitionMetadataMsgList.get(partitionIdx);
      partitionMetadataList.add(new PartitionMetadata<>(
          SerializationUtils.deserialize(partitionMetadataMsg.getKey().toByteArray()),
          partitionMetadataMsg.getPartitionSize(),
          partitionMetadataMsg.getOffset(),
          partitionMetadataMsg.getNumElements()
      ));
    }

    return new RemoteFileMetadata<>(blockId, connectionToMaster, partitionMetadataList);
  }
}
