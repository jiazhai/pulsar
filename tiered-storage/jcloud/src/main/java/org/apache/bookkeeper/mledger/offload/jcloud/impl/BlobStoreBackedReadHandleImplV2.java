/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.val;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.offload.jcloud.BackedInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockV2;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockV2Builder;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.DataBlockUtils.VersionCheck;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.KeyNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobStoreBackedReadHandleImplV2 implements ReadHandle {
    private static final Logger log = LoggerFactory.getLogger(BlobStoreBackedReadHandleImplV2.class);

    private final long ledgerId;
    private final List<OffloadIndexBlockV2> indices;
    private final List<BackedInputStream> inputStreams;
    private final List<DataInputStream> dataStreams;
    private final ExecutorService executor;
    private volatile State state = null;
    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();

    enum State {
        Opened,
        Closed
    }

    static class GroupedReader {
        @Override
        public String toString() {
            return "GroupedReader{"
                    + "ledgerId=" + ledgerId
                    + ", firstEntry=" + firstEntry
                    + ", lastEntry=" + lastEntry
                    + '}';
        }

        public final long ledgerId;
        public final long firstEntry;
        public final long lastEntry;
        OffloadIndexBlockV2 index;
        BackedInputStream inputStream;
        DataInputStream dataStream;

        public GroupedReader(long ledgerId, long firstEntry, long lastEntry,
                             OffloadIndexBlockV2 index,
                             BackedInputStream inputStream, DataInputStream dataStream) {
            this.ledgerId = ledgerId;
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.index = index;
            this.inputStream = inputStream;
            this.dataStream = dataStream;
        }
    }

    private BlobStoreBackedReadHandleImplV2(long ledgerId, List<OffloadIndexBlockV2> indices,
                                            List<BackedInputStream> inputStreams,
                                            ExecutorService executor) {
        this.ledgerId = ledgerId;
        this.indices = indices;
        this.inputStreams = inputStreams;
        this.dataStreams = new LinkedList<>();
        for (BackedInputStream inputStream : inputStreams) {
            dataStreams.add(new DataInputStream(inputStream));
        }
        this.executor = executor;
        this.state = State.Opened;
    }

    @Override
    public long getId() {
        return ledgerId;
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        //get the most complete one
        return indices.get(indices.size() - 1).getLedgerMetadata(ledgerId);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (closeFuture.get() != null || !closeFuture.compareAndSet(null, new CompletableFuture<>())) {
            return closeFuture.get();
        }

        CompletableFuture<Void> promise = closeFuture.get();
        executor.execute(() -> {
            try {
                for (OffloadIndexBlockV2 indexBlock : indices) {
                    indexBlock.close();
                }
                for (DataInputStream dataStream : dataStreams) {
                    dataStream.close();
                }
                state = State.Closed;
                promise.complete(null);
            } catch (IOException t) {
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        if (log.isDebugEnabled()) {
            log.debug("Ledger {}: reading {} - {}", getId(), firstEntry, lastEntry);
        }
        CompletableFuture<LedgerEntries> promise = new CompletableFuture<>();
        executor.execute(() -> {
            if (state == State.Closed) {
                log.warn("Reading a closed read handler. Ledger ID: {}, Read range: {}-{}",
                        ledgerId, firstEntry, lastEntry);
                promise.completeExceptionally(new ManagedLedgerException.OffloadReadHandleClosedException());
                return;
            }

            if (firstEntry > lastEntry
                    || firstEntry < 0
                    || lastEntry > getLastAddConfirmed()) {
                promise.completeExceptionally(new BKException.BKIncorrectParameterException());
                return;
            }
            List<LedgerEntry> entries = new ArrayList<LedgerEntry>();
            List<GroupedReader> groupedReaders = null;
            try {
                groupedReaders = getGroupedReader(firstEntry, lastEntry);
            } catch (Exception e) {
                promise.completeExceptionally(e);
                return;
            }

            for (GroupedReader groupedReader : groupedReaders) {
                long entriesToRead = (groupedReader.lastEntry - groupedReader.firstEntry) + 1;
                long nextExpectedId = groupedReader.firstEntry;
                try {
                    while (entriesToRead > 0) {
                        int length = groupedReader.dataStream.readInt();
                        if (length < 0) { // hit padding or new block
                            groupedReader.inputStream
                                    .seek(groupedReader.index
                                            .getIndexEntryForEntry(groupedReader.ledgerId, nextExpectedId)
                                            .getDataOffset());
                            continue;
                        }
                        long entryId = groupedReader.dataStream.readLong();

                        if (entryId == nextExpectedId) {
                            ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(length, length);
                            entries.add(LedgerEntryImpl.create(ledgerId, entryId, length, buf));
                            int toWrite = length;
                            while (toWrite > 0) {
                                toWrite -= buf.writeBytes(groupedReader.dataStream, toWrite);
                            }
                            entriesToRead--;
                            nextExpectedId++;
                        } else if (entryId > nextExpectedId) {
                            groupedReader.inputStream
                                    .seek(groupedReader.index
                                            .getIndexEntryForEntry(groupedReader.ledgerId, nextExpectedId)
                                            .getDataOffset());
                            continue;
                        } else if (entryId < nextExpectedId
                                && !groupedReader.index.getIndexEntryForEntry(groupedReader.ledgerId, nextExpectedId)
                                .equals(
                                        groupedReader.index.getIndexEntryForEntry(groupedReader.ledgerId, entryId))) {
                            groupedReader.inputStream
                                    .seek(groupedReader.index
                                            .getIndexEntryForEntry(groupedReader.ledgerId, nextExpectedId)
                                            .getDataOffset());
                            continue;
                        } else if (entryId > groupedReader.lastEntry) {
                            log.info("Expected to read {}, but read {}, which is greater than last entry {}",
                                    nextExpectedId, entryId, groupedReader.lastEntry);
                            throw new BKException.BKUnexpectedConditionException();
                        } else {
                            val skipped = groupedReader.inputStream.skip(length);
                        }
                    }
                } catch (Throwable t) {
                    if (t instanceof KeyNotFoundException) {
                        promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsException());
                    } else {
                        promise.completeExceptionally(t);
                    }
                    entries.forEach(LedgerEntry::close);
                }

            }
            promise.complete(LedgerEntriesImpl.create(entries));
        });
        return promise;
    }

    private List<GroupedReader> getGroupedReader(long firstEntry, long lastEntry) throws Exception {
        List<GroupedReader> groupedReaders = new LinkedList<>();
        for (int i = indices.size() - 1; i >= 0 && firstEntry <= lastEntry; i--) {
            final OffloadIndexBlockV2 index = indices.get(i);
            final long startEntryId = index.getStartEntryId(ledgerId);
            if (startEntryId > lastEntry) {
                log.debug("entries are in earlier indices, skip this segment ledger id: {}, begin entry id: {}",
                        ledgerId, startEntryId);
            } else {
                groupedReaders.add(new GroupedReader(ledgerId, startEntryId, lastEntry, index, inputStreams.get(i),
                        dataStreams.get(i)));
                lastEntry = startEntryId - 1;
            }
        }

        checkArgument(firstEntry > lastEntry);
        for (int i = 0; i < groupedReaders.size() - 1; i++) {
            final GroupedReader readerI = groupedReaders.get(i);
            final GroupedReader readerII = groupedReaders.get(i + 1);
            checkArgument(readerI.ledgerId == readerII.ledgerId);
            checkArgument(readerI.firstEntry >= readerII.lastEntry);
        }
        return groupedReaders;
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        return readAsync(firstEntry, lastEntry);
    }

    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(getLastAddConfirmed());
    }

    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(getLastAddConfirmed());
    }

    @Override
    public long getLastAddConfirmed() {
        return getLedgerMetadata().getLastEntryId();
    }

    @Override
    public long getLength() {
        return getLedgerMetadata().getLength();
    }

    @Override
    public boolean isClosed() {
        return getLedgerMetadata().isClosed();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
                                                                                      long timeOutInMillis,
                                                                                      boolean parallel) {
        CompletableFuture<LastConfirmedAndEntry> promise = new CompletableFuture<>();
        promise.completeExceptionally(new UnsupportedOperationException());
        return promise;
    }

    public static ReadHandle open(ScheduledExecutorService executor,
                                  BlobStore blobStore, String bucket, List<String> keys, List<String> indexKeys,
                                  VersionCheck versionCheck,
                                  long ledgerId, int readBufferSize, LedgerOffloaderStats offloaderStats,
                                  String managedLedgerName)
            throws IOException, BKException.BKNoSuchLedgerExistsException {
        List<BackedInputStream> inputStreams = new LinkedList<>();
        List<OffloadIndexBlockV2> indice = new LinkedList<>();
        String topicName = TopicName.fromPersistenceNamingEncoding(managedLedgerName);
        for (int i = 0; i < indexKeys.size(); i++) {
            String indexKey = indexKeys.get(i);
            String key = keys.get(i);
            log.debug("open bucket: {} index key: {}", bucket, indexKey);
            long startTime = System.nanoTime();
            Blob blob = blobStore.getBlob(bucket, indexKey);
            if (blob == null) {
                log.error("{} not found in container {}", indexKey, bucket);
                throw new BKException.BKNoSuchLedgerExistsException();
            }
            offloaderStats.recordReadOffloadIndexLatency(topicName,
                    System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
            log.debug("indexKey blob: {} {}", indexKey, blob);
            versionCheck.check(indexKey, blob);
            OffloadIndexBlockV2Builder indexBuilder = OffloadIndexBlockV2Builder.create();
            OffloadIndexBlockV2 index;
            try (InputStream payloadStream = blob.getPayload().openStream()) {
                index = indexBuilder.fromStream(payloadStream);
            }

            BackedInputStream inputStream = new BlobStoreBackedInputStreamImpl(blobStore, bucket, key,
                    versionCheck, index.getDataObjectLength(), readBufferSize, offloaderStats, managedLedgerName);
            inputStreams.add(inputStream);
            indice.add(index);
        }
        return new BlobStoreBackedReadHandleImplV2(ledgerId, indice, inputStreams, executor);
    }
}
