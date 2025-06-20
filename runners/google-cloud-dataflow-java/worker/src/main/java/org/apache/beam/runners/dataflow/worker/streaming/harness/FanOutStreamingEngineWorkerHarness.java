/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.dataflow.worker.streaming.harness;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataResponse.EndpointType;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints.Endpoint;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkerMetadataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.StreamGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ThrottlingGetDataMetricTracker;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcDispatcherClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetDistributor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StreamingWorkerHarness} implementation that manages fan out to multiple backend
 * destinations. Given a {@link GetWorkBudget}, divides the budget and starts the {@link
 * WindmillStream.GetWorkStream}(s).
 */
@Internal
@CheckReturnValue
@ThreadSafe
public final class FanOutStreamingEngineWorkerHarness implements StreamingWorkerHarness {
  private static final Logger LOG =
      LoggerFactory.getLogger(FanOutStreamingEngineWorkerHarness.class);
  private static final String WORKER_METADATA_CONSUMER_THREAD_NAME =
      "WindmillWorkerMetadataConsumerThread";
  private static final String STREAM_MANAGER_THREAD_NAME = "WindmillStreamManager-%d";

  private final JobHeader jobHeader;
  private final GrpcWindmillStreamFactory streamFactory;
  private final WorkItemScheduler workItemScheduler;
  private final ChannelCachingStubFactory channelCachingStubFactory;
  private final GrpcDispatcherClient dispatcherClient;
  private final GetWorkBudgetDistributor getWorkBudgetDistributor;
  private final GetWorkBudget totalGetWorkBudget;
  private final Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory;
  private final ThrottlingGetDataMetricTracker getDataMetricTracker;
  private final ExecutorService windmillStreamManager;
  private final ExecutorService workerMetadataConsumer;
  private final Object metadataLock = new Object();
  private final Function<WindmillConnection, WindmillStreamSender> windmillStreamPoolSenderFactory;

  /** Writes are guarded by synchronization, reads are lock free. */
  private final AtomicReference<StreamingEngineBackends> backends;

  @GuardedBy("this")
  private long activeMetadataVersion;

  @GuardedBy("metadataLock")
  private long pendingMetadataVersion;

  @GuardedBy("metadataLock")
  private EndpointType activeEndpointType = EndpointType.UNKNOWN;

  @GuardedBy("this")
  private boolean started;

  @GuardedBy("this")
  private @Nullable GetWorkerMetadataStream getWorkerMetadataStream = null;

  private FanOutStreamingEngineWorkerHarness(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamFactory,
      WorkItemScheduler workItemScheduler,
      ChannelCachingStubFactory channelCachingStubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      ThrottlingGetDataMetricTracker getDataMetricTracker,
      ExecutorService workerMetadataConsumer,
      Function<WindmillConnection, WindmillStreamSender> windmillStreamPoolSenderFactory) {
    this.jobHeader = jobHeader;
    this.getDataMetricTracker = getDataMetricTracker;
    this.started = false;
    this.streamFactory = streamFactory;
    this.workItemScheduler = workItemScheduler;
    this.backends = new AtomicReference<>(StreamingEngineBackends.EMPTY);
    this.channelCachingStubFactory = channelCachingStubFactory;
    this.dispatcherClient = dispatcherClient;
    this.windmillStreamManager =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat(STREAM_MANAGER_THREAD_NAME).build());
    this.workerMetadataConsumer = workerMetadataConsumer;
    this.getWorkBudgetDistributor = getWorkBudgetDistributor;
    this.totalGetWorkBudget = totalGetWorkBudget;
    this.activeMetadataVersion = Long.MIN_VALUE;
    this.workCommitterFactory = workCommitterFactory;
    this.windmillStreamPoolSenderFactory = windmillStreamPoolSenderFactory;
  }

  /**
   * Creates an instance of {@link FanOutStreamingEngineWorkerHarness} in a non-started state.
   *
   * @implNote Does not block the calling thread. Callers must explicitly call {@link #start()}.
   */
  public static FanOutStreamingEngineWorkerHarness create(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkItemScheduler processWorkItem,
      ChannelCachingStubFactory channelCachingStubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      ThrottlingGetDataMetricTracker getDataMetricTracker,
      Function<WindmillConnection, WindmillStreamSender> windmillStreamPoolSenderFactory) {
    return new FanOutStreamingEngineWorkerHarness(
        jobHeader,
        totalGetWorkBudget,
        streamingEngineStreamFactory,
        processWorkItem,
        channelCachingStubFactory,
        getWorkBudgetDistributor,
        dispatcherClient,
        workCommitterFactory,
        getDataMetricTracker,
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat(WORKER_METADATA_CONSUMER_THREAD_NAME).build()),
        windmillStreamPoolSenderFactory);
  }

  @VisibleForTesting
  static FanOutStreamingEngineWorkerHarness forTesting(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamFactory,
      WorkItemScheduler processWorkItem,
      ChannelCachingStubFactory stubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      ThrottlingGetDataMetricTracker getDataMetricTracker,
      Function<WindmillConnection, WindmillStreamSender> windmillStreamPoolSenderFactory) {
    FanOutStreamingEngineWorkerHarness fanOutStreamingEngineWorkProvider =
        new FanOutStreamingEngineWorkerHarness(
            jobHeader,
            totalGetWorkBudget,
            streamFactory,
            processWorkItem,
            stubFactory,
            getWorkBudgetDistributor,
            dispatcherClient,
            workCommitterFactory,
            getDataMetricTracker,
            // Run the workerMetadataConsumer on the direct calling thread to remove waiting and
            // make unit tests more deterministic as we do not have to worry about network IO being
            // blocked by the consumeWorkerMetadata() task. Test suites run in different
            // environments and non-determinism has lead to past flakiness. See
            // https://github.com/apache/beam/issues/28957.
            MoreExecutors.newDirectExecutorService(),
            windmillStreamPoolSenderFactory);
    fanOutStreamingEngineWorkProvider.start();
    return fanOutStreamingEngineWorkProvider;
  }

  @Override
  public synchronized void start() {
    Preconditions.checkState(!started, "FanOutStreamingEngineWorkerHarness cannot start twice.");
    getWorkerMetadataStream =
        streamFactory.createGetWorkerMetadataStream(
            dispatcherClient::getWindmillMetadataServiceStubBlocking, this::consumeWorkerMetadata);
    getWorkerMetadataStream.start();
    started = true;
  }

  public ImmutableSet<HostAndPort> currentWindmillEndpoints() {
    return backends.get().windmillStreams().keySet().stream()
        .map(Endpoint::directEndpoint)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(WindmillServiceAddress::getServiceAddress)
        .collect(toImmutableSet());
  }

  /**
   * Fetches {@link GetDataStream} mapped to globalDataKey if or throws {@link
   * NoSuchElementException} if one is not found.
   */
  private GetDataStream getGlobalDataStream(String globalDataKey) {
    return Optional.ofNullable(backends.get().globalDataStreams().get(globalDataKey))
        .map(GlobalDataStreamSender::stream)
        .orElseThrow(
            () -> new NoSuchElementException("No endpoint for global data tag: " + globalDataKey));
  }

  @VisibleForTesting
  @Override
  public synchronized void shutdown() {
    Preconditions.checkState(started, "FanOutStreamingEngineWorkerHarness never started.");
    Preconditions.checkNotNull(getWorkerMetadataStream).shutdown();
    workerMetadataConsumer.shutdownNow();
    // Close all the streams blocking until this completes to not leak resources.
    closeStreamsNotIn(WindmillEndpoints.none()).join();
    channelCachingStubFactory.shutdown();

    try {
      Preconditions.checkNotNull(getWorkerMetadataStream).awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted waiting for GetWorkerMetadataStream to shutdown.", e);
    }

    windmillStreamManager.shutdown();
    boolean isStreamManagerShutdown = false;
    try {
      isStreamManagerShutdown = windmillStreamManager.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted waiting for windmillStreamManager to shutdown.", e);
    }
    if (!isStreamManagerShutdown) {
      windmillStreamManager.shutdownNow();
    }
  }

  private void consumeWorkerMetadata(WindmillEndpoints windmillEndpoints) {
    synchronized (metadataLock) {
      // Only process versions greater than what we currently have to prevent double processing of
      // metadata. workerMetadataConsumer is single-threaded so we maintain ordering.
      // But in case the endpoint type in worker metadata is different from the active
      // endpoint type, also process those endpoints
      if (windmillEndpoints.version() > pendingMetadataVersion
          || (windmillEndpoints.version() == pendingMetadataVersion
              && windmillEndpoints.endpointType() != activeEndpointType)) {
        pendingMetadataVersion = windmillEndpoints.version();
        workerMetadataConsumer.execute(() -> consumeWindmillWorkerEndpoints(windmillEndpoints));
      }
    }
  }

  private synchronized void consumeWindmillWorkerEndpoints(WindmillEndpoints newWindmillEndpoints) {
    // Since this is run on a single threaded executor, multiple versions of the metadata maybe
    // queued up while a previous version of the windmillEndpoints were being consumed. Only consume
    // the endpoints if they are the most current version, or if the endpoint type is different
    // from currently active endpoints.
    synchronized (metadataLock) {
      if (newWindmillEndpoints.version() < pendingMetadataVersion) {
        return;
      }
      activeEndpointType = newWindmillEndpoints.endpointType();
    }

    LOG.debug(
        "Consuming new endpoints: {}. previous metadata version: {}, current metadata version: {}",
        newWindmillEndpoints,
        activeMetadataVersion,
        newWindmillEndpoints.version());
    closeStreamsNotIn(newWindmillEndpoints).join();
    ImmutableMap<Endpoint, WindmillStreamSender> newStreams =
        createAndStartNewStreams(
                newWindmillEndpoints.windmillEndpoints(), newWindmillEndpoints.endpointType())
            .join();

    StreamingEngineBackends newBackends =
        StreamingEngineBackends.builder()
            .setWindmillStreams(newStreams)
            .setGlobalDataStreams(
                createNewGlobalDataStreams(newWindmillEndpoints.globalDataEndpoints()))
            .build();
    backends.set(newBackends);
    getWorkBudgetDistributor.distributeBudget(newStreams.values(), totalGetWorkBudget);
    activeMetadataVersion = newWindmillEndpoints.version();
  }

  /** Close the streams that are no longer valid asynchronously. */
  @CanIgnoreReturnValue
  private CompletableFuture<Void> closeStreamsNotIn(WindmillEndpoints newWindmillEndpoints) {
    StreamingEngineBackends currentBackends = backends.get();
    Stream<CompletableFuture<Void>> closeStreamFutures =
        currentBackends.windmillStreams().entrySet().stream()
            .filter(
                connectionAndStream ->
                    !newWindmillEndpoints
                        .windmillEndpoints()
                        .contains(connectionAndStream.getKey()))
            .map(
                entry ->
                    CompletableFuture.runAsync(
                        () -> closeStreamSender(entry.getKey(), entry.getValue()),
                        windmillStreamManager));

    Set<Endpoint> newGlobalDataEndpoints =
        new HashSet<>(newWindmillEndpoints.globalDataEndpoints().values());
    Stream<CompletableFuture<Void>> closeGlobalDataStreamFutures =
        currentBackends.globalDataStreams().values().stream()
            .filter(sender -> !newGlobalDataEndpoints.contains(sender.endpoint()))
            .map(
                sender ->
                    CompletableFuture.runAsync(
                        () -> closeStreamSender(sender.endpoint(), sender), windmillStreamManager));

    return CompletableFuture.allOf(
        Streams.concat(closeStreamFutures, closeGlobalDataStreamFutures)
            .toArray(CompletableFuture[]::new));
  }

  private void closeStreamSender(Endpoint endpoint, StreamSender sender) {
    LOG.debug("Closing streams to endpoint={}, sender={}", endpoint, sender);
    try {
      sender.close();
      endpoint.directEndpoint().ifPresent(channelCachingStubFactory::remove);
      LOG.debug("Successfully closed streams to {}", endpoint);
    } catch (Exception e) {
      LOG.error("Error closing streams to endpoint={}, sender={}", endpoint, sender);
    }
  }

  private synchronized CompletableFuture<ImmutableMap<Endpoint, WindmillStreamSender>>
      createAndStartNewStreams(
          ImmutableSet<Endpoint> newWindmillEndpoints, EndpointType endpointType) {
    ImmutableMap<Endpoint, WindmillStreamSender> currentStreams = backends.get().windmillStreams();
    return MoreFutures.allAsList(
            newWindmillEndpoints.stream()
                .map(
                    endpoint ->
                        getOrCreateWindmillStreamSenderFuture(
                            endpoint, currentStreams, endpointType))
                .collect(Collectors.toList()))
        .thenApply(
            backends -> backends.stream().collect(toImmutableMap(Pair::getLeft, Pair::getRight)))
        .toCompletableFuture();
  }

  private CompletionStage<Pair<Endpoint, WindmillStreamSender>>
      getOrCreateWindmillStreamSenderFuture(
          Endpoint endpoint,
          ImmutableMap<Endpoint, WindmillStreamSender> currentStreams,
          EndpointType endpointType) {
    return Optional.ofNullable(currentStreams.get(endpoint))
        .map(backend -> CompletableFuture.completedFuture(Pair.of(endpoint, backend)))
        .orElseGet(
            () ->
                MoreFutures.supplyAsync(
                        () ->
                            Pair.of(
                                endpoint,
                                createAndStartWindmillStreamSender(endpoint, endpointType)),
                        windmillStreamManager)
                    .toCompletableFuture());
  }

  public long currentActiveCommitBytes() {
    return backends.get().windmillStreams().values().stream()
        .map(WindmillStreamSender::getCurrentActiveCommitBytes)
        .reduce(0L, Long::sum);
  }

  @VisibleForTesting
  StreamingEngineBackends currentBackends() {
    return backends.get();
  }

  private ImmutableMap<String, GlobalDataStreamSender> createNewGlobalDataStreams(
      ImmutableMap<String, Endpoint> newGlobalDataEndpoints) {
    ImmutableMap<String, GlobalDataStreamSender> currentGlobalDataStreams =
        backends.get().globalDataStreams();
    return newGlobalDataEndpoints.entrySet().stream()
        .collect(
            toImmutableMap(
                Entry::getKey,
                keyedEndpoint ->
                    getOrCreateGlobalDataSteam(keyedEndpoint, currentGlobalDataStreams)));
  }

  private GlobalDataStreamSender getOrCreateGlobalDataSteam(
      Entry<String, Endpoint> keyedEndpoint,
      ImmutableMap<String, GlobalDataStreamSender> currentGlobalDataStreams) {
    return Optional.ofNullable(currentGlobalDataStreams.get(keyedEndpoint.getKey()))
        .orElseGet(
            () ->
                new GlobalDataStreamSender(
                    streamFactory.createGetDataStream(createWindmillStub(keyedEndpoint.getValue())),
                    keyedEndpoint.getValue()));
  }

  private WindmillStreamSender createAndStartWindmillStreamSender(
      Endpoint endpoint, EndpointType enpointType) {
    WindmillStreamSender windmillStreamSender =
        enpointType == EndpointType.DIRECTPATH
            ? WindmillDirectStreamSender.create(
                WindmillConnection.from(endpoint, this::createWindmillStub),
                GetWorkRequest.newBuilder()
                    .setClientId(jobHeader.getClientId())
                    .setJobId(jobHeader.getJobId())
                    .setProjectId(jobHeader.getProjectId())
                    .setWorkerId(jobHeader.getWorkerId())
                    .build(),
                GetWorkBudget.noBudget(),
                streamFactory,
                workItemScheduler,
                getDataStream ->
                    StreamGetDataClient.create(
                        getDataStream, this::getGlobalDataStream, getDataMetricTracker),
                workCommitterFactory)
            : windmillStreamPoolSenderFactory.apply(
                WindmillConnection.from(endpoint, this::createWindmillStub));
    windmillStreamSender.start();
    return windmillStreamSender;
  }

  private CloudWindmillServiceV1Alpha1Stub createWindmillStub(Endpoint endpoint) {
    return endpoint
        .directEndpoint()
        .map(channelCachingStubFactory::createWindmillServiceStub)
        .orElseGet(dispatcherClient::getWindmillServiceStub);
  }
}
