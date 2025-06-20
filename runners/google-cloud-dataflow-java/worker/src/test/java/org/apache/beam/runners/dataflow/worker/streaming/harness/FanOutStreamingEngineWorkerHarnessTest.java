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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillMetadataServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataResponse.EndpointType;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ThrottlingGetDataMetricTracker;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcDispatcherClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannels;
import org.apache.beam.runners.dataflow.worker.windmill.testing.FakeWindmillStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.testing.FakeWindmillStubFactoryFactory;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetDistributor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetSpender;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessSocketAddress;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FanOutStreamingEngineWorkerHarnessTest {
  private static final String CHANNEL_NAME = "FanOutStreamingEngineWorkerHarnessTest";
  private static final long WAIT_FOR_METADATA_INJECTIONS_SECONDS = 5;
  private static final long SERVER_SHUTDOWN_TIMEOUT_SECONDS = 30;
  private static final WindmillServiceAddress DEFAULT_WINDMILL_SERVICE_ADDRESS =
      WindmillServiceAddress.create(HostAndPort.fromParts(WindmillChannels.LOCALHOST, 443));
  private static final String AUTHENTICATING_SERVICE = "test.googleapis.com";
  private static final ImmutableMap<String, WorkerMetadataResponse.Endpoint> DEFAULT =
      ImmutableMap.of(
          "global_data",
          WorkerMetadataResponse.Endpoint.newBuilder()
              .setDirectEndpoint(DEFAULT_WINDMILL_SERVICE_ADDRESS.gcpServiceAddress().toString())
              .build());

  private static final String JOB_ID = "jobId";
  private static final String PROJECT_ID = "projectId";
  private static final String WORKER_ID = "workerId";
  private static final JobHeader JOB_HEADER =
      JobHeader.newBuilder()
          .setJobId(JOB_ID)
          .setProjectId(PROJECT_ID)
          .setWorkerId(WORKER_ID)
          .setClientId(1L)
          .build();

  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule().setTimeout(3, TimeUnit.MINUTES);

  private final GrpcWindmillStreamFactory streamFactory =
      spy(GrpcWindmillStreamFactory.of(JOB_HEADER).build());
  private final ChannelCachingStubFactory stubFactory =
      new FakeWindmillStubFactory(
          () -> grpcCleanup.register(WindmillChannels.inProcessChannel(CHANNEL_NAME)));
  private final GrpcDispatcherClient dispatcherClient =
      GrpcDispatcherClient.forTesting(
          PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class),
          new FakeWindmillStubFactoryFactory(stubFactory),
          new ArrayList<>(),
          new ArrayList<>(),
          new HashSet<>());
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  private Server fakeStreamingEngineServer;
  private CountDownLatch getWorkerMetadataReady;
  private GetWorkerMetadataTestStub fakeGetWorkerMetadataStub;
  private FanOutStreamingEngineWorkerHarness fanOutStreamingEngineWorkProvider;

  private static WorkItemScheduler noOpProcessWorkItemFn() {
    return (workItem,
        serializedWorkItemSize,
        watermarks,
        processingContext,
        getWorkStreamLatencies) -> {};
  }

  private static GetWorkRequest getWorkRequest(long items, long bytes) {
    return GetWorkRequest.newBuilder()
        .setJobId(JOB_ID)
        .setProjectId(PROJECT_ID)
        .setWorkerId(WORKER_ID)
        .setClientId(JOB_HEADER.getClientId())
        .setMaxItems(items)
        .setMaxBytes(bytes)
        .build();
  }

  private static WorkerMetadataResponse.Endpoint metadataResponseEndpoint(String workerToken) {
    return WorkerMetadataResponse.Endpoint.newBuilder()
        .setDirectEndpoint(DEFAULT_WINDMILL_SERVICE_ADDRESS.gcpServiceAddress().getHost())
        .setBackendWorkerToken(workerToken)
        .build();
  }

  @Before
  public void setUp() throws IOException {
    getWorkerMetadataReady = new CountDownLatch(1);
    fakeGetWorkerMetadataStub = new GetWorkerMetadataTestStub(getWorkerMetadataReady);
    fakeStreamingEngineServer =
        grpcCleanup
            .register(
                InProcessServerBuilder.forName(CHANNEL_NAME)
                    .directExecutor()
                    .addService(fakeGetWorkerMetadataStub)
                    .addService(new WindmillServiceFakeStub())
                    .build())
            .start();

    dispatcherClient.consumeWindmillDispatcherEndpoints(
        ImmutableSet.of(
            HostAndPort.fromString(new InProcessSocketAddress(CHANNEL_NAME).toString())));
  }

  @After
  public void cleanUp() throws InterruptedException {
    Preconditions.checkNotNull(fanOutStreamingEngineWorkProvider).shutdown();
    stubFactory.shutdown();
    fakeStreamingEngineServer.shutdown();
    if (!fakeStreamingEngineServer.awaitTermination(
        SERVER_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
      fakeStreamingEngineServer.shutdownNow();
    }
  }

  private FanOutStreamingEngineWorkerHarness newFanOutStreamingEngineWorkerHarness(
      GetWorkBudget getWorkBudget,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      WorkItemScheduler workItemScheduler)
      throws InterruptedException {
    FanOutStreamingEngineWorkerHarness harness =
        FanOutStreamingEngineWorkerHarness.forTesting(
            JOB_HEADER,
            getWorkBudget,
            streamFactory,
            workItemScheduler,
            stubFactory,
            getWorkBudgetDistributor,
            dispatcherClient,
            ignored -> mock(WorkCommitter.class),
            new ThrottlingGetDataMetricTracker(mock(MemoryMonitor.class)),
            (connection) ->
                WindmillStreamPoolSender.create(
                    connection,
                    GetWorkRequest.newBuilder()
                        .setClientId(JOB_HEADER.getClientId())
                        .setJobId(JOB_HEADER.getJobId())
                        .setProjectId(JOB_HEADER.getProjectId())
                        .setWorkerId(JOB_HEADER.getWorkerId())
                        .setMaxItems(getWorkBudget.items())
                        .setMaxBytes(getWorkBudget.bytes())
                        .build(),
                    getWorkBudget,
                    streamFactory,
                    mock(WorkCommitter.class),
                    mock(GetDataClient.class),
                    mock(HeartbeatSender.class),
                    mock(StreamingWorkScheduler.class),
                    () -> {},
                    ignored -> Optional.empty()));
    getWorkerMetadataReady.await();
    return harness;
  }

  @Test
  public void testStreamsStartCorrectly() throws InterruptedException {
    long items = 10L;
    long bytes = 10L;

    TestGetWorkBudgetDistributor getWorkBudgetDistributor = spy(new TestGetWorkBudgetDistributor());

    fanOutStreamingEngineWorkProvider =
        newFanOutStreamingEngineWorkerHarness(
            GetWorkBudget.builder().setItems(items).setBytes(bytes).build(),
            getWorkBudgetDistributor,
            noOpProcessWorkItemFn());

    String workerToken = "workerToken1";
    String workerToken2 = "workerToken2";

    fakeGetWorkerMetadataStub.injectWorkerMetadata(
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addWorkEndpoints(metadataResponseEndpoint(workerToken))
            .addWorkEndpoints(metadataResponseEndpoint(workerToken2))
            .setEndpointType(EndpointType.DIRECTPATH)
            .putAllGlobalDataEndpoints(DEFAULT)
            .build());

    StreamingEngineBackends currentBackends = fanOutStreamingEngineWorkProvider.currentBackends();

    assertEquals(2, currentBackends.windmillStreams().size());
    Set<String> workerTokens =
        currentBackends.windmillStreams().keySet().stream()
            .map(endpoint -> endpoint.workerToken().orElseThrow(IllegalStateException::new))
            .collect(Collectors.toSet());

    assertTrue(workerTokens.contains(workerToken));
    assertTrue(workerTokens.contains(workerToken2));

    verify(getWorkBudgetDistributor, atLeast(1))
        .distributeBudget(
            any(), eq(GetWorkBudget.builder().setItems(items).setBytes(bytes).build()));

    verify(streamFactory, times(2))
        .createDirectGetWorkStream(
            any(), eq(getWorkRequest(0, 0)), any(), any(), any(), eq(noOpProcessWorkItemFn()));

    verify(streamFactory, times(2)).createDirectGetDataStream(any());
    verify(streamFactory, times(2)).createDirectCommitWorkStream(any());
  }

  @Test
  public void testOnNewWorkerMetadata_correctlyRemovesStaleWindmillServers()
      throws InterruptedException {
    GetWorkBudgetDistributor getWorkBudgetDistributor = mock(GetWorkBudgetDistributor.class);
    fanOutStreamingEngineWorkProvider =
        newFanOutStreamingEngineWorkerHarness(
            GetWorkBudget.builder().setItems(1).setBytes(1).build(),
            getWorkBudgetDistributor,
            noOpProcessWorkItemFn());

    String workerToken = "workerToken1";
    String workerToken2 = "workerToken2";
    String workerToken3 = "workerToken3";

    WorkerMetadataResponse firstWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder()
                    .setBackendWorkerToken(workerToken)
                    .build())
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder()
                    .setBackendWorkerToken(workerToken2)
                    .build())
            .setExternalEndpoint(AUTHENTICATING_SERVICE)
            .setEndpointType(EndpointType.DIRECTPATH)
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();
    WorkerMetadataResponse secondWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(2)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder()
                    .setBackendWorkerToken(workerToken3)
                    .build())
            .setExternalEndpoint(AUTHENTICATING_SERVICE)
            .setEndpointType(EndpointType.DIRECTPATH)
            .build();

    fakeGetWorkerMetadataStub.injectWorkerMetadata(firstWorkerMetadata);
    fakeGetWorkerMetadataStub.injectWorkerMetadata(secondWorkerMetadata);
    StreamingEngineBackends currentBackends = fanOutStreamingEngineWorkProvider.currentBackends();
    assertEquals(1, currentBackends.windmillStreams().size());
    Set<String> workerTokens =
        fanOutStreamingEngineWorkProvider.currentBackends().windmillStreams().keySet().stream()
            .map(endpoint -> endpoint.workerToken().orElseThrow(IllegalStateException::new))
            .collect(Collectors.toSet());

    assertFalse(workerTokens.contains(workerToken));
    assertFalse(workerTokens.contains(workerToken2));
    assertTrue(currentBackends.globalDataStreams().isEmpty());
  }

  @Test
  public void testOnNewWorkerMetadata_endpointTypeChanged() throws InterruptedException {
    GetWorkBudgetDistributor getWorkBudgetDistributor = mock(GetWorkBudgetDistributor.class);
    fanOutStreamingEngineWorkProvider =
        newFanOutStreamingEngineWorkerHarness(
            GetWorkBudget.builder().setItems(1).setBytes(1).build(),
            getWorkBudgetDistributor,
            noOpProcessWorkItemFn());

    String workerToken = "workerToken1";
    String workerToken2 = "workerToken2";

    WorkerMetadataResponse firstWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder()
                    .setBackendWorkerToken(workerToken)
                    .build())
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder()
                    .setBackendWorkerToken(workerToken2)
                    .build())
            .setExternalEndpoint(AUTHENTICATING_SERVICE)
            .setEndpointType(EndpointType.DIRECTPATH)
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();

    WorkerMetadataResponse secondWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder()
                    .setDirectEndpoint(
                        DEFAULT_WINDMILL_SERVICE_ADDRESS.gcpServiceAddress().toString())
                    .build())
            .setExternalEndpoint(AUTHENTICATING_SERVICE)
            .setEndpointType(EndpointType.CLOUDPATH)
            .build();

    fakeGetWorkerMetadataStub.injectWorkerMetadata(firstWorkerMetadata);
    StreamingEngineBackends currentBackends = fanOutStreamingEngineWorkProvider.currentBackends();
    assertEquals(2, currentBackends.windmillStreams().size());
    Set<String> workerTokens =
        fanOutStreamingEngineWorkProvider.currentBackends().windmillStreams().keySet().stream()
            .map(endpoint -> endpoint.workerToken().orElseThrow(IllegalStateException::new))
            .collect(Collectors.toSet());
    assertTrue(workerTokens.contains(workerToken));
    assertTrue(workerTokens.contains(workerToken2));

    fakeGetWorkerMetadataStub.injectWorkerMetadata(secondWorkerMetadata);
    currentBackends = fanOutStreamingEngineWorkProvider.currentBackends();
    assertEquals(1, currentBackends.windmillStreams().size());
    Set<String> directEndpointStrings =
        fanOutStreamingEngineWorkProvider.currentBackends().windmillStreams().keySet().stream()
            .filter(endpoint -> endpoint.directEndpoint().isPresent())
            .map(endpoint -> endpoint.directEndpoint().get())
            .map(serviceAddress -> serviceAddress.getServiceAddress().toString())
            .collect(Collectors.toSet());
    assertTrue(
        directEndpointStrings.contains(
            DEFAULT_WINDMILL_SERVICE_ADDRESS.gcpServiceAddress().toString()));
  }

  @Test
  public void testOnNewWorkerMetadata_redistributesBudget() throws InterruptedException {
    String workerToken = "workerToken1";
    String workerToken2 = "workerToken2";

    WorkerMetadataResponse firstWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder()
                    .setBackendWorkerToken(workerToken)
                    .build())
            .setEndpointType(EndpointType.DIRECTPATH)
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();
    WorkerMetadataResponse secondWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(2)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder()
                    .setBackendWorkerToken(workerToken2)
                    .build())
            .setExternalEndpoint(AUTHENTICATING_SERVICE)
            .setEndpointType(EndpointType.DIRECTPATH)
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();

    TestGetWorkBudgetDistributor getWorkBudgetDistributor = spy(new TestGetWorkBudgetDistributor());
    fanOutStreamingEngineWorkProvider =
        newFanOutStreamingEngineWorkerHarness(
            GetWorkBudget.builder().setItems(1).setBytes(1).build(),
            getWorkBudgetDistributor,
            noOpProcessWorkItemFn());

    fakeGetWorkerMetadataStub.injectWorkerMetadata(firstWorkerMetadata);
    verify(getWorkBudgetDistributor, times(1)).distributeBudget(any(), any());
    TimeUnit.SECONDS.sleep(WAIT_FOR_METADATA_INJECTIONS_SECONDS);
    fakeGetWorkerMetadataStub.injectWorkerMetadata(secondWorkerMetadata);
    verify(getWorkBudgetDistributor, times(2)).distributeBudget(any(), any());
    TimeUnit.SECONDS.sleep(WAIT_FOR_METADATA_INJECTIONS_SECONDS);
  }

  private static class WindmillServiceFakeStub
      extends CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1ImplBase {
    @Override
    public StreamObserver<Windmill.StreamingGetDataRequest> getDataStream(
        StreamObserver<Windmill.StreamingGetDataResponse> responseObserver) {
      return new StreamObserver<Windmill.StreamingGetDataRequest>() {
        @Override
        public void onNext(Windmill.StreamingGetDataRequest getDataRequest) {}

        @Override
        public void onError(Throwable throwable) {
          responseObserver.onError(throwable);
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      };
    }

    @Override
    public StreamObserver<Windmill.StreamingGetWorkRequest> getWorkStream(
        StreamObserver<Windmill.StreamingGetWorkResponseChunk> responseObserver) {
      return new StreamObserver<Windmill.StreamingGetWorkRequest>() {
        @Override
        public void onNext(Windmill.StreamingGetWorkRequest getWorkRequest) {}

        @Override
        public void onError(Throwable throwable) {
          responseObserver.onError(throwable);
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      };
    }

    @Override
    public StreamObserver<Windmill.StreamingCommitWorkRequest> commitWorkStream(
        StreamObserver<Windmill.StreamingCommitResponse> responseObserver) {
      return new StreamObserver<Windmill.StreamingCommitWorkRequest>() {
        @Override
        public void onNext(Windmill.StreamingCommitWorkRequest streamingCommitWorkRequest) {}

        @Override
        public void onError(Throwable throwable) {
          responseObserver.onError(throwable);
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      };
    }
  }

  private static class GetWorkerMetadataTestStub
      extends CloudWindmillMetadataServiceV1Alpha1Grpc
          .CloudWindmillMetadataServiceV1Alpha1ImplBase {
    private final CountDownLatch ready;
    private @Nullable StreamObserver<WorkerMetadataResponse> responseObserver;

    private GetWorkerMetadataTestStub(CountDownLatch ready) {
      this.ready = ready;
    }

    @Override
    public StreamObserver<WorkerMetadataRequest> getWorkerMetadata(
        StreamObserver<WorkerMetadataResponse> responseObserver) {
      if (this.responseObserver == null) {
        ready.countDown();
        this.responseObserver = responseObserver;
      }

      return new StreamObserver<WorkerMetadataRequest>() {
        @Override
        public void onNext(WorkerMetadataRequest workerMetadataRequest) {}

        @Override
        public void onError(Throwable throwable) {
          if (responseObserver != null) {
            responseObserver.onError(throwable);
          }
        }

        @Override
        public void onCompleted() {
          if (responseObserver != null) {
            responseObserver.onCompleted();
          }
        }
      };
    }

    private void injectWorkerMetadata(WorkerMetadataResponse response) {
      if (responseObserver != null) {
        responseObserver.onNext(response);
      }
    }
  }

  private static class TestGetWorkBudgetDistributor implements GetWorkBudgetDistributor {
    @Override
    public <T extends GetWorkBudgetSpender> void distributeBudget(
        ImmutableCollection<T> streams, GetWorkBudget getWorkBudget) {
      streams.forEach(stream -> stream.setBudget(getWorkBudget.items(), getWorkBudget.bytes()));
    }
  }
}
