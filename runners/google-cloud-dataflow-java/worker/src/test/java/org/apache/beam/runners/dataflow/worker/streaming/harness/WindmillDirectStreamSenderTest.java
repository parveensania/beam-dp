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

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.testing.GrpcCleanupRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WindmillDirectStreamSenderTest {
  private static final GetWorkRequest GET_WORK_REQUEST =
      GetWorkRequest.newBuilder().setClientId(1L).setJobId("job").setProjectId("project").build();
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private final GrpcWindmillStreamFactory streamFactory =
      spy(
          GrpcWindmillStreamFactory.of(
                  JobHeader.newBuilder()
                      .setJobId("job")
                      .setProjectId("project")
                      .setWorkerId("worker")
                      .build())
              .build());
  private final WorkItemScheduler workItemScheduler =
      (workItem,
          serializedWorkItemSize,
          watermarks,
          processingContext,
          getWorkStreamLatencies) -> {};
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  private ManagedChannel inProcessChannel;
  private WindmillConnection connection;

  @Before
  public void setUp() {
    inProcessChannel =
        grpcCleanup.register(
            InProcessChannelBuilder.forName("WindmillDirectStreamSenderTest")
                .directExecutor()
                .build());
    grpcCleanup.register(inProcessChannel);
    connection =
        WindmillConnection.builder()
            .setStubSupplier(() -> CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel))
            .build();
  }

  @After
  public void cleanUp() {
    inProcessChannel.shutdownNow();
  }

  @Test
  public void testStartStream_startsAllStreams() {
    long itemBudget = 1L;
    long byteBudget = 1L;

    WindmillDirectStreamSender windmillStreamSender =
        newWindmillStreamSender(
            GetWorkBudget.builder().setBytes(byteBudget).setItems(itemBudget).build());

    windmillStreamSender.start();

    verify(streamFactory)
        .createDirectGetWorkStream(
            eq(connection),
            eq(
                GET_WORK_REQUEST
                    .toBuilder()
                    .setMaxItems(itemBudget)
                    .setMaxBytes(byteBudget)
                    .build()),
            any(),
            any(),
            any(),
            eq(workItemScheduler));

    verify(streamFactory).createDirectGetDataStream(eq(connection));
    verify(streamFactory).createDirectCommitWorkStream(eq(connection));
  }

  @Test
  public void testStartStream_onlyStartsStreamsOnce() {
    long itemBudget = 1L;
    long byteBudget = 1L;

    WindmillDirectStreamSender windmillStreamSender =
        newWindmillStreamSender(
            GetWorkBudget.builder().setBytes(byteBudget).setItems(itemBudget).build());

    windmillStreamSender.start();
    windmillStreamSender.start();
    windmillStreamSender.start();

    verify(streamFactory, times(1))
        .createDirectGetWorkStream(
            eq(connection),
            eq(
                GET_WORK_REQUEST
                    .toBuilder()
                    .setMaxItems(itemBudget)
                    .setMaxBytes(byteBudget)
                    .build()),
            any(),
            any(),
            any(),
            eq(workItemScheduler));

    verify(streamFactory, times(1)).createDirectGetDataStream(eq(connection));
    verify(streamFactory, times(1)).createDirectCommitWorkStream(eq(connection));
  }

  @Test
  public void testStartStream_onlyStartsStreamsOnceConcurrent() throws InterruptedException {
    long itemBudget = 1L;
    long byteBudget = 1L;

    WindmillDirectStreamSender windmillStreamSender =
        newWindmillStreamSender(
            GetWorkBudget.builder().setBytes(byteBudget).setItems(itemBudget).build());

    Thread startStreamThread = new Thread(windmillStreamSender::start);
    startStreamThread.start();

    windmillStreamSender.start();

    startStreamThread.join();

    verify(streamFactory, times(1))
        .createDirectGetWorkStream(
            eq(connection),
            eq(
                GET_WORK_REQUEST
                    .toBuilder()
                    .setMaxItems(itemBudget)
                    .setMaxBytes(byteBudget)
                    .build()),
            any(),
            any(),
            any(),
            eq(workItemScheduler));

    verify(streamFactory, times(1)).createDirectGetDataStream(eq(connection));
    verify(streamFactory, times(1)).createDirectCommitWorkStream(eq(connection));
  }

  @Test
  public void testCloseAllStreams_closesAllStreams() {
    long itemBudget = 1L;
    long byteBudget = 1L;
    GetWorkRequest getWorkRequestWithBudget =
        GET_WORK_REQUEST.toBuilder().setMaxItems(itemBudget).setMaxBytes(byteBudget).build();
    GrpcWindmillStreamFactory mockStreamFactory = mock(GrpcWindmillStreamFactory.class);
    GetWorkStream mockGetWorkStream = mock(GetWorkStream.class);
    GetDataStream mockGetDataStream = mock(GetDataStream.class);
    CommitWorkStream mockCommitWorkStream = mock(CommitWorkStream.class);

    when(mockStreamFactory.createDirectGetWorkStream(
            eq(connection),
            eq(getWorkRequestWithBudget),
            any(),
            any(),
            any(),
            eq(workItemScheduler)))
        .thenReturn(mockGetWorkStream);

    when(mockStreamFactory.createDirectGetDataStream(eq(connection))).thenReturn(mockGetDataStream);
    when(mockStreamFactory.createDirectCommitWorkStream(eq(connection)))
        .thenReturn(mockCommitWorkStream);

    WindmillDirectStreamSender windmillStreamSender =
        newWindmillStreamSender(
            GetWorkBudget.builder().setBytes(byteBudget).setItems(itemBudget).build(),
            mockStreamFactory);

    windmillStreamSender.start();
    windmillStreamSender.close();

    verify(mockGetWorkStream).shutdown();
    verify(mockGetDataStream).shutdown();
    verify(mockCommitWorkStream).shutdown();
  }

  @Test
  public void testCloseAllStreams_doesNotStartStreamsAfterClose() {
    long itemBudget = 1L;
    long byteBudget = 1L;
    GetWorkRequest getWorkRequestWithBudget =
        GET_WORK_REQUEST.toBuilder().setMaxItems(itemBudget).setMaxBytes(byteBudget).build();
    GrpcWindmillStreamFactory mockStreamFactory = mock(GrpcWindmillStreamFactory.class);
    GetWorkStream mockGetWorkStream = mock(GetWorkStream.class);
    GetDataStream mockGetDataStream = mock(GetDataStream.class);
    CommitWorkStream mockCommitWorkStream = mock(CommitWorkStream.class);

    when(mockStreamFactory.createDirectGetWorkStream(
            eq(connection),
            eq(getWorkRequestWithBudget),
            any(),
            any(),
            any(),
            eq(workItemScheduler)))
        .thenReturn(mockGetWorkStream);

    when(mockStreamFactory.createDirectGetDataStream(eq(connection))).thenReturn(mockGetDataStream);
    when(mockStreamFactory.createDirectCommitWorkStream(eq(connection)))
        .thenReturn(mockCommitWorkStream);

    WindmillDirectStreamSender windmillStreamSender =
        newWindmillStreamSender(
            GetWorkBudget.builder().setBytes(byteBudget).setItems(itemBudget).build(),
            mockStreamFactory);

    windmillStreamSender.close();

    verify(mockGetWorkStream, times(0)).start();
    verify(mockGetDataStream, times(0)).start();
    verify(mockCommitWorkStream, times(0)).start();

    verify(mockGetWorkStream).shutdown();
    verify(mockGetDataStream).shutdown();
    verify(mockCommitWorkStream).shutdown();
  }

  @Test
  public void testStartStream_afterCloseThrows() {
    long itemBudget = 1L;
    long byteBudget = 1L;

    WindmillDirectStreamSender windmillStreamSender =
        newWindmillStreamSender(
            GetWorkBudget.builder().setBytes(byteBudget).setItems(itemBudget).build());

    windmillStreamSender.close();
    assertThrows(IllegalStateException.class, windmillStreamSender::start);
  }

  private WindmillDirectStreamSender newWindmillStreamSender(GetWorkBudget budget) {
    return newWindmillStreamSender(budget, streamFactory);
  }

  private WindmillDirectStreamSender newWindmillStreamSender(
      GetWorkBudget budget, GrpcWindmillStreamFactory streamFactory) {
    return WindmillDirectStreamSender.create(
        connection,
        GET_WORK_REQUEST,
        budget,
        streamFactory,
        workItemScheduler,
        ignored -> mock(GetDataClient.class),
        ignored -> mock(WorkCommitter.class));
  }
}
