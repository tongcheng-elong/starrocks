// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.rpc;

import com.baidu.jprotobuf.pbrpc.ProtobufRPC;
import com.starrocks.proto.ExecuteCommandRequestPB;
import com.starrocks.proto.ExecuteCommandResultPB;
import com.starrocks.proto.PCancelPlanFragmentRequest;
import com.starrocks.proto.PCancelPlanFragmentResult;
import com.starrocks.proto.PCollectQueryStatisticsResult;
import com.starrocks.proto.PExecBatchPlanFragmentsResult;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PExecShortCircuitResult;
import com.starrocks.proto.PFetchArrowSchemaRequest;
import com.starrocks.proto.PFetchArrowSchemaResult;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.proto.PGetFileSchemaResult;
import com.starrocks.proto.PListFailPointResponse;
import com.starrocks.proto.PMVMaintenanceTaskResult;
import com.starrocks.proto.PProcessDictionaryCacheRequest;
import com.starrocks.proto.PProcessDictionaryCacheResult;
import com.starrocks.proto.PProxyRequest;
import com.starrocks.proto.PProxyResult;
import com.starrocks.proto.PPulsarProxyRequest;
import com.starrocks.proto.PPulsarProxyResult;
import com.starrocks.proto.PTriggerProfileReportResult;
import com.starrocks.proto.PUpdateFailPointStatusRequest;
import com.starrocks.proto.PUpdateFailPointStatusResponse;
import com.starrocks.proto.PUpdateTransactionStateRequest;
import com.starrocks.proto.PUpdateTransactionStateResponse;

import java.util.concurrent.Future;

public interface PBackendService {
    @ProtobufRPC(serviceName = "PInternalService", methodName = "exec_plan_fragment",
            attachmentHandler = ThriftClientAttachmentHandler.class, onceTalkTimeout = 60000)
    Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "exec_batch_plan_fragments",
            attachmentHandler = ThriftClientAttachmentHandler.class, onceTalkTimeout = 60000)
    Future<PExecBatchPlanFragmentsResult> execBatchPlanFragmentsAsync(PExecBatchPlanFragmentsRequest request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "cancel_plan_fragment",
            onceTalkTimeout = 5000)
    Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(PCancelPlanFragmentRequest request);

    // we set timeout to 1 day, because now there is no way to give different timeout for each RPC call
    @ProtobufRPC(serviceName = "PInternalService", methodName = "fetch_data",
            attachmentHandler = ThriftClientAttachmentHandler.class, onceTalkTimeout = 86400000)
    Future<PFetchDataResult> fetchDataAsync(PFetchDataRequest request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "trigger_profile_report",
            attachmentHandler = ThriftClientAttachmentHandler.class, onceTalkTimeout = 10000)
    Future<PTriggerProfileReportResult> triggerProfileReport(PTriggerProfileReportRequest request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "collect_query_statistics",
            attachmentHandler = ThriftClientAttachmentHandler.class, onceTalkTimeout = 10000)
    Future<PCollectQueryStatisticsResult> collectQueryStatistics(PCollectQueryStatisticsRequest request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "get_info", onceTalkTimeout = 600000)
    Future<PProxyResult> getInfo(PProxyRequest request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "get_pulsar_info", onceTalkTimeout = 600000)
    Future<PPulsarProxyResult> getPulsarInfo(PPulsarProxyRequest request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "get_file_schema",
            attachmentHandler = ThriftClientAttachmentHandler.class, onceTalkTimeout = 600000)
    Future<PGetFileSchemaResult> getFileSchema(PGetFileSchemaRequest request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "submit_mv_maintenance_task", onceTalkTimeout = 60000,
            attachmentHandler = ThriftClientAttachmentHandler.class)
    Future<PMVMaintenanceTaskResult> submitMVMaintenanceTaskAsync(PMVMaintenanceTaskRequest request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "execute_command", onceTalkTimeout = 600000)
    Future<ExecuteCommandResultPB> executeCommandAsync(ExecuteCommandRequestPB request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "update_fail_point_status", onceTalkTimeout = 60000)
    Future<PUpdateFailPointStatusResponse> updateFailPointStatusAsync(PUpdateFailPointStatusRequest request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "list_fail_point",
            attachmentHandler = ThriftClientAttachmentHandler.class, onceTalkTimeout = 60000)
    Future<PListFailPointResponse> listFailPointAsync(PListFailPointRequest request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "exec_short_circuit",
            attachmentHandler = ThriftClientAttachmentHandler.class, onceTalkTimeout = 60000)
    Future<PExecShortCircuitResult> execShortCircuit(PExecShortCircuitRequest request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "process_dictionary_cache", onceTalkTimeout = 600000)
    Future<PProcessDictionaryCacheResult> processDictionaryCache(PProcessDictionaryCacheRequest request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "fetch_arrow_schema", onceTalkTimeout = 600000)
    Future<PFetchArrowSchemaResult> fetchArrowSchema(PFetchArrowSchemaRequest request);

    @ProtobufRPC(serviceName = "PInternalService", methodName = "update_transaction_state", onceTalkTimeout = 600000)
    Future<PUpdateTransactionStateResponse> updateTransactionState(PUpdateTransactionStateRequest request);
}

