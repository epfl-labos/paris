/*
 * PaRiS 
 *
 * Copyright 2019 Operating Systems Laboratory EPFL
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



#ifndef SCC_RPC_RPC_ID_H
#define SCC_RPC_RPC_ID_H

namespace scc {

    enum class RPCMethod {
        // key-value server
        EchoTest = 1,
        GetServerConfig,
        Add,
        Get,
        Set,
        TxGet,
        ShowItem, // show the version chain of one key
        ShowDB, // show the version chains of all items
        ShowState,
        ShowStateCSV,
        DumpLatencyMeasurement,
        GetRemotePartitionsIds,

        // partiton
        InternalGet,
        InternalSet,
        InternalTxSliceGet,
        InternalShowItem,
        InitializePartitioning,
        InternalDependencyCheck,

        // replication,
        InitializeReplication,
        ReplicateUpdate,
        SendHeartbeat,
        PushGST,
        SendGSTAndUST,

        // GST
        SendLST,
        SendGST,
        SendGSTRequest,
#ifdef DEP_VECTORS
        SendPVV,
      SendGSV,
      SendGSVRequest,
#endif

        InternalTxSliceGetResult,
        ParallelInternalTxSliceGetResult,
        InternalTxSliceReadKeys,
        InternalTxSliceReadKeysResult,
        InternalPrepareRequest,
        InternalPrepareReply,
        InternalCommitRequest,

        // manager
        RegisterPartition,
        GetRegisteredPartitions,
        GetReplicationFactor,
        GetPartitionUsersIds,
        NotifyReadiness,

// read-write transactions
        TxStart,
        TxCommit,
        TxWrite,
        TxRead

//stabilization protocol
        ,SendStabilizationTimes



    };

    static const char *RPCMethodS[] = {
            "EchoTest",
            "GetServerConfig",
            "Add",
            "Get",
            "Set",
            "TxGet",
            "ShowItem",
            "ShowDB",
            "ShowState",
            "ShowStateCSV",
            "DumpLatencyMeasurement",
            "GetRemotePartitionsIds",

            "InternalGet",
            "InternalSet",
            "InternalTxSliceGet",
            "InternalShowItem",
            "InitializePartitioning",
            "InternalDependencyCheck",

            "InitializeReplication",
            "ReplicateUpdate",
            "SendHeartbeat",
            "PushGST",
            "SendGSTAndUST",
            "SendLST",
            "SendGST",
            "SendGSTRequest",
#ifdef DEP_VECTORS
    "SendPVV",
    "SendGSV",
    "SendGSVRequest",
#endif

            "InternalTxSliceGetResult",
            "ParallelInternalTxSliceGetResult",
            "InternalTxSliceReadKeys",
            "InternalTxSliceReadKeysResult",
            "InternalPrepareRequest",
            "InternalPrepareReply",
            "InternalCommitRequest",


            "RegisterPartition",
            "GetRegisteredPartitions",
            "GetReplicationFactor",
            "GetPartitionUsersIds",
            "NotifyReadiness",

            "TxStart",
            "TxCommit",
            "TxWrite",
            "TxRead"
            ,"SendStabilizationTimes"


    };

    const char *getTextForEnum(RPCMethod rid) {
        return RPCMethodS[((int) rid) - 1];
    }

}

#endif