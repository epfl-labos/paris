
# PaRiS

## What is PaRiS?

PaRiS(**Pa**rtially **R**epl**i**cated **S**ystem) is the first transactional causally consistent system that supports partial replication and implements non-blocking parallel read operations. The latter reduce read latency which is of paramount importance for the performance of read-intensive applications. PaRiS relies on a novel protocol to track dependencies, called Universal Stable Time (UST). By means of a lightweight background gossip process, UST identifies a snapshot of the data that has been installed by every data center in the system. Hence, transactions can consistently read from such a snapshot on any server in any replication site without having to block. Moreover, PaRiS requires only one timestamp to track dependencies and define transactional snapshots, thereby achieving resource efficiency and scalability.
You can read more about PaRiS in the paper "PaRiS: Causally Consistent Transactions with Non-blocking Reads and Partial Replication", presented at the 39th IEEE International Conference on Distributed Computing Systems (ICDCS'19) (7-9 July 2019, Dallas, Texas).

You can find the paper at:
https://infoscience.epfl.ch/record/264093?ln=en

## Compilation

In order to compile this project\'s code, you need to install gcc4.8, protobuf-2.6.2, boost-1.63.0 and gtest-1.7.0.
The default project and build directories are `/paris` and `/paris/build`. You can change them by changing the PROJECT and BUILD values in the Makefile.
The code is then compiled simply by running twice:

```
    $make all
```

## Running PaRiS

In order to run PaRiS, you need to run the group manager, the server and client programs.

The group manager program can be run with:

```
    $/paris/build/group_server_program <Port> <NumPartitions> <NumReplicasPerPartition> <ReplicationFactor>
```

A server program can be run with:

```
    $/paris/build/kv_server_program <Causal> <KVServerName> <PublicPort> <PartitionPort> <ReplicationPort> <PartitionId> <ReplicaId> <TotalNumKeys> <Durability: Memory> <GroupServerName> <GroupServerPort> <GSTDerivationMode: tree> <GSTInterval (us)> <UpdatePropagationBatchTime (us)> <ReplicationHeartbeatInterval (us)> <GetSpinTime (us)> <NumChannelsPerPartition: default 4> <ReplicationFactor>"
```

A client program can be run with:

```
$/paris/build/run_experiments <Causal> <RequestDistribution: zipfian/uniform> <RequestDistributionParameter> <ReadRatio> <WriteRatio> <LocalDCOnlyTransactionsRatio> <LocalRemoteDCTransactionsRatio> <GroupManagerName> <GroupManagerPort> <TotalNumItems> <WriteValueSize> <NumOpsPerThread: default 0> <NumThreads> <ServingPartitionId> <ServingReplicaId> <ClientServerName> <OutputResultsFileName> <ReservoirSampling: default false> <ReservoirSamplingLimit> <ExperimentDuration: (ms)> <ExperimentType> <ClientSessionResetLimit> <EnableClientSessionResetLimit: default false> <NumTxReadItems> <NumTxWriteItems>\n"
```

Additionally, you can run an interactive client program with:

```
    $/paris/build/interactive_kv_client_program <Causal> <ServerName> <ServerPort>
```

and provide it with the command, or
```
    $/paris/build/interactive_kv_client_program <Causal> <ServerName> <ServerPort> <Command>
```

where 'Command' can be one of:

- TxStart
- Read k1,k2, ..,kn
- Write k1,k2, ..,kn v1,v2, ..,vn
- TxCommit

You can run three protocols with this code. PaRiS is the default protocol. If you want to run the BPR(**B**locking **P**artial **R**eplication) protocol or the eventual consistent system you need to set `-DBLOCKING_ON` or `-DEVENTUAL_CONSISTENCY` in the make file instead of `-DWREN` or define them in the code using the `#define` directive.

## Licensing

**PaRiS is released under the Apache License, Version 2.0.**

Please see the LICENSE file.

## Contact

- Kristina Spirovska <kristina.spirovska@epfl.ch>
- Diego Didona <diego.didona@epfl.ch>

## Contributors

- Kristina Spirovska
- Diego Didona
- Jiaqing Du
- Calin Iorgulescu
- Amitabha Roy
- Sameh Elnikety


