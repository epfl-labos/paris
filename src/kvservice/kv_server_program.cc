#include "kvservice/kv_server.h"
#include "common/sys_config.h"
#include "common/utils.h"
#include "common/types.h"
#include <stdlib.h>
#include <iostream>

using namespace scc;
const int ARG_NUM = 19;

int main(int argc, char *argv[]) {

    fprintf(stdout, "[INFO]: Starting kv_server.\n");
    if (argc == ARG_NUM) {
        SysConfig::Consistency = Utils::str2consistency(argv[1]);
        std::string serverName = argv[2];
        int publicPort = atoi(argv[3]);
        int partitionPort = atoi(argv[4]);
        int replicationPort = atoi(argv[5]);
        int partitionId = atoi(argv[6]);
        int replicaId = atoi(argv[7]);
        int totalNumKeys = atoi(argv[8]);
        std::string duraStr = argv[9];

        if (duraStr == "Disk") {
            SysConfig::DurabilityOption = DurabilityType::Disk;
        } else if (duraStr == "Memory") {
            SysConfig::DurabilityOption = DurabilityType::Memory;
        } else {
            fprintf(stdout, "<Durability: Disk/Memory>\n");
            exit(1);
        }
        std::string groupServerName = argv[10];
        int groupServerPort = atoi(argv[11]);

        std::string gstStr = argv[12];
        if (gstStr == "tree") {
            SysConfig::GSTDerivationMode = GSTDerivationType::TREE;
            cout << "[INFO]: GSTDerivationMode set to TREE." << endl;
        } else {
            cout << "[INFO]: GSTDerivationMode currently not supported!" << endl;
            fprintf(stdout, "<GSTDerivationMode: tree>\n");
            exit(1);
        }

        SysConfig::RSTComputationInterval = atoi(argv[13]);
        SysConfig::GSTComputationInterval = atoi(argv[13]);

        SysConfig::UpdatePropagationBatchTime = atoi(argv[14]);
        SysConfig::ReplicationHeartbeatInterval = atoi(argv[15]);

        SysConfig::GetSpinTime = atoi(argv[16]);

        SysConfig::NumChannelsPartition = atoi(argv[17]);
        int replicationFactor = atoi(argv[18]);

        // start server
        KVTxServer server(serverName, publicPort, partitionPort, replicationPort, partitionId, replicaId, totalNumKeys,
                          groupServerName, groupServerPort, replicationFactor);

        server.Run();

    } else {
        cout << "[INFO]: You provided " << argc << " arguments instead of " << ARG_NUM << " !\n";
        fprintf(stdout,
                "Usage: %s <Causal> <KVServerName> <PublicPort> <PartitionPort> <ReplicationPort> <PartitionId> <ReplicaId> <TotalNumKeys> <Durability: Disk/Memory> <GroupServerName> <GroupServerPort> <GSTDerivationMode: tree/broadcast> <DST/GST Interval (us)> <UpdatePropagationBatchTime (us)> <ReplicationHeartbeatInterval (us)> <GetSpinTime>\n",
                argv[0]);
        exit(1);
    }
}

// /opt/gentlerain/build/kv_server_program Causal dco-node001 3000 5000 4000 0 0 999999 Memory dco-node001 2000 tree 5000 999999 1000 5000
// /opt/gentlerain/build/kv_server_program Causal dco-node002 3000 5000 4000 1 0 999999 Memory dco-node001 2000 tree 5000 999999 1000 5000
// /opt/gentlerain/build/kv_server_program Causal dco-node003 3000 5000 4000 0 1 999999 Memory dco-node001 2000 tree 5000 999999 1000 5000
// /opt/gentlerain/build/kv_server_program Causal dco-node001 3000 5000 4000 1 1 999999 Memory dco-node001 2000 tree 5000 999999 1000 5000