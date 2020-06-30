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



#ifndef SCC_RPC_RPC_SERVER_H
#define SCC_RPC_RPC_SERVER_H

#include "rpc/message_channel.h"
#include "messages/rpc_messages.pb.h"
#include <memory>

namespace scc {

class RPCServer {
public:
  RPCServer(TCPSocket* socket);
  bool RecvRequest(PbRpcRequest& request);
  bool SendReply(PbRpcReply& reply);
private:
  MessageChannel _mc;
  std::string _clientName;
  int _clientPort;
};

typedef std::shared_ptr<RPCServer> RPCServerPtr;

}

#endif
