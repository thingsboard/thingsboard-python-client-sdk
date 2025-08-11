#  Copyright 2025 ThingsBoard
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

RPC_RESPONSE_RULE_CHAIN = {
  "ruleChain": {
    "name": "test-client-side-rpc",
    "type": "CORE",
    "firstRuleNodeId": None,
    "root": False,
    "debugMode": False,
    "configuration": None,
    "additionalInfo": {
      "description": ""
    }
  },
  "metadata": {
    "ruleChainId": {
    },
    "version": 3,
    "firstNodeIndex": 0,
    "nodes": [
        {
            "createdTime": 1754561486165,
            "type": "org.thingsboard.rule.engine.filter.TbMsgTypeSwitchNode",
            "name": "msg_type_switch",
            "debugSettings": None,
            "singletonMode": False,
            "queueName": None,
            "configurationVersion": 0,
            "configuration": {
                "version": 0
            },
            "externalId": None,
            "additionalInfo": {
                "description": "",
                "layoutX": 277,
                "layoutY": 150
            }
        },
        {
            "createdTime": 1754561486166,
            "type": "org.thingsboard.rule.engine.rpc.TbSendRPCReplyNode",
            "name": "rpc_reply",
            "debugSettings": None,
            "singletonMode": False,
            "queueName": None,
            "configurationVersion": 0,
            "configuration": {
                "serviceIdMetaDataAttribute": "serviceId",
                "sessionIdMetaDataAttribute": "sessionId",
                "requestIdMetaDataAttribute": "requestId"
            },
            "externalId": None,
            "additionalInfo": {
                "description": "",
                "layoutX": 542,
                "layoutY": 153
            }
        }
    ],
    "connections": [
        {
            "fromIndex": 0,
            "toIndex": 1,
            "type": "RPC Request from Device"
        }
    ],
    "ruleChainConnections": None
}
}