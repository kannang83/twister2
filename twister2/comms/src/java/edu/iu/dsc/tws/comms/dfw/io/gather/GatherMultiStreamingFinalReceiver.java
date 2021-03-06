//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.comms.dfw.io.gather;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MultiMessageReceiver;
import edu.iu.dsc.tws.comms.dfw.io.KeyedReceiver;
import edu.iu.dsc.tws.comms.dfw.io.gather.keyed.KGatherStreamingFinalReceiver;

public class GatherMultiStreamingFinalReceiver implements MultiMessageReceiver {

  /**
   * The final receiver specified by the user
   */
  private BulkReceiver bulkReceiver;

  /**
   * Map that keeps final receivers for each target
   */
  private Map<Integer, KeyedReceiver> receiverMap = new HashMap<>();

  public GatherMultiStreamingFinalReceiver(BulkReceiver receiver) {
    this.bulkReceiver = receiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op,
                   Map<Integer, Map<Integer, List<Integer>>> expectedIds) {
    for (Map.Entry<Integer, Map<Integer, List<Integer>>> e : expectedIds.entrySet()) {
      KGatherStreamingFinalReceiver finalReceiver = new KGatherStreamingFinalReceiver(
          bulkReceiver, 1);
      receiverMap.put(e.getKey(), finalReceiver);
      finalReceiver.init(cfg, op, e.getValue());


    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    KeyedReceiver finalReceiver = receiverMap.get(path);
    return finalReceiver.onMessage(source, path, target, flags, object);
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;

    for (Map.Entry<Integer, KeyedReceiver> e : receiverMap.entrySet()) {
      needsFurtherProgress = needsFurtherProgress | e.getValue().progress();
    }
    return needsFurtherProgress;
  }
}
