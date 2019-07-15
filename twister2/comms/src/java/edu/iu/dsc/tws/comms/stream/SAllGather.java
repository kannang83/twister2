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
package edu.iu.dsc.tws.comms.stream;

import java.util.Set;

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.comms.dfw.AllGather;

/**
 * Streaming ALLGather Operation
 */
public class SAllGather {
  /**
   * The actual operation
   */
  private AllGather gather;

  /**
   * Construct a Streaming AllGather operation
   *
   * @param comm the communicator
   * @param plan task plan
   * @param sources source tasks
   * @param targets target tasks
   * @param rcvr receiver
   * @param dataType data type
   */
  public SAllGather(Communicator comm, LogicalPlan plan,
                    Set<Integer> sources, Set<Integer> targets,
                    BulkReceiver rcvr, MessageType dataType, int gtrEdgeId, int bcstEdgeId) {
    if (sources.size() == 0) {
      throw new IllegalArgumentException("The sources cannot be empty");
    }

    if (targets.size() == 0) {
      throw new IllegalArgumentException("The destination cannot be empty");
    }
    int middleTask = comm.nextId();

    int firstSource = sources.iterator().next();
    plan.addChannelToExecutor(plan.getExecutorForChannel(firstSource), middleTask);
    gather = new AllGather(comm.getConfig(), comm.getChannel(), plan, sources, targets,
        middleTask, rcvr, dataType, gtrEdgeId, bcstEdgeId, true);
  }

  public SAllGather(Communicator comm, LogicalPlan plan,
                    Set<Integer> sources, Set<Integer> targets,
                    BulkReceiver rcvr, MessageType dataType) {
    this(comm, plan, sources, targets, rcvr, dataType, comm.nextEdge(), comm.nextEdge());
  }

  /**
   * Send a message to be gathered
   *
   * @param src source
   * @param message message
   * @param flags message flag
   * @return true if the message is accepted
   */
  public boolean gather(int src, Object message, int flags) {
    return gather.send(src, message, flags);
  }

  /**
   * Progress the gather operation, if not called, messages will not be processed
   *
   * @return true if further progress is needed
   */
  public boolean progress() {
    return gather.progress();
  }

  /**
   * Weather we have messages pending
   *
   * @return true if there are messages pending
   */
  public boolean hasPending() {
    return !gather.isComplete();
  }

  /**
   * Indicate the end of the communication
   *
   * @param src the source that is ending
   */
  public void finish(int src) {
    gather.finish(src);
  }

  public void close() {
    // deregister from the channel
    gather.close();
  }

  /**
   * Clean the operation, this doesn't close it
   */
  public void reset() {
    gather.reset();
  }
}
