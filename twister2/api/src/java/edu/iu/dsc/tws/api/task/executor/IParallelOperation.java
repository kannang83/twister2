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
package edu.iu.dsc.tws.api.task.executor;

import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.api.task.IMessage;

/**
 * Represents a parallel communication operation
 */
public interface IParallelOperation {
  /**
   * Send a message over the operation
   * @param source source
   * @param message the message
   */
  boolean send(int source, IMessage message, int flags);

  /**
   * Register a queue for receiving message
   * @param targetTask
   * @param queue
   */
  void register(int targetTask, BlockingQueue<IMessage> queue);

  /**
   * Register a callback to notify when a sync happens
   * @param targetTask the target
   * @param sync sync
   */
  void registerSync(int targetTask, ISync sync);

  /**
   * Progress the parallel operation
   */
  boolean progress();

  /**
   * Check weather the operation is complete
   * @return true if the operation is complete
   */
  boolean isComplete();

  /**
   * Indicate the end of the operation
   * @param source the source
   */
  default void finish(int source) {
  }

  /**
   * Close the parallel operation
   */
  default void close() {
  }

  /**
   * Refresh the operation to start from beginning
   */
  default void reset() {
  }
}