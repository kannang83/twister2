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

package edu.iu.dsc.tws.api.compute.executor;

public interface IExecution {
  /**
   * Wait for execution to complete
   * @return if execution is success
   */
  boolean waitForCompletion();

  /**
   * Progress the computation
   * @return false if the execution is completed or finished
   */
  boolean progress();

  /**
   * Cleanup the execution
   */
  void close();

  /**
   * Stop a particular execution
   */
  void stop();
}