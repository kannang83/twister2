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

package edu.iu.dsc.tws.task.batch;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.TaskContext;

/**
 * The abstract class that represents the Last task of a job. This task will be responsible of
 * outputing the results to various output sources such as files or console.
 * The task takes inputs from another task and outputs to a output source
 */
public abstract class BaseBatchSink implements ISink {
  private static final long serialVersionUID = -254264120110286748L;

  protected TaskContext context;

  protected Config config;

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    this.context = ctx;
    this.config = cfg;
  }
}
