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
package edu.iu.dsc.tws.tsched.spi.taskschedule;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.HierarchicalTaskGraph;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;

public interface IHierarchicalTaskScheduler {

  void initialize(Config cfg);

  TaskSchedulePlan schedule(HierarchicalTaskGraph hierarchicalTaskGraph, WorkerPlan workerPlan);

  /*List<TaskSchedulePlan> schedule(Config config, HierarchicalTaskGraph hierarchicalTaskGraph,
   WorkerPlan workerPlan);*/
}
