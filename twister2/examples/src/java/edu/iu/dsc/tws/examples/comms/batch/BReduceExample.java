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
package edu.iu.dsc.tws.examples.comms.batch;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.batch.BReduce;
import edu.iu.dsc.tws.comms.op.functions.reduction.ReduceOperationFunction;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.comms.BenchWorker;
import edu.iu.dsc.tws.examples.verification.ExperimentVerification;
import edu.iu.dsc.tws.examples.verification.VerificationException;
import edu.iu.dsc.tws.executor.core.OperationNames;

public class BReduceExample extends BenchWorker {
  private static final Logger LOG = Logger.getLogger(BReduceExample.class.getName());

  private BReduce reduce;

  private boolean reduceDone;

  @Override
  protected void execute() {
    TaskPlan taskPlan = Utils.createStageTaskPlan(config, workerId,
        jobParameters.getTaskStages(), workerList);

    Set<Integer> sources = new HashSet<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    int target = noOfSourceTasks;
    // create the communication
    reduce = new BReduce(communicator, taskPlan, sources, target,
        new ReduceOperationFunction(Op.SUM, MessageType.INTEGER), new FinalSingularReceiver(),
        MessageType.INTEGER);

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);
    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }

    if (!taskPlan.getChannelsOfExecutor(workerId).contains(target)) {
      reduceDone = true;
    }

    LOG.log(Level.INFO, String.format("%d Sources %s target %d this %s",
        workerId, sources, target, tasksOfExecutor));
    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  protected void progressCommunication() {
    reduce.progress();
  }

  @Override
  protected boolean sendMessages(int task, Object data, int flag) {
    while (!reduce.reduce(task, data, flag)) {
      // lets wait a litte and try again
      reduce.progress();
    }
    return true;
  }

  @Override
  protected boolean isDone() {
    return reduceDone && sourcesDone && !reduce.hasPending();
  }

  public class FinalSingularReceiver implements SingularReceiver {
    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
    }

    @Override
    public boolean receive(int target, Object object) {
      experimentData.setOutput(object);
      LOG.info("Reduced value : " + Arrays.toString((int[]) object));
      reduceDone = true;
      try {
        verify();
      } catch (VerificationException e) {
        LOG.info("Exception Message : " + e.getMessage());
      }
      return true;
    }
  }

  public void verify() throws VerificationException {
    boolean doVerify = jobParameters.isDoVerify();
    boolean isVerified = false;
    if (doVerify) {
      LOG.info("Verifying results ...");
      ExperimentVerification experimentVerification
          = new ExperimentVerification(experimentData, OperationNames.REDUCE);
      isVerified = experimentVerification.isVerified();
      if (isVerified) {
        LOG.info("Results generated from the experiment are verified.");
      } else {
        throw new VerificationException("Results do not match");
      }
    }
  }

  @Override
  protected void finishCommunication(int src) {
    reduce.finish(src);
  }
}
