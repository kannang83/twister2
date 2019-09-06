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
package edu.iu.dsc.tws.examples.internal.jobmaster;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.master.worker.JMWorkerController;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.NodeInfoUtils;
import edu.iu.dsc.tws.proto.utils.WorkerInfoUtils;

public final class JobMasterClientExample {
  private static final Logger LOG = Logger.getLogger(JobMasterClientExample.class.getName());

  private JobMasterClientExample() {
  }

  /**
   * a test class to run JMWorkerAgent
   * First, a JobMaster instance should be started on a machine
   * This client should connect to that server
   * <p>
   * It reads config files from conf/kubernetes directory
   * It uses the first ComputeResource in that config file as the ComputeResource of this worker
   * Number of workers is the number of workers in the first ComputeResource
   * <p>
   * When all workers joined, they get the full worker list
   * Then, each worker sends a barrier message
   * Then, each worker sends a completed message and closes
   */
  public static void main(String[] args) {

    // we assume that the twister2Home is the current directory
    String configDir = "conf/kubernetes/";
    String twister2Home = Paths.get("").toAbsolutePath().toString();
    Config config = ConfigLoader.loadConfig(twister2Home, "conf", "kubernetes");
    config = updateConfig(config);
    LOG.info("Loaded: " + config.size() + " parameters from configuration directory: " + configDir);

    Twister2Job twister2Job = Twister2Job.loadTwister2Job(config, null);
    JobAPI.Job job = twister2Job.serialize();

    simulateClient(config, job);
  }

  /**
   * a method to simulate JMWorkerAgent running in workers
   */
  public static void simulateClient(Config config, JobAPI.Job job) {

    String workerIP = JMWorkerController.convertStringToIP("localhost").getHostAddress();
    int workerPort = 10000 + (int) (Math.random() * 10000);

    JobMasterAPI.NodeInfo nodeInfo = NodeInfoUtils.createNodeInfo("node.ip", "rack01", null);

    int workerTempID = 0;
    JobAPI.ComputeResource computeResource = job.getComputeResource(0);
    int numberOfWorkers = computeResource.getInstances() * computeResource.getWorkersPerPod();

    Map<String, Integer> additionalPorts = generateAdditionalPorts(config, workerPort);

    JobMasterAPI.WorkerInfo workerInfo = WorkerInfoUtils.createWorkerInfo(
        workerTempID, workerIP, workerPort, nodeInfo, computeResource, additionalPorts);

    String jobMasterAddress = "localhost";
    int jobMasterPort = JobMasterContext.jobMasterPort(config);
    JMWorkerAgent client = JMWorkerAgent.createJMWorkerAgent(
        config, workerInfo, jobMasterAddress, jobMasterPort, numberOfWorkers);

    client.startThreaded(false);

    IWorkerController workerController = client.getJMWorkerController();

    // wait up to 2sec
    sleeeep((long) (Math.random() * 2000));

    client.sendWorkerRunningMessage();

    List<JobMasterAPI.WorkerInfo> workerList = workerController.getJoinedWorkers();
    LOG.info(WorkerInfoUtils.workerListAsString(workerList));

    try {
      workerList = workerController.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }
    LOG.info(WorkerInfoUtils.workerListAsString(workerList));

    // wait up to 10sec
    sleeeep((long) (Math.random() * 1000000));
    try {
      client.getJMWorkerController().waitOnBarrier();
      LOG.info("All workers reached the barrier. Proceeding.");
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
    }

    // wait up to 3sec
    sleeeep((long) (Math.random() * 3000));

    client.sendWorkerCompletedMessage();

    client.close();

    System.out.println("Client has finished the computation. Client exiting.");
  }

  /**
   * construct a Config object
   */
  public static Config updateConfig(Config config) {
    return Config.newBuilder()
        .putAll(config)
        .put(JobMasterContext.JOB_MASTER_ASSIGNS_WORKER_IDS, true)
        .build();
  }

  /**
   * generate the additional requested ports for this worker
   */
  public static Map<String, Integer> generateAdditionalPorts(Config config, int workerPort) {

    // if no port is requested, return null
    List<String> portNames = SchedulerContext.additionalPorts(config);
    if (portNames == null) {
      return null;
    }

    HashMap<String, Integer> ports = new HashMap<>();
    int i = 1;
    for (String portName : portNames) {
      ports.put(portName, workerPort + i++);
    }

    return ports;
  }

  public static void sleeeep(long duration) {

    LOG.info("Sleeping " + duration + "ms............");

    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void printUsage() {
    LOG.info("Usage:\n"
        + "java JobMasterClientExample numberOfWorkers");
  }

}
