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
package edu.iu.dsc.tws.rsched.schedulers.standalone;

import java.io.File;
import java.nio.CharBuffer;
import java.nio.IntBuffer;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.logging.LoggingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.nomad.NomadContext;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

import mpi.MPI;
import mpi.MPIException;

/**
 * This is the base process started by the resource scheduler. This process will lanch the container
 * code and it will eventually will load the tasks.
 */
public final class MPIWorker {
  private static final Logger LOG = Logger.getLogger(MPIWorker.class.getName());

  private MPIWorker() {
  }

  public static void main(String[] args) {
    Options cmdOptions = null;
    try {
      MPI.Init(args);

      int rank = MPI.COMM_WORLD.getRank();
      int size = MPI.COMM_WORLD.getSize();

      cmdOptions = setupOptions();
      CommandLineParser parser = new DefaultParser();
      // parse the help options first.
      CommandLine cmd = parser.parse(cmdOptions, args);

      // load the configuration
      // we are loading the configuration for all the components
      Config config = loadConfigurations(cmd, rank);
      // normal worker
      LOG.log(Level.FINE, "A worker process is starting...");
      worker(config, rank);
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Failed the MPI process", e);
      throw new RuntimeException(e);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("SubmitterMain", cmdOptions);
      throw new RuntimeException("Error parsing command line options: ", e);
    } catch (Throwable t) {
      String msg = "Un-expected error";
      LOG.log(Level.SEVERE, msg, t);
      throw new RuntimeException(msg);
    } finally {
      try {
        MPI.Finalize();
      } catch (MPIException ignore) {
      }
    }
  }

  /**
   * Setup the command line options for the MPI process
   * @return cli options
   */
  private static Options setupOptions() {
    Options options = new Options();

    Option containerClass = Option.builder("c")
        .desc("The class name of the container to launch")
        .longOpt("container_class")
        .hasArgs()
        .argName("container class")
        .required()
        .build();

    Option configDirectory = Option.builder("d")
        .desc("The class name of the container to launch")
        .longOpt("config_dir")
        .hasArgs()
        .argName("configuration directory")
        .required()
        .build();

    Option twister2Home = Option.builder("t")
        .desc("The class name of the container to launch")
        .longOpt("twister2_home")
        .hasArgs()
        .argName("twister2 home")
        .required()
        .build();

    Option clusterType = Option.builder("n")
        .desc("The clustr type")
        .longOpt("cluster_type")
        .hasArgs()
        .argName("cluster type")
        .required()
        .build();

    Option jobName = Option.builder("j")
        .desc("Job name")
        .longOpt("job_name")
        .hasArgs()
        .argName("job name")
        .required()
        .build();
    options.addOption(twister2Home);
    options.addOption(containerClass);
    options.addOption(configDirectory);
    options.addOption(clusterType);
    options.addOption(jobName);

    return options;
  }

  private static Config loadConfigurations(CommandLine cmd, int id) {
    String twister2Home = cmd.getOptionValue("twister2_home");
    String container = cmd.getOptionValue("container_class");
    String configDir = cmd.getOptionValue("config_dir");
    String clusterType = cmd.getOptionValue("cluster_type");
    String jobName = cmd.getOptionValue("job_name");

    LOG.log(Level.FINE, String.format("Initializing process with "
        + "twister_home: %s container_class: %s config_dir: %s cluster_type: %s",
        twister2Home, container, configDir, clusterType));

    Config config = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterType);

    Config workerConfig = Config.newBuilder().putAll(config).
        put(MPIContext.TWISTER2_HOME.getKey(), twister2Home).
        put(MPIContext.WORKER_CLASS, container).
        put(MPIContext.TWISTER2_CONTAINER_ID, id).
        put(MPIContext.TWISTER2_CLUSTER_TYPE, clusterType).build();

    String jobDescFile = JobUtils.getJobDescriptionFilePath(jobName, workerConfig);
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFile);

    Config updatedConfig = JobUtils.overrideConfigs(job, config);

    updatedConfig = Config.newBuilder().putAll(updatedConfig).
        put(MPIContext.TWISTER2_HOME.getKey(), twister2Home).
        put(MPIContext.WORKER_CLASS, container).
        put(MPIContext.TWISTER2_CONTAINER_ID, id).
        put(MPIContext.JOB_NAME, jobName).
        put(MPIContext.TWISTER2_CLUSTER_TYPE, clusterType).build();
    return updatedConfig;
  }

  private static void master(Config config, int rank) {
    // lets do a barrier here so everyone is synchronized at the start
    // lets create the resource plan
    createResourcePlan(config);
  }

  private static void worker(Config config, int rank) {
    try {
      String twister2Home = Context.twister2Home(config);
      // initialize the logger
      initLogger(config, rank, twister2Home);

      // lets create the resource plan
      Map<Integer, String> processNames = createResourcePlan(config);
      // now create the worker
      IWorkerController controller = new MPIWorkerController(MPI.COMM_WORLD.getRank(),
          processNames);

      String workerClass = MPIContext.workerClass(config);
      try {
        Object object = ReflectionUtils.newInstance(workerClass);
        if (object instanceof IWorker) {
          IWorker container = (IWorker) object;
          // now initialize the container
          container.execute(config, rank, controller, null, null);
        } else {
          throw new RuntimeException("Cannot instantiate class: " + object.getClass());
        }
        LOG.log(Level.FINE, "loaded worker class: " + workerClass);
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        LOG.log(Level.SEVERE, String.format("failed to load the worker class %s",
            workerClass), e);
        throw new RuntimeException(e);
      }

      // lets do a barrier here so everyone is synchronized at the end

      MPI.COMM_WORLD.barrier();
      LOG.log(Level.FINE, String.format("Worker %d: the cluster is ready...", rank));
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Failed to synchronize the workers at the start");
      throw new RuntimeException(e);
    }
  }

  /**
   * create a AllocatedResources
   * @param config configuration
   * @return  a map of rank to hostname
   */
  public static Map<Integer, String> createResourcePlan(Config config) {
    try {
      String processName = MPI.getProcessorName();
      char[] processNameChars = new char[processName.length()];
      int length = processNameChars.length;
      processName.getChars(0, length, processNameChars, 0);

      IntBuffer countSend = MPI.newIntBuffer(1);
      int worldSize = MPI.COMM_WORLD.getSize();
      IntBuffer countReceive = MPI.newIntBuffer(worldSize);
      // now calculate the total number of characters
      countSend.put(length);
      MPI.COMM_WORLD.allGather(countSend, 1, MPI.INT, countReceive,
          1, MPI.INT);

      int[] receiveSizes = new int[worldSize];
      int[] displacements = new int[worldSize];
      int sum = 0;
      for (int i = 0; i < worldSize; i++) {
        receiveSizes[i] = countReceive.get(i);
        displacements[i] = sum;
        sum += receiveSizes[i];
      }
      // first we need to send the expected number of characters
      //  MPI.COMM_WORLD.allGather(countSend, 1, MPI.INT, countReceive, worldSize, MPI.INT);

      // now we need to send this to all the nodes
      CharBuffer sendBuffer = MPI.newCharBuffer(length);
      CharBuffer receiveBuffer = MPI.newCharBuffer(sum);
      sendBuffer.append(processName);

      // now lets receive the process names of each rank
      MPI.COMM_WORLD.allGatherv(sendBuffer, length, MPI.CHAR, receiveBuffer,
          receiveSizes, displacements, MPI.CHAR);

      Map<Integer, String> processNames = new HashMap<>();

      for (int i = 0; i < receiveSizes.length; i++) {
        char[] c = new char[receiveSizes[i]];
        receiveBuffer.get(c);
        processNames.put(i, new String(c));
        LOG.log(Level.FINE, String.format("Process %d name: %s", i, processNames.get(i)));
      }

      return processNames;
    } catch (MPIException e) {
      throw new RuntimeException("Failed to communicate", e);
    }
  }

  /**
   * Initialize the loggers to log into the task local directory
   * @param cfg the configuration
   * @param workerID worker id
   */
  private static void initLogger(Config cfg, int workerID, String logDirectory) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    // set logging level
    LoggingHelper.setLogLevel(LoggingContext.loggingLevel(cfg));

    String persistentJobDir;
    String jobWorkingDirectory = NomadContext.workingDirectory(cfg);
    String jobName = NomadContext.jobName(cfg);
    if (NomadContext.getLoggingSandbox(cfg)) {
      persistentJobDir = Paths.get(jobWorkingDirectory, jobName).toString();
    } else {
      persistentJobDir = logDirectory;
    }

    // if no persistent volume requested, return
    if (persistentJobDir == null) {
      return;
    }
    String logDir = persistentJobDir + "/logs/worker-" + workerID;
    File directory = new File(logDir);
    if (!directory.exists()) {
      if (!directory.mkdirs()) {
        throw new RuntimeException("Failed to create log directory: " + logDir);
      }
    }
    LoggingHelper.setupLogging(cfg, logDir, "worker-" + workerID);
  }
}
