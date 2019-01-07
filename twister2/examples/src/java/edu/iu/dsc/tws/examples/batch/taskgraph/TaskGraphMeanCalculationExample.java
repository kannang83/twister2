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
package edu.iu.dsc.tws.examples.batch.taskgraph;

import java.util.HashMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataSet;
import edu.iu.dsc.tws.dataset.Partition;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class TaskGraphMeanCalculationExample extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(TaskGraphMeanCalculationExample.class.getName());
  private JobParameters jobParameters;
  private static final long serialVersionUID = -5190777711234234L;

  @Override
  public void execute() {
    this.jobParameters = JobParameters.build(config);
    int parallelismValue = jobParameters.getParallelismValue();
    int workers = jobParameters.getWorkers();
    int noOfPoints = jobParameters.getNumberOfPoints();
    int dataSeedValue = jobParameters.getPointsSeedValue();
    int dimension = jobParameters.getDimension();
    String dataPointsFile = jobParameters.getPointsFile();
    String fileSystem = jobParameters.getFileSystem();
    String inputData = jobParameters.getDataInput();

    FileReader fileReader = new FileReader(config, fileSystem);
    if ("generate".equals(inputData)) {
      DataGenerator.generateDataPointsFile(
          dataPointsFile + workerId, noOfPoints, dimension, dataSeedValue, config,
          fileSystem);
    }
    double[] dataPoint = fileReader.readDataPoints(dataPointsFile + workerId, dimension);
    MeanSourceTask meanSourceTask = new MeanSourceTask();
    MeanReduceTask meanReduceTask = new MeanReduceTask();

    TaskGraphBuilder graphBuilder = TaskGraphBuilder.newBuilder(config);
    graphBuilder.addSource("source", meanSourceTask, parallelismValue);
    ComputeConnection computeConnection = graphBuilder.addSink("sink", meanReduceTask,
        parallelismValue);
    computeConnection.allreduce("source", "all-reduce",
        new MeanValueAggregator(), DataType.OBJECT);
    graphBuilder.setMode(OperationMode.BATCH);
    DataFlowTaskGraph graph = graphBuilder.build();

    //Store datapoints and meanvalue
    DataSet<Object> datapoints = new DataSet<>(0);
    datapoints.addPartition(0, dataPoint);
    ExecutionPlan plan = taskExecutor.plan(graph);

    taskExecutor.addInput(graph, plan, "source", "points", datapoints);
    taskExecutor.execute(graph, plan);
    DataSet<Object> dataSet = taskExecutor.getOutput(graph, plan, "sink");
    Set<Object> values = dataSet.getData();
    double meanValue = 0.0;
    for (Object value : values) {
      meanValue = (double) value;
    }
    LOG.log(Level.INFO, "%%% Calculated Mean Value: %%%" + meanValue);
  }

  private static class MeanSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -354264120110286748L;
    private DataSet<Object> input;
    private double[] datapoints = null;
    private MeanCalculator meanCalculator = null;

    @Override
    public void execute() {
      int startIndex = context.taskIndex() * datapoints.length / context.getParallelism();
      int endIndex = startIndex + datapoints.length / context.getParallelism();
      meanCalculator = new MeanCalculator(datapoints, context.taskIndex(), startIndex, endIndex);
      double meanValue = meanCalculator.calculate();
      context.writeEnd("all-reduce", meanValue);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(String name, DataSet<Object> data) {
      LOG.log(Level.INFO, "Received input: " + name);
      input = data;
      int id = input.getId();
      if (id == 0) {
        Set<Object> dataPoints = input.getData();
        this.datapoints = (double[]) dataPoints.iterator().next();
      }
    }
  }

  private static class MeanReduceTask extends BaseSink implements Collector<Object> {
    private static final long serialVersionUID = -4190777711234234L;
    private double meanValue;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Context Values: " + context.getWorkerId() + ":" + context.taskId());
      meanValue = (double) message.getContent();
      return true;
    }

    @Override
    public Partition<Object> get() {
      return new Partition<>(context.taskIndex(), meanValue);
    }
  }

  public class MeanValueAggregator implements IFunction {
    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public Object onMessage(Object object1, Object object2) throws ArrayIndexOutOfBoundsException {
      double meanValue = (double) object1 + (double) object2;
      LOG.log(Level.FINE, "Final Mean Value:" + meanValue);
      return meanValue;
    }
  }

  public static void main(String[] args) throws ParseException {

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    Options options = new Options();
    options.addOption(JobConstants.ARGS_WORKERS, true, "workers");

    options.addOption(JobConstants.ARGS_FNAME, true, "fname");
    options.addOption(JobConstants.ARGS_NUMBER_OF_POINTS, true, "points");
    options.addOption(JobConstants.ARGS_POINTS, true, "pointsfile");
    options.addOption(JobConstants.ARGS_FILESYSTEM, true, "filesystem");
    options.addOption(JobConstants.ARGS_DIMENSIONS, true, "dim");

    options.addOption(JobConstants.ARGS_POINTS_SEED_VALUE, true, "pseedvalue");
    options.addOption(JobConstants.ARGS_DATA_INPUT, true, "generate");
    options.addOption(JobConstants.ARGS_PARALLELISM_VALUE, true, "4");

    @SuppressWarnings("deprecation")
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine = commandLineParser.parse(options, args);

    String fileName = commandLine.getOptionValue(JobConstants.ARGS_FNAME);
    String datapointsFile = commandLine.getOptionValue(JobConstants.ARGS_POINTS);
    String fileSystem = commandLine.getOptionValue(JobConstants.ARGS_FILESYSTEM);
    String dataInput = commandLine.getOptionValue(JobConstants.ARGS_DATA_INPUT);

    int numberOfPoints = Integer.parseInt(commandLine.getOptionValue(
        JobConstants.ARGS_NUMBER_OF_POINTS));
    int workers = Integer.parseInt(commandLine.getOptionValue(JobConstants.ARGS_WORKERS));
    int pSeedValue =
        Integer.parseInt(commandLine.getOptionValue(JobConstants.ARGS_POINTS_SEED_VALUE));
    int parallelismValue =
        Integer.parseInt(commandLine.getOptionValue(JobConstants.ARGS_PARALLELISM_VALUE));
    int dim = Integer.parseInt(commandLine.getOptionValue(JobConstants.ARGS_DIMENSIONS));

    LOG.fine("workers:" + workers + "\tfilename:" + fileName
        + "\tnumber of datapoints:" + numberOfPoints + "\tdatapoints file:" + datapointsFile
        + "\tfilesys:" + fileSystem + "\tparllelism:" + parallelismValue);

    configurations.put(JobConstants.ARGS_FNAME, fileName);
    configurations.put(JobConstants.ARGS_POINTS, datapointsFile);
    configurations.put(JobConstants.ARGS_FILESYSTEM, fileSystem);
    configurations.put(JobConstants.ARGS_DATA_INPUT, dataInput);

    configurations.put(JobConstants.ARGS_DIMENSIONS, Integer.toString(dim));
    configurations.put(JobConstants.ARGS_NUMBER_OF_POINTS, Integer.toString(numberOfPoints));
    configurations.put(JobConstants.ARGS_WORKERS, Integer.toString(workers));
    configurations.put(JobConstants.ARGS_POINTS_SEED_VALUE, Integer.toString(pSeedValue));
    configurations.put(JobConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallelismValue));

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("TaskGraph");
    jobBuilder.setWorkerClass(TaskGraphMeanCalculationExample.class.getName());
    jobBuilder.addComputeResource(2, 512, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}

