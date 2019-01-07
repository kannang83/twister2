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

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

public class JobParameters {

  private static final Logger LOG = Logger.getLogger(JobParameters.class.getName());

  /**
   * Number of Workers
   */
  private int workers;

  /**
   * Number of datapoints to be generated
   */
  private int numberOfPoints;

  /**
   * Dimension of the datapoints and centroids
   */
  private int dimension;

  /**
   * Datapoints file name
   */
  private String pointsFile;

  /**
   * Filesytem to store the generated data "local" or "hdfs"
   */
  private String fileSystem;

  /**
   * Seed value for the data points random number generation
   */
  private int pointsSeedValue;

  /**
   * Data input value represents data to be generated or else to read it directly from
   * the input file and the options may be "generate" or "read"
   */
  private String dataInput;

  /**
   * File Name
   */
  private String fileName;

  /**
   * Task parallelism value
   */
  private int parallelismValue;

  public JobParameters(int workers) {
    this.workers = workers;
  }

  /**
   * This method is to build the job parameters which is based on the configuration value.
   */
  public static JobParameters build(Config cfg) {

    String fileName = cfg.getStringValue(JobConstants.ARGS_FNAME.trim());
    String pointFile = cfg.getStringValue(JobConstants.ARGS_POINTS.trim());
    String fileSys = cfg.getStringValue(JobConstants.ARGS_FILESYSTEM.trim());
    String dataInput = cfg.getStringValue(JobConstants.ARGS_DATA_INPUT.trim());

    int workers = Integer.parseInt(cfg.getStringValue(JobConstants.ARGS_WORKERS));
    int points = Integer.parseInt(cfg.getStringValue(JobConstants.ARGS_NUMBER_OF_POINTS));
    int pointsVal =
        Integer.parseInt(cfg.getStringValue(JobConstants.ARGS_POINTS_SEED_VALUE));
    int parallelismVal =
        Integer.parseInt(cfg.getStringValue(JobConstants.ARGS_PARALLELISM_VALUE));
    int dim = Integer.parseInt(cfg.getStringValue(JobConstants.ARGS_DIMENSIONS));

    JobParameters jobParameters = new JobParameters(workers);

    jobParameters.workers = workers;
    jobParameters.numberOfPoints = points;

    jobParameters.fileName = fileName;
    jobParameters.pointsFile = pointFile;

    jobParameters.fileSystem = fileSys;
    jobParameters.dataInput = dataInput;

    jobParameters.pointsSeedValue = pointsVal;
    jobParameters.parallelismValue = parallelismVal;
    jobParameters.dimension = dim;

    return jobParameters;
  }

  public int getParallelismValue() {
    return parallelismValue;
  }

  public int getWorkers() {
    return workers;
  }

  public String getFileName() {
    return fileName;
  }

  public String getPointsFile() {
    return pointsFile;
  }

  public String getFileSystem() {
    return fileSystem;
  }

  public int getPointsSeedValue() {
    return pointsSeedValue;
  }

  public String getDataInput() {
    return dataInput;
  }

  public int getNumberOfPoints() {
    return numberOfPoints;
  }

  public int getDimension() {
    return dimension;
  }

  public void setDimension(int dimension) {
    this.dimension = dimension;
  }

  @Override
  public String toString() {

    LOG.fine("workers:" + workers + "\tfilename:" + fileName
        + "\tdatapoints file:" + pointsFile + "\tfilesys:" + fileSystem
        + "\tparallelism:" + parallelismValue);

    return "JobParameters{"
        + ", workers=" + workers
        + '}';
  }
}
