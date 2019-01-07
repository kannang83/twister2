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
package edu.iu.dsc.tws.examples.batch.taskgraph;

import java.util.Arrays;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

/**
 * This class acts as an interface for reading the input datapoints and centroid values from
 * the local file system or from the distributed file system (HDFS).
 */
public class FileReader {

  private static final Logger LOG = Logger.getLogger(FileReader.class.getName());

  private Config config;
  private String fileSystem;

  public FileReader(Config cfg, String fileSys) {
    this.config = cfg;
    this.fileSystem = fileSys;
  }

  /**
   * It reads the datapoints from the corresponding file system and store the data in a two
   * -dimensional array for the later processing.
   */
  public double[] readDataPoints(String fName, int dimension) {

    double[] dataPoints = null;

    if ("local".equals(fileSystem)) {
      MeansLocalFileReader meansLocalFileReader = new MeansLocalFileReader();
      dataPoints = meansLocalFileReader.readDataPoints(fName);
    }
    LOG.fine("%%%% Datapoints:" + Arrays.toString(dataPoints));
    return dataPoints;
  }
}

