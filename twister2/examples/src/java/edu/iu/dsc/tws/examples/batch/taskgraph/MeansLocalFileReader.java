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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

/**
 * This class acts as an interface for reading the input datapoints and centroid values from
 * the local file system or from the distributed file system (HDFS).
 */
public class MeansLocalFileReader {

  private static final Logger LOG = Logger.getLogger(MeansLocalFileReader.class.getName());

  private Config config;
  private String fileSystem;

  public MeansLocalFileReader() {
  }

  /**
   * It reads the datapoints from the corresponding file system and store the data in a two
   * -dimensional array for the later processing.
   */
  public double[] readDataPoints(String fName) {

    BufferedReader bufferedReader = null;
    File f = new File(fName);
    try {
      bufferedReader = new BufferedReader(new FileReader(f));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    String line = "";
    int value = 0;
    int lengthOfFile = getNumberOfLines(fName);
    double[] dataPoints = new double[lengthOfFile];
    try {
      while ((line = bufferedReader.readLine()) != null) {
        String data = line;
        for (int i = 0; i < lengthOfFile; i++) {
          dataPoints[value] = Double.parseDouble(data.trim());
        }
        value++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        bufferedReader.close();
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
    return dataPoints;
  }


  /**
   * It calculates the number of lines in the file name.
   */
  private int getNumberOfLines(String fileName) {

    int noOfLines = 0;
    try {
      File file = new File(fileName);
      LineNumberReader numberReader = new LineNumberReader(new FileReader(file));
      numberReader.skip(Long.MAX_VALUE);
      noOfLines = numberReader.getLineNumber();
      numberReader.close();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    return noOfLines;
  }

}

