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
package edu.iu.dsc.tws.tset.fn.impl;

import java.io.IOException;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import edu.iu.dsc.tws.data.api.formatters.LocalCSVInputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;

public class CSVBasedSourceFunction<T> extends BaseSourceFunc<T> {

  private static final Logger LOG = Logger.getLogger(CSVBasedSourceFunction.class.getName());

  private DataSource<T, FileInputSplit<T>> dataSource;
  private InputSplit<T> dataSplit;
  private TSetContext ctx;

  private String datainputDirectory;
  private int dataSize;

  public CSVBasedSourceFunction(String dataInputdirectory, int datasize) {
    this.datainputDirectory = dataInputdirectory;
    this.dataSize = datasize;
  }

  @Override
  public void prepare(TSetContext context) {
    super.prepare(context);
    this.ctx = context;
    this.dataSource = new DataSource(context.getConfig(), new LocalCSVInputPartitioner(
        new Path(datainputDirectory), context.getParallelism(), dataSize, context.getConfig()),
        context.getParallelism());
    this.dataSplit = this.dataSource.getNextSplit(context.getIndex());
    LOG.info("%%%% task index value:" + context.getIndex() + "\t" + this.dataSource);
  }

  @Override
  public boolean hasNext() {
    try {
      if (dataSplit == null || dataSplit.reachedEnd()) {
        dataSplit = dataSource.getNextSplit(getTSetContext().getIndex());
      }
      return dataSplit != null && !dataSplit.reachedEnd();
    } catch (IOException e) {
      throw new RuntimeException("Unable read data split!");
    }
  }

  @Override
  public T next() {
    try {
//      Object object = dataSplit.nextRecord(null);
//      LOG.info("next object is:" + object);
//      return (T) object;
      return dataSplit.nextRecord(null);
    } catch (IOException e) {
      throw new RuntimeException("Unable read data split!");
    }
  }
}
