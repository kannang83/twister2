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
package edu.iu.dsc.tws.data.api.formatters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.BlockLocation;
import edu.iu.dsc.tws.api.data.FileStatus;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.InputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

public abstract class CSVInputPartitioner<OT> implements InputPartitioner<OT, FileInputSplit<OT>> {

  private static final Logger LOG = Logger.getLogger(CSVInputPartitioner.class.getName());

  private static final long serialVersionUID = -1L;
  private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;

  protected transient int numSplits;

  protected Path filePath;
  protected Config config;

  private long minSplitSize = 0;
  private boolean enumerateNestedFiles = false;

  public CSVInputPartitioner(Path filePath) {
    this.filePath = filePath;
  }

  public CSVInputPartitioner(Path filePath, Config cfg) {
    this.filePath = filePath;
    this.config = cfg;
  }

  @Override
  public void configure(Config parameters) {
    this.config = parameters;
  }

  /**
   * It create the number of splits based on the task parallelism value.
   *
   * @param minNumSplits Number of minimal input splits, as a hint.
   */
  @Override
  public FileInputSplit<OT>[] createInputSplits(int minNumSplits) throws IOException {
    if (minNumSplits < 1) {
      throw new IllegalArgumentException("Number of input splits has to be at least 1.");
    }

    int curminNumSplits = Math.max(minNumSplits, this.numSplits);

    final Path path = this.filePath;
    final List<FileInputSplit> inputSplits = new ArrayList<>(curminNumSplits);
    List<FileStatus> files = new ArrayList<>();
    long totalLength = 0;

    final FileSystem fs = FileSystemUtils.get(path, config);
    final FileStatus pathFile = fs.getFileStatus(path);

    if (pathFile.isDir()) {
      totalLength += sumFilesInDir(path, files, true);
    } else {
      files.add(pathFile);
      totalLength += pathFile.getLen();
    }

    final long maxSplitSize = totalLength / curminNumSplits
        + (totalLength % curminNumSplits == 0 ? 0 : 1);

    //Generate the splits
    int splitNum = 0;
    for (final FileStatus file : files) {
      final long len = file.getLen();
      final long blockSize = file.getBlockSize();

      final long localminSplitSize;
      if (this.minSplitSize <= blockSize) {
        localminSplitSize = this.minSplitSize;
      } else {
        LOG.log(Level.WARNING, "Minimal split size of " + this.minSplitSize
            + " is larger than the block size of " + blockSize
            + ". Decreasing minimal split size to block size.");
        localminSplitSize = blockSize;
      }

      final long splitSize = Math.max(localminSplitSize, Math.min(maxSplitSize, blockSize));
      final long halfSplit = splitSize >>> 1;
      final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);
      if (len > 0) {
        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
        Arrays.sort(blocks);
        long bytesUnassigned = len;
        long position = 0;

        int blockIndex = 0;
        while (bytesUnassigned > maxBytesForLastSplit) {
          blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
          final FileInputSplit fis = createSplit(splitNum++, file.getPath(), position, splitSize,
              blocks[blockIndex].getHosts());
          inputSplits.add(fis);
          position += splitSize;
          bytesUnassigned -= splitSize;
        }

        if (bytesUnassigned > 0) {
          blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
          final FileInputSplit fis = createSplit(splitNum++, file.getPath(), position,
              bytesUnassigned, blocks[blockIndex].getHosts());
          inputSplits.add(fis);
        }

      } else {
        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
        String[] hosts;
        if (blocks.length > 0) {
          hosts = blocks[0].getHosts();
        } else {
          hosts = new String[0];
        }
        final FileInputSplit fis = createSplit(splitNum++, file.getPath(), 0, 0, hosts);
        inputSplits.add(fis);
      }
    }
    LOG.info("input splits value:" + inputSplits.size() + "\t"
        + Arrays.toString(inputSplits.toArray()));
    return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
  }

  protected abstract FileInputSplit createSplit(int num, Path file, long start,
                                                long length, String[] hosts);

  @Override
  public InputSplitAssigner<OT> getInputSplitAssigner(FileInputSplit<OT>[] inputSplits) {
    return null;
  }

  long sumFilesInDir(Path path, List<FileStatus> files, boolean logExcludedFiles)
      throws IOException {
    final FileSystem fs = FileSystemUtils.get(path);
    long length = 0;
    for (FileStatus file : fs.listFiles(path)) {
      if (file.isDir()) {
        if (acceptFile(file) && enumerateNestedFiles) {
          length += sumFilesInDir(file.getPath(), files, logExcludedFiles);
        } else {
          if (logExcludedFiles) {
            LOG.log(Level.INFO, "Directory " + file.getPath().toString() + " did not pass the "
                + "file-filter and is excluded.");
          }
        }
      } else {
        if (acceptFile(file)) {
          files.add(file);
          length += file.getLen();
        } else {
          if (logExcludedFiles) {
            LOG.log(Level.INFO, "Directory " + file.getPath().toString()
                + " did not pass the file-filter and is excluded.");
          }
        }
      }
    }
    return length;
  }

  private boolean acceptFile(FileStatus fileStatus) {
    final String name = fileStatus.getPath().getName();
    return !name.startsWith("_")
        && !name.startsWith(".");
  }

  int getBlockIndexForPosition(BlockLocation[] blocks, long offset,
                               long halfSplitSize, int startIndex) {
    for (int i = startIndex; i < blocks.length; i++) {
      long blockStart = blocks[i].getOffset();
      long blockEnd = blockStart + blocks[i].getLength();
      if (offset >= blockStart && offset < blockEnd) {
        if (i < blocks.length - 1 && blockEnd - offset < halfSplitSize) {
          return i + 1;
        } else {
          return i;
        }
      }
    }
    throw new IllegalArgumentException("The given offset is not contained in the any block.");
  }
}