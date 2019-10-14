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


package edu.iu.dsc.tws.tset.sets.batch;

import java.util.Collections;
import java.util.Comparator;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.TaskPartitioner;
import edu.iu.dsc.tws.api.compute.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTupleTSet;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.fn.MapCompute;
import edu.iu.dsc.tws.tset.fn.MapIterCompute;
import edu.iu.dsc.tws.tset.links.batch.JoinTLink;
import edu.iu.dsc.tws.tset.links.batch.KeyedGatherTLink;
import edu.iu.dsc.tws.tset.links.batch.KeyedPartitionTLink;
import edu.iu.dsc.tws.tset.links.batch.KeyedReduceTLink;
import edu.iu.dsc.tws.tset.ops.MapToTupleIterOp;
import edu.iu.dsc.tws.tset.ops.MapToTupleOp;
import edu.iu.dsc.tws.tset.sets.BaseTSet;

/**
 * Attaches a key to the oncoming data.
 *
 * @param <K> key type
 * @param <V> data (value) type
 */
public class KeyedTSet<K, V> extends BaseTSet<V> implements BatchTupleTSet<K, V> {
  private TFunction<Tuple<K, V>, ?> mapToTupleFunc;

  public KeyedTSet(BatchTSetEnvironment tSetEnv, MapCompute<Tuple<K, V>, ?> mapFunc,
                   int parallelism) {
    super(tSetEnv, "keyed", parallelism);
    this.mapToTupleFunc = mapFunc;
  }

  public KeyedTSet(BatchTSetEnvironment tSetEnv, MapIterCompute<Tuple<K, V>, ?> mapFunc,
                   int parallelism) {
    super(tSetEnv, "keyed", parallelism);
    this.mapToTupleFunc = mapFunc;
  }

  // since keyed tset is the base impl for BatchTupleTSet, it needs to override the env getter
  @Override
  public BatchTSetEnvironment getTSetEnv() {
    return (BatchTSetEnvironment) super.getTSetEnv();
  }

  @Override
  public KeyedReduceTLink<K, V> keyedReduce(ReduceFunc<V> reduceFn) {
    KeyedReduceTLink<K, V> reduce = new KeyedReduceTLink<>(getTSetEnv(), reduceFn,
        getParallelism());
    addChildToGraph(reduce);
    return reduce;
  }

  @Override
  public KeyedPartitionTLink<K, V> keyedPartition(PartitionFunc<K> partitionFn) {
    KeyedPartitionTLink<K, V> partition = new KeyedPartitionTLink<>(getTSetEnv(), partitionFn,
        getParallelism());
    addChildToGraph(partition);
    return partition;
  }

  @Override
  public KeyedGatherTLink<K, V> keyedGather() {
    KeyedGatherTLink<K, V> gather = new KeyedGatherTLink<>(getTSetEnv(), getParallelism());
    addChildToGraph(gather);
    return gather;
  }

  @Override
  public <VR> JoinTLink<K, V, VR> join(BatchTupleTSet<K, VR> rightTSet,
                                       CommunicationContext.JoinType type,
                                       Comparator<K> keyComparator,
                                       TaskPartitioner<K> partitioner) {
    JoinTLink<K, V, VR> join;
    if (partitioner != null) {
      join = new JoinTLink<>(getTSetEnv(), type, keyComparator, partitioner, this, rightTSet);
    } else {
      join = new JoinTLink<>(getTSetEnv(), type, keyComparator, this, rightTSet);
    }

    addChildToGraph(join);

    // add the right tset connection
    getTSetEnv().getGraph().connectTSets(rightTSet, join);

    return join;
  }

  @Override
  public <VR> JoinTLink<K, V, VR> join(BatchTupleTSet<K, VR> rightTSet,
                                       CommunicationContext.JoinType type,
                                       Comparator<K> keyComparator) {
    return join(rightTSet, type, keyComparator, null);
  }

  @Override
  public KeyedTSet<K, V> setName(String n) {
    rename(n);
    return this;
  }

  @Override
  public ICompute getINode() {

    if (mapToTupleFunc instanceof MapCompute) {
      return new MapToTupleOp<>((MapCompute<Tuple<K, V>, ?>) mapToTupleFunc, this,
          Collections.emptySet());
    } else if (mapToTupleFunc instanceof MapIterCompute) {
      return new MapToTupleIterOp<>((MapIterCompute<Tuple<K, V>, ?>) mapToTupleFunc, this,
          Collections.emptySet());
    }

    throw new RuntimeException("Unknown map function passed to keyed tset" + mapToTupleFunc);
  }
}
