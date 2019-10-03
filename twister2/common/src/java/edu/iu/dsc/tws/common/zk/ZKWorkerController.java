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

package edu.iu.dsc.tws.common.zk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.api.resource.ControllerContext;
import edu.iu.dsc.tws.api.resource.IAllJoinedListener;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.IWorkerFailureListener;
import edu.iu.dsc.tws.api.resource.IWorkerStatusUpdater;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerInfo;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerState;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * we assume each worker is assigned a unique ID outside of this class
 * If a worker joins with an ID that already exists in the group,
 * we assume that the worker is coming from failure. It is the same worker.
 * It is very important that there is no worker ID collusion among different workers in the same job
 *
 * We create a persistent znode for the job.
 * Job znode is created with submitting client.
 * We create an ephemeral znode for each worker under the job znode.
 *
 * Each worker znode keeps two pieces of data:
 *    WorkerInfo object of the worker that created the ephemeral worker znode
 *    status of this worker. Worker status changes during job execution and this field is updated.
 *
 * When workers fail, and their znodes are deleted from ZooKeeper,
 * Their workerInfo objects will be also gone.
 * So, we keep a list of all joined workers in each worker locally.
 * When the worker comes back from a failure,
 * its WorkerInfo is updated in each local worker list.
 *
 * When the last worker leaves the job, It deletes persistent job resources.
 * However, sometimes persistent job znodes may not be deleted.
 * Two workers may leave simultaneously, they both think that they ar ethe last worker.
 * So they don't delete persistent job resources. Or, last worker may fail.
 * Therefore, when a job is submitted, it is important to check whether there is
 * any persistent job znodes from previous sessions in ZooKeeper.
 *
 * we use a barrier to make all workers wait until the last worker arrives at the barrier point
 * we count the number of waiting workers by using a DistributedAtomicInteger
 */

public class ZKWorkerController implements IWorkerController, IWorkerStatusUpdater {
  public static final Logger LOG = Logger.getLogger(ZKWorkerController.class.getName());

  // WorkerInfo object for this worker
  private WorkerInfo workerInfo;

  // number of workers in this job
  private int numberOfWorkers;

  // name of this job
  private String jobName;

  // the client to connect to ZK server
  private CuratorFramework client;

  // the path, znode and children cache objects for this job
  private String jobPath;

  // persistent ephemeral znode for this worker
  private PersistentNode workerZNode;

  // children cache for job znode
  private NodeCache jobZnodeCache;

  // children cache for job znode
  private PathChildrenCache childrenCache;

  // variables related to the barrier
  private DistributedAtomicInteger daiForBarrier;
  private DistributedBarrier barrier;

  // config object
  private Config config;
  private String rootPath;

  // a flag to show whether all workers joined the job
  // Initially it is false. It becomes true when all workers joined the job.
  private boolean allJoined = false;

  // synchronization object for waiting all workers to join the job
  private Object waitObject = new Object();

  // all workers in the job, including completed and failed ones
  private HashMap<WorkerInfo, WorkerState> jobWorkers;

  // Inform worker failure events
  private IWorkerFailureListener failureListener;

  // Inform when all workers joined the job
  private IAllJoinedListener allJoinedListener;

  public ZKWorkerController(Config config,
                            String jobName,
                            int numberOfWorkers,
                            WorkerInfo workerInfo) {
    this.config = config;
    this.rootPath = ZKContext.rootNode(config);

    this.jobName = jobName;
    this.numberOfWorkers = numberOfWorkers;
    this.jobPath = ZKUtils.constructJobPath(rootPath, jobName);
    this.workerInfo = workerInfo;

    jobWorkers = new HashMap<>(numberOfWorkers);
  }

  /**
   * connect to the server
   * append this worker info to the body of job znode
   * create an ephemeral znode for this worker
   * worker znode body has the state of this worker
   * it will be updated as the status of worker changes from STARTING, RUNNING, COMPLETED
   * @return
   */
  public void initialize() throws Exception {

    try {
      String zkServerAddresses = ZKContext.serverAddresses(config);
      int sessionTimeoutMs = FaultToleranceContext.sessionTimeout(config);
      client = ZKUtils.connectToServer(zkServerAddresses, sessionTimeoutMs);

      String barrierPath = ZKUtils.constructBarrierPath(rootPath, jobName);
      barrier = new DistributedBarrier(client, barrierPath);

      String daiPathForBarrier = ZKUtils.constructDaiPathForBarrier(rootPath, jobName);
      daiForBarrier = new DistributedAtomicInteger(client,
          daiPathForBarrier, new ExponentialBackoffRetry(1000, 3));

      createWorkerZnode();

      // cache jobZnode data, and watch that node for changes
      jobZnodeCache = new NodeCache(client, jobPath);
      addJobNodeListener();
      jobZnodeCache.start();

      // We cache children data for parent path.
      // So we will listen for all workers in the job
      childrenCache = new PathChildrenCache(client, jobPath, true);
      addChildrenCacheListener(childrenCache);
      childrenCache.start();

      LOG.info("This worker: " + workerInfo.getWorkerID() + " initialized successfully.");

    } catch (Exception e) {
//      LOG.log(Level.SEVERE, "Exception when initializing ZKWorkerController", e);
      throw e;
    }
  }

  /**
   * add a single IWorkerFailureListener
   * if additional IWorkerFailureListener tried to be added,
   * do not add and return false
   * @param iWorkerFailureListener
   * @return
   */
  public boolean addFailureListener(IWorkerFailureListener iWorkerFailureListener) {
    if (this.failureListener != null) {
      return false;
    }

    this.failureListener = iWorkerFailureListener;
    return true;
  }

  public boolean addAllJoinedListener(IAllJoinedListener iAllJoinedListener) {
    if (allJoinedListener != null) {
      return false;
    }

    allJoinedListener = iAllJoinedListener;
    return true;
  }

  /**
   * Update worker status with new state
   * return true if successful
   *
   * Initially worker status is set at STARTING.
   * Therefore, there is no need to call this method after starting this IWorkerController
   * This method should be called to change worker status to RUNNING, COMPLETED, etc.
   * FAILED status can not be set with this method because the worker already failed.
   * Other workers figure out failed worker when the worker looses its connection to ZK server
   *
   * @param newStatus
   * @return
   */
  @Override
  public boolean updateWorkerStatus(WorkerState newStatus) {

    byte[] workerZnodeBody = ZKUtils.encodeWorkerInfo(workerInfo, newStatus.getNumber());

    try {
      client.setData().forPath(workerZNode.getActualPath(), workerZnodeBody);
      return true;
    } catch (Exception e) {
      LOG.log(Level.SEVERE,
          "Could not update worker status in znode: " + workerInfo.getWorkerID(), e);
      return false;
    }
  }

  @Override
  public WorkerInfo getWorkerInfo() {
    return workerInfo;
  }

  @Override
  public WorkerInfo getWorkerInfoForID(int id) {
    for (WorkerInfo info: jobWorkers.keySet()) {
      if (info.getWorkerID() == id) {
        return info;
      }
    }

    return null;
  }

  @Override
  public WorkerState getWorkerStatusForID(int id) {
    for (Map.Entry<WorkerInfo, WorkerState> entry: jobWorkers.entrySet()) {
      if (entry.getKey().getWorkerID() == id) {
        return entry.getValue();
      }
    }

    return null;
  }

  @Override
  public int getNumberOfWorkers() {
    return numberOfWorkers;
  }

  /**
   * create the znode for this worker
   */
  private void createWorkerZnode() {
    String workerPath = ZKUtils.constructWorkerPath(jobPath, workerInfo.getWorkerID());

    // put WorkerInfo and its state into znode body
    int initialState = WorkerState.STARTING_VALUE;
    byte[] workerZnodeBody = ZKUtils.encodeWorkerInfo(workerInfo, initialState);
    workerZNode = ZKUtils.createPersistentEphemeralZnode(workerPath, workerZnodeBody);
    workerZNode.start();
    try {
      workerZNode.waitForInitialCreate(10000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE,
          "Could not create worker znode: " + workerInfo.getWorkerID(), e);
      throw new RuntimeException("Could not create worker znode: " + workerInfo, e);
    }

    String fullWorkerPath = workerZNode.getActualPath();
    LOG.info("An ephemeral znode is created for this worker: " + fullWorkerPath);
  }

  /**
   * Get current list of workers
   * This list does not have the workers that have failed or already left
   */
  public List<WorkerInfo> getCurrentWorkers() {

    List<WorkerInfo> currentWorkers = new ArrayList<>();

    for (ChildData child: childrenCache.getCurrentData()) {
      int id = ZKUtils.getWorkerIDFromPath(child.getPath());
      WorkerInfo worker = getWorkerInfoForID(id);
      if (worker != null) {
        currentWorkers.add(worker);
      }

    }
    return currentWorkers;
  }

  /**
   * get number of current workers in the job as seen from this worker
   */
  public int getNumberOfCurrentWorkers() {
    return childrenCache.getCurrentData().size();
  }

  /**
   * Get all joined workers including the ones that have already completed or failed
   */
  @Override
  public List<WorkerInfo> getJoinedWorkers() {

    return cloneJobWorkers();
  }

  /**
   * create a mirror of jobWorkers
   * do not create clones of each WorkerInfo, since they are read only
   * @return
   */
  private List<WorkerInfo> cloneJobWorkers() {

    List<WorkerInfo> clone = new LinkedList<>();
    for (WorkerInfo info: jobWorkers.keySet()) {
      clone.add(info);
    }
    return clone;
  }

  /**
   * count joined and live workers
   * @return
   */
  private int countJoinedLiveWorkers() {

    int count = 0;
    for (WorkerState state: jobWorkers.values()) {
      if (state == WorkerState.STARTING
          || state == WorkerState.RUNNING
          || state == WorkerState.RESTARTING) {
        count++;
      }
    }
    return count;
  }

  /**
   * wait to make sure that the number of workers reached the total number of workers in the job
   * return all joined workers in the job including the ones that have already left or failed
   * throws an exception if the timeLimit is reached
   */
  @Override
  public List<WorkerInfo> getAllWorkers() throws TimeoutException {

    // if all workers already joined, return the workers list
    if (allJoined) {
      return cloneJobWorkers();
    }

    // wait until all workers joined or time limit is reached
    long timeLimit = ControllerContext.maxWaitTimeForAllToJoin(config);
    long startTime = System.currentTimeMillis();

    long delay = 0;
    while (delay < timeLimit) {
      synchronized (waitObject) {
        try {
          waitObject.wait(timeLimit - delay);

          // proceeding with notification or timeout
          if (allJoined) {
            return cloneJobWorkers();
          } else {
            throw new TimeoutException("Not all workers joined the job on the given time limit: "
                + timeLimit + "ms.");
          }

        } catch (InterruptedException e) {
          delay = System.currentTimeMillis() - startTime;
        }
      }
    }

    if (allJoined) {
      return cloneJobWorkers();
    } else {
      throw new TimeoutException("Not all workers joined the job on the given time limit: "
          + timeLimit + "ms.");
    }
  }

  /**
   * try to increment the daiForBarrier
   * try 100 times if fails
   * @param tryCount
   * @return
   */
  private boolean incrementBarrierDAI(int tryCount, long timeLimitMilliSec) {

    if (tryCount == 100) {
      return false;
    }

    try {
      AtomicValue<Integer> incremented = daiForBarrier.increment();
      if (incremented.succeeded()) {
        LOG.fine("DistributedAtomicInteger for Barrier increased to: " + incremented.postValue());

        // if this is the last worker to enter, remove the barrier and let all workers be released
        if (incremented.postValue() % numberOfWorkers == 0) {
          barrier.removeBarrier();
          return true;

        // if this is not the last worker, set the barrier and wait
        } else {
          barrier.setBarrier();
          return barrier.waitOnBarrier(timeLimitMilliSec, TimeUnit.MILLISECONDS);
        }

      } else {
        return incrementBarrierDAI(tryCount + 1, timeLimitMilliSec);
      }
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Failed to increment the DistributedAtomicInteger for Barrier. "
          + "Will try again ...", e);
      return incrementBarrierDAI(tryCount + 1, timeLimitMilliSec);
    }
  }

  /**
   * we use a DistributedAtomicInteger to count the number of workers
   * that have reached to the barrier point.
   *
   * Last worker to call this method increases the DistributedAtomicInteger,
   * removes the barrier and lets all previous waiting workers be released.
   *
   * Other workers to call this method increase the DistributedAtomicInteger,
   * enable the barrier by calling setBarrier method and wait.
   *
   * It is enough to call setBarrier method by only the first worker,
   * however, it does not harm calling by many workers.
   *
   * If we let only the first worker to set the barrier with setBarrier method,
   * then, the second worker may call this method after the dai is increased
   * but before the setBarrier method is called. To prevent this,
   * we may need to use a distributed InterProcessMutex.
   * So, instead of using a distributed InterProcessMutex, we call this method many times.
   *
   * DistributedAtomicInteger always increases.
   * We check whether it is a multiple of numberOfWorkers in a job
   * If so, all workers have reached the barrier.
   *
   * This method may be called many times during a computation.
   *
   * if timeout is reached, throws TimeoutException.
   * @return
   */
  @Override
  public void waitOnBarrier() throws TimeoutException {

    boolean allArrived = incrementBarrierDAI(0, ControllerContext.maxWaitTimeOnBarrier(config));
    if (!allArrived) {
      throw new TimeoutException("All workers have not arrived at the barrier on the time limit: "
          + ControllerContext.maxWaitTimeOnBarrier(config) + "ms.");
    }
  }

  private void addJobNodeListener() {
    NodeCacheListener listener = new NodeCacheListener() {
      @Override
      public void nodeChanged() throws Exception {
        byte[] jobZnodeBodyBytes = client.getData().forPath(jobPath);
        JobAPI.Job job = ZKJobZnodeUtil.decodeJobZnode(jobZnodeBodyBytes);
        LOG.info("NodeChanged. New Number of workers: " + job.getNumberOfWorkers());

        if (numberOfWorkers == job.getNumberOfWorkers()) {
          // if numberOfWorkers in this worker and the one in the job are equal,
          // nothing to be done
          // the job is not scaled up or down

        } else if (numberOfWorkers < job.getNumberOfWorkers()) {
          // this means the job is scaled up
          // update numberOfWorkers
          // unset the flag allJoined
          numberOfWorkers = job.getNumberOfWorkers();
          allJoined = false;
        } else {
          // the job is scaled down
          // update numberOfWorkers
          numberOfWorkers = job.getNumberOfWorkers();
          // remove scaled down workers from worker list
          jobWorkers.keySet().removeIf(wInfo -> wInfo.getWorkerID() >= numberOfWorkers);
        }
      }
    };

    jobZnodeCache.getListenable().addListener(listener);
  }

  private void addChildrenCacheListener(PathChildrenCache cache) {
    PathChildrenCacheListener listener = new PathChildrenCacheListener() {

      public void childEvent(CuratorFramework clientOfEvent, PathChildrenCacheEvent event) {

        switch (event.getType()) {
          case CHILD_ADDED:
            childZnodeAdded(event);
            break;

          case CHILD_UPDATED:
            childZnodeUpdated(event);
            break;

          case CHILD_REMOVED:
            childZnodeRemoved(event);
            break;

          default:
            // nothing to do
        }
      }
    };
    cache.getListenable().addListener(listener);
  }

  /**
   * when a new znode added to this job znode,
   * take necessary actions
   * @param event
   */
  private void childZnodeAdded(PathChildrenCacheEvent event) {

    Pair<WorkerInfo, WorkerState> pair = ZKUtils.decodeWorkerInfo(event.getData().getData());
    int newWorkerID = pair.getKey().getWorkerID();

    if (newWorkerID >= numberOfWorkers) {
      LOG.severe(String.format(
          "A worker joined but its workerID[%s] is higher than numberOfWorkers[%s]. "
              + "Not adding this worker to the list of workers in the job.",
          newWorkerID, numberOfWorkers));
      return;
    }

    // if there is an existing WorkerInfo for newly added worker
    // and its previous status is FAILED, it means that this worker is coming from failure
    // Inform the failure listener
    WorkerInfo existingWorkerInfo = getWorkerInfoForID(newWorkerID);
    if (existingWorkerInfo != null
        && getWorkerStatusForID(newWorkerID) == WorkerState.FAILED
        && failureListener != null) {

      jobWorkers.put(pair.getKey(), pair.getValue());
      failureListener.failedWorkerRejoined(pair.getKey());
    } else {

      jobWorkers.put(pair.getKey(), pair.getValue());
    }

    // if currently all workers exist in the job, let the workers know that all joined
    // we don't check the size of jobWorkers,
    // because some workers may have joined and failed.
    // This shows currently existing workers in the job group
    if (numberOfWorkers == countJoinedLiveWorkers() && !allJoined) {
      allJoined = true;
      synchronized (waitObject) {
        waitObject.notify();
      }

      // inform the allJoinedListener
      if (allJoinedListener != null) {
        allJoinedListener.allWorkersJoined(getJoinedWorkers());
      }
    }
    LOG.info(String.format("Worker[%s] added: ", newWorkerID));
  }

  /**
   * when a child znode content is updated,
   * take necessary actions
   * @param event
   */
  private void childZnodeUpdated(PathChildrenCacheEvent event) {
    Pair<WorkerInfo, WorkerState> pair = ZKUtils.decodeWorkerInfo(event.getData().getData());

    // update the worker state in the map
    jobWorkers.put(pair.getKey(), pair.getValue());

    LOG.info(String.format("Worker[%s] znode updated. New state: %s ",
        pair.getKey().getWorkerID(), pair.getValue()));

  }

  /**
   * when a znode is removed from this job znode,
   * take necessary actions
   * @param event
   */
  private void childZnodeRemoved(PathChildrenCacheEvent event) {

    // need to distinguish between completed, scaled down and failed workers
    // if a worker completed before, it has left the job by completion
    // if the workerID of removed worker is higher than the number of workers in the job,
    // it means that is a scaled down worker.
    // otherwise, the worker failed. We inform the failureListener.
    int removedWorkerID = ZKUtils.getWorkerIDFromPath(event.getData().getPath());

    // this is the scaled down worker
    if (removedWorkerID >= numberOfWorkers) {

      LOG.info(String.format("Scaled down worker[%s] removed: ", removedWorkerID));

    // removed event received for completed worker
    } else if (getWorkerStatusForID(removedWorkerID) == WorkerState.COMPLETED) {

      LOG.info(String.format("Worker[%s] completed: ", removedWorkerID));

    // worker failed
    } else {
      LOG.info(String.format("Worker[%s] failed: ", removedWorkerID));
      if (failureListener != null) {
        // first change worker state into failed in local list
        WorkerInfo failedWorker = getWorkerInfoForID(removedWorkerID);
        jobWorkers.put(failedWorker, WorkerState.FAILED);
        // inform the listener
        failureListener.workerFailed(failedWorker);
      }
    }
  }

  /**
   * close the children cache
   * close persistent node for this worker
   * close the connection
   *
   * if this is the last worker to complete, delete all relevant znode for this job
   */
  public void close() {
    if (client != null) {
      try {
        jobZnodeCache.close();
        int noOfChildren =  childrenCache.getCurrentData().size();
        workerZNode.close();
        CloseableUtils.closeQuietly(childrenCache);
        // if this is the last worker, delete znodes for the job
        if (noOfChildren == 1) {
          LOG.log(Level.INFO, "This is the last worker to finish. Deleting the job znodes.");
          ZKJobZnodeUtil.deleteJobZNodes(client, ZKContext.rootNode(config), jobName);
        }
        CloseableUtils.closeQuietly(client);
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Exception when closing", e);
      }
    }
  }

}
