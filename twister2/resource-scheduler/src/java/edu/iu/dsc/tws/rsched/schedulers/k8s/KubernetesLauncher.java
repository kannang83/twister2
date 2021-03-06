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
package edu.iu.dsc.tws.rsched.schedulers.k8s;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.NodeInfoUtils;
import edu.iu.dsc.tws.master.IJobTerminator;
import edu.iu.dsc.tws.master.JobMaster;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.interfaces.ILauncher;
import edu.iu.dsc.tws.rsched.schedulers.k8s.master.JobMasterRequestObject;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

import io.kubernetes.client.models.V1PersistentVolumeClaim;
import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1beta2StatefulSet;

public class KubernetesLauncher implements ILauncher, IJobTerminator {

  private static final Logger LOG = Logger.getLogger(KubernetesLauncher.class.getName());

  private Config config;
  private KubernetesController controller;
  private String namespace;
  private JobSubmissionStatus jobSubmissionStatus;

  public KubernetesLauncher() {
    controller = new KubernetesController();
    jobSubmissionStatus = new JobSubmissionStatus();
  }

  @Override
  public void initialize(Config conf) {
    this.config = conf;
    namespace = KubernetesContext.namespace(config);
    controller.init();
  }

  /**
   * Launch the processes according to the resource plan.
   *
   * @return true if the request is granted
   */
  @Override
  public boolean launch(JobAPI.Job job) {

    if (!configParametersOK(job)) {
      return false;
    }

    RequestObjectBuilder.init(config, job.getJobName());
    JobMasterRequestObject.init(config, job.getJobName());

    String jobName = job.getJobName();

    String jobPackageFile = SchedulerContext.temporaryPackagesPath(config) + "/"
        + SchedulerContext.jobPackageFileName(config);

    File jobFile = new File(jobPackageFile);
    if (!jobFile.exists()) {
      LOG.log(Level.SEVERE, "Can not access job package file: " + jobPackageFile
          + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
      return false;
    }

    long jobFileSize = jobFile.length();

    // start job package transfer threads to watch pods to start
    if (KubernetesContext.clientToPodsUploading(config)) {
      JobPackageTransferThread.startTransferThreads(
          namespace, job, jobPackageFile, KubernetesContext.watchBeforeUploadAttempts(config));
    }

    // initialize the service in Kubernetes master
    boolean servicesCreated = initServices(jobName);
    if (!servicesCreated) {
      clearupWhenSubmissionFails(jobName);
      return false;
    }

    // if persistent volume is requested, create a persistent volume claim
    if (SchedulerContext.persistentVolumeRequested(config)) {
      boolean volumesSetup = initPersistentVolumeClaim(job);
      if (!volumesSetup) {
        clearupWhenSubmissionFails(jobName);
        return false;
      }
    }

    // initialize StatefulSets for this job
    boolean statefulSetInitialized = initStatefulSets(jobName, jobFileSize, job);
    if (!statefulSetInitialized) {
      clearupWhenSubmissionFails(jobName);
      return false;
    }

    if (KubernetesContext.clientToPodsUploading(config)) {
      // transfer the job package to pods, measure the transfer time
      long start = System.currentTimeMillis();
      boolean transferred = JobPackageTransferThread.completeFileTransfers();

      if (transferred) {
        long duration = System.currentTimeMillis() - start;
        LOG.info("Transferring all files took: " + duration + " ms.");
      } else {
        LOG.log(Level.SEVERE, "Transferring the job package to some pods failed."
            + "\nPlease run terminate job to clear up any artifacts from previous jobs, "
            + "or submit the job with a different name."
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");

        clearupWhenSubmissionFails(jobName);
        return false;
      }
    }

    // start the Job Master locally if requested
    if (JobMasterContext.jobMasterRunsInClient(config)) {
      JobMaster jobMaster = null;
      try {
        jobMaster =
            new JobMaster(config, InetAddress.getLocalHost().getHostAddress(), this, jobName);
        jobMaster.addShutdownHook();
        jobMaster.startJobMasterBlocking();
      } catch (UnknownHostException e) {
        LOG.log(Level.SEVERE, "Exception when getting local host address: "
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++", e);
        clearupWhenSubmissionFails(jobName);
        return false;
      }
    }

    return true;
  }

  private boolean initServices(String jobName) {
    // first check whether there are services running with the same name
    String workersServiceName = KubernetesUtils.createServiceName(jobName);
    ArrayList<String> serviceNames = new ArrayList<>();
    serviceNames.add(workersServiceName);

    String jobMasterServiceName = KubernetesUtils.createJobMasterServiceName(jobName);
    if (!JobMasterContext.jobMasterRunsInClient(config)) {
      serviceNames.add(jobMasterServiceName);
    }

    boolean serviceExists = controller.servicesExist(namespace, serviceNames);
    if (serviceExists) {
      LOG.severe("Another job might be running. "
          + "\nFirst terminate that job or create a job with a different name."
          + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
      return false;
    }

    // if NodePort service is requested start one,
    // otherwise start a headless service
    V1Service serviceForWorkers = null;
    if (KubernetesContext.nodePortServiceRequested(config)) {
      serviceForWorkers = RequestObjectBuilder.createNodePortServiceObject();
    } else {
      serviceForWorkers = RequestObjectBuilder.createJobServiceObject();
    }

    boolean serviceCreated = controller.createService(namespace, serviceForWorkers);
    if (serviceCreated) {
      jobSubmissionStatus.setServiceForWorkersCreated(true);
    } else {
      LOG.severe("Following service could not be created: " + workersServiceName
          + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
      return false;
    }

    // if Job Master runs as a separate pod, initialize a service for that
    if (!JobMasterContext.jobMasterRunsInClient(config)) {

      V1Service serviceForJobMaster = JobMasterRequestObject.createJobMasterHeadlessServiceObject();
//      V1Service serviceForJobMaster = JobMasterRequestObject.createJobMasterServiceObject();
      serviceCreated = controller.createService(namespace, serviceForJobMaster);
      if (serviceCreated) {
        jobSubmissionStatus.setServiceForJobMasterCreated(true);
      } else {
        LOG.severe("Following service could not be created: " + jobMasterServiceName
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
        return false;
      }
    }

    return true;
  }

  private boolean initPersistentVolumeClaim(JobAPI.Job job) {

    String pvcName = KubernetesUtils.createPersistentVolumeClaimName(job.getJobName());
    // check whether there is a PersistentVolumeClaim object, if so, no need to create a new one
    // otherwise create a PersistentVolumeClaim
    V1PersistentVolumeClaim pvc = controller.getPersistentVolumeClaim(namespace, pvcName);
    if (pvc == null) {
      pvc =
          RequestObjectBuilder.createPersistentVolumeClaimObject(pvcName, job.getNumberOfWorkers());

      boolean claimCreated = controller.createPersistentVolumeClaim(namespace, pvc);
      if (claimCreated) {
        jobSubmissionStatus.setPvcCreated(true);
      } else {
        LOG.log(Level.SEVERE, "PersistentVolumeClaim could not be created. "
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
        return false;
      }

    } else {
      LOG.log(Level.WARNING, "There is already a PersistentVolumeClaim with the name: " + pvcName
          + "\nPlease terminate any artifacts from previous jobs or change your job name. "
          + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
      return false;
    }

    return true;
  }


  private boolean initStatefulSets(String jobName,
                                   long jobFileSize,
                                   JobAPI.Job job) {

    // first check whether there is a StatefulSet with the same name,
    // if so, do not submit new job. Give a message and terminate
    // user needs to explicitly terminate that job
    ArrayList<String> statefulSetNames = new ArrayList<>();
    for (int i = 0; i < job.getComputeResourceList().size(); i++) {
      statefulSetNames.add(KubernetesUtils.createWorkersStatefulSetName(jobName, i));
    }

    if (!JobMasterContext.jobMasterRunsInClient(config)) {
      String jobMasterStatefulSetName = KubernetesUtils.createJobMasterStatefulSetName(jobName);
      statefulSetNames.add(jobMasterStatefulSetName);
    }

    boolean statefulSetExists = controller.statefulSetsExist(namespace, statefulSetNames);

    if (statefulSetExists) {
      LOG.severe("First terminate the previously running job with the same name. "
          + "\nOr submit the job with a different job name"
          + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
      return false;
    }

    // create statefulset for the job master
    if (!JobMasterContext.jobMasterRunsInClient(config)) {

      // create the StatefulSet object for this job
      V1beta2StatefulSet jobMasterStatefulSet = JobMasterRequestObject.createStatefulSetObject();

      if (jobMasterStatefulSet == null) {
        return false;
      }

      boolean statefulSetCreated = controller.createStatefulSetJob(namespace, jobMasterStatefulSet);
      if (statefulSetCreated) {
        jobSubmissionStatus.addCreatedStatefulSetName(jobMasterStatefulSet.getMetadata().getName());
      } else {
        LOG.severe("Please run terminate job to clear up any artifacts from previous jobs."
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
        return false;
      }
    }

    String encodedNodeInfoList = null;
    // if node locations will be retrieved from Kubernetes master
    if (!KubernetesContext.nodeLocationsFromConfig(config)) {
      // first get Node list and build encoded NodeInfoUtils Strings
      String rackLabelKey = KubernetesContext.rackLabelKeyForK8s(config);
      String dcLabelKey = KubernetesContext.datacenterLabelKeyForK8s(config);
      ArrayList<JobMasterAPI.NodeInfo> nodeInfoList =
          controller.getNodeInfo(rackLabelKey, dcLabelKey);
      encodedNodeInfoList = NodeInfoUtils.encodeNodeInfoList(nodeInfoList);
      LOG.fine("NodeInfo objects: size " + nodeInfoList.size()
          + "\n" + NodeInfoUtils.listToString(nodeInfoList));
    }

    // let the transfer threads know that we are about to submit the StatefulSets
    JobPackageTransferThread.setSubmittingStatefulSets();

    // create StatefulSets for workers
    for (int i = 0; i < job.getComputeResourceList().size(); i++) {

      JobAPI.ComputeResource computeResource = JobUtils.getComputeResource(job, i);
      if (computeResource == null) {
        LOG.severe("Something wrong with the job object. Can not get ComputeResource from job"
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
        return false;
      }

      // create the StatefulSet object for this job
      V1beta2StatefulSet statefulSet = RequestObjectBuilder.createStatefulSetForWorkers(
          computeResource, jobFileSize, encodedNodeInfoList);

      if (statefulSet == null) {
        return false;
      }

      boolean statefulSetCreated = controller.createStatefulSetJob(namespace, statefulSet);
      if (statefulSetCreated) {
        jobSubmissionStatus.addCreatedStatefulSetName(statefulSet.getMetadata().getName());
      } else {
        LOG.severe("Please run terminate job to clear up any artifacts from previous jobs."
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
        return false;
      }
    }

    return true;
  }

  /**
   * check whether configuration parameters are accurate
   * @return
   */
  private boolean configParametersOK(JobAPI.Job job) {

    // number of workers has to be divisible by workersPerPod in each ComputeResource
    // all pods will have equal number of containers
    // all pods will be identical
    for (JobAPI.ComputeResource computeResource: job.getComputeResourceList()) {
      int workersPerPod = computeResource.getWorkersPerPod();
      int numberOfWorkers = computeResource.getNumberOfWorkers();
      if (numberOfWorkers % workersPerPod != 0) {
        LOG.log(Level.SEVERE, String.format("workersPerPod has to be divisible by worker instances."
            + " workersPerPod: " + workersPerPod + " numberOfWorkers: " + numberOfWorkers
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++"));
        return false;
      }
    }

    // when OpenMPI is enabled, a Secret object has to be available in the cluster
    if (SchedulerContext.useOpenMPI(config)) {
      String secretName = KubernetesContext.secretName(config);
      boolean secretExists = controller.secretExist(namespace, secretName);

      if (!secretExists) {
        LOG.severe("No Secret object is available in the cluster with the name: " + secretName
            + "\nFirst create this object or make that object created by your cluster admin."
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
        return false;
      }
    }

    // when OpenMPI is enabled, all pods need to have equal numbers workers
    // we check whether all workersPerPod values are equal to first ComputeResource workersPerPod
    if (SchedulerContext.useOpenMPI(config)) {
      int workersPerPod = job.getComputeResource(0).getWorkersPerPod();
      for (int i = 1; i < job.getComputeResourceList().size(); i++) {
        if (workersPerPod != job.getComputeResource(i).getWorkersPerPod()) {
          LOG.log(Level.SEVERE, String.format("When OpenMPI is used, all workersPerPod values "
              + "in ComputeResources have to be the same. "
              + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++"));
          return false;
        }
      }
    }

    // if statically binding requested, number for CPUs per worker has to be an integer
    if (KubernetesContext.bindWorkerToCPU(config)) {
      for (JobAPI.ComputeResource computeResource: job.getComputeResourceList()) {
        double cpus = computeResource.getCpu();
        if (cpus % 1 != 0) {
          LOG.log(Level.SEVERE, String.format("When %s is true, the value of cpu has to be an int"
                  + " cpu= " + cpus
                  + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++",
              KubernetesContext.K8S_BIND_WORKER_TO_CPU));
          return false;
        }
      }
    }

    // when worker to nodes mapping is requested
    // if the operator is either Exists or DoesNotExist,
    // values list must be empty
    if (KubernetesContext.workerToNodeMapping(config)) {
      String operator = KubernetesContext.workerMappingOperator(config);
      List<String> values = KubernetesContext.workerMappingValues(config);
      if (("Exists".equalsIgnoreCase(operator) || "DoesNotExist".equalsIgnoreCase(operator))
          && values != null && values.size() != 0) {
        LOG.log(Level.SEVERE, String.format("When the value of %s is either Exists or DoesNotExist"
                + "\n%s list must be empty. Current content of this list: " + values
                + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++",
            KubernetesContext.K8S_WORKER_MAPPING_OPERATOR,
            KubernetesContext.K8S_WORKER_MAPPING_VALUES));
        return false;
      }
    }

    // When nodePort service requested, WORKERS_PER_POD value must be 1
    if (KubernetesContext.nodePortServiceRequested(config)) {
      for (JobAPI.ComputeResource computeResource: job.getComputeResourceList()) {
        if (computeResource.getWorkersPerPod() != 1) {
          LOG.log(Level.SEVERE, "workersPerPod value must be 1, when starting NodePort service. "
              + "Please change the config value and resubmit the job"
              + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Close up any resources
   */
  @Override
  public void close() {
  }

  /**
   * We clear up the resources that we created during the job submission process
   * we don't delete the resources that may be left from previous jobs
   * They have to be deleted explicitly by the user
   */
  private void clearupWhenSubmissionFails(String jobName) {

    LOG.info("Will clear up any resources created during the job submission process.");

    JobPackageTransferThread.cancelTransfers();

    // first delete the service objects
    // delete the job service
    if (jobSubmissionStatus.isServiceForWorkersCreated()) {
      String serviceName = KubernetesUtils.createServiceName(jobName);
      controller.deleteService(namespace, serviceName);
    }

    // delete the job master service
    if (jobSubmissionStatus.isServiceForJobMasterCreated()) {
      String jobMasterServiceName = KubernetesUtils.createJobMasterServiceName(jobName);
      controller.deleteService(namespace, jobMasterServiceName);
    }

    // delete created StatefulSet objects
    ArrayList<String> ssNameLists = jobSubmissionStatus.getCreatedStatefulSetNames();
    for (String ssName: ssNameLists) {
      controller.deleteStatefulSetJob(namespace, ssName);
    }

    // delete the persistent volume claim
    if (jobSubmissionStatus.isPvcCreated()) {
      String pvcName = KubernetesUtils.createPersistentVolumeClaimName(jobName);
      boolean claimDeleted = controller.deletePersistentVolumeClaim(namespace, pvcName);
    }
  }

  /**
   * Terminate the Kubernetes Job
   */
  @Override
  public boolean terminateJob(String jobName) {

    // first delete the job master StatefulSet
    String jobMasterStatefulSetName = KubernetesUtils.createJobMasterStatefulSetName(jobName);
    boolean deleted = controller.deleteStatefulSetJob(namespace, jobMasterStatefulSetName);

    // delete workers the StatefulSets
    ArrayList<String> ssNameLists = controller.getStatefulSetsForJobWorkers(namespace, jobName);
    for (String ssName: ssNameLists) {
      controller.deleteStatefulSetJob(namespace, ssName);
    }

    // delete the job service
    String serviceName = KubernetesUtils.createServiceName(jobName);
    deleted = controller.deleteService(namespace, serviceName);

    // delete the job master service
    String jobMasterServiceName = KubernetesUtils.createJobMasterServiceName(jobName);
    controller.deleteService(namespace, jobMasterServiceName);

    // delete the persistent volume claim
    String pvcName = KubernetesUtils.createPersistentVolumeClaimName(jobName);
    boolean claimDeleted = controller.deletePersistentVolumeClaim(namespace, pvcName);

    return true;
  }
}
