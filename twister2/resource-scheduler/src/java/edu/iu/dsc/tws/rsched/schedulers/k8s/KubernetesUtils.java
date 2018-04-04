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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.resource.RequestedResources;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;

import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ContainerPort;
import io.kubernetes.client.models.V1EmptyDirVolumeSource;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1EnvVarSource;
import io.kubernetes.client.models.V1LabelSelector;
import io.kubernetes.client.models.V1NFSVolumeSource;
import io.kubernetes.client.models.V1ObjectFieldSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1PersistentVolume;
import io.kubernetes.client.models.V1PersistentVolumeClaim;
import io.kubernetes.client.models.V1PersistentVolumeClaimSpec;
import io.kubernetes.client.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.models.V1PersistentVolumeSpec;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodTemplateSpec;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1ServicePort;
import io.kubernetes.client.models.V1ServiceSpec;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;
import io.kubernetes.client.models.V1beta2StatefulSet;
import io.kubernetes.client.models.V1beta2StatefulSetSpec;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.POD_SHARED_VOLUME;

public final class KubernetesUtils {
  private static final Logger LOG = Logger.getLogger(KubernetesUtils.class.getName());

  private KubernetesUtils() {
  }

  /**
   * when the given name is in the form of "name-id"
   * it returns the id as int
   * @param name
   * @return
   */
  public static int idFromName(String name) {
    return Integer.parseInt(name.substring(name.lastIndexOf("-") + 1));
  }

  /**
   * create file copy command to a pod
   * @return
   */
  public static String[] createCopyCommand(String filename, String namespace, String podName) {
    String targetDir = String.format("%s/%s:%s", namespace, podName, POD_SHARED_VOLUME);
    return new String[]{"kubectl", "cp", filename, targetDir};
  }

  /**
   * create podName from jobName with pod index
   * @param jobName
   * @return
   */
  public static String podNameFromJobName(String jobName, int podIndex) {
    return jobName + "-" + podIndex;
  }

  /**
   * create service name from job name
   * @param jobName
   * @return
   */
  public static String createServiceName(String jobName) {
    return KubernetesConstants.TWISTER2_SERVICE_PREFIX + jobName;
  }

  /**
   * create persistent directory name for a job
   * @param jobName
   * @return
   */
  public static String createPersistentJobDirName(String jobName) {
    return KubernetesConstants.PERSISTENT_VOLUME_MOUNT + "/" + jobName
        + "-" + System.currentTimeMillis();
  }

  /**
   * create storage claim name name from job name
   * @param jobName
   * @return
   */
  public static String createStorageClaimName(String jobName) {
    return KubernetesConstants.TWISTER2_STORAGE_CLAIM_PREFIX + jobName;
  }

  /**
   * create storage claim name name from job name
   * @param jobName
   * @return
   */
  public static String createPersistentVolumeName(String jobName) {
    return "persistent-volume-" + jobName;
  }

  /**
   * create service label from job name
   * this label is used when constructing statefulset
   * @param jobName
   * @return
   */
  public static String createServiceLabel(String jobName) {
    return KubernetesConstants.SERVICE_LABEL_PREFIX + jobName;
  }

  /**
   * this label is used when submitting queries to kubernetes master
   * @param jobName
   * @return
   */
  public static String createServiceLabelWithApp(String jobName) {
    return "app=" + createServiceLabel(jobName);
  }

  /**
   * create container name with the given containerIndex
   * each container in a pod will have a unique name with this index
   * @param containerIndex
   * @return
   */
  public static String createContainerName(int containerIndex) {
    return KubernetesConstants.CONTAINER_NAME_PREFIX + containerIndex;
  }

  /**
   * TBD: calculate containers per pod or issue error
   * when the numberOfContainers can not be divisible by the number of containersPerPod
   * @param jobName
   * @param resourceRequest
   * @return
   */
  public static V1beta2StatefulSet createStatefulSetObjectForJob(String jobName,
                                                                 RequestedResources resourceRequest,
                                                                 long jobFileSize,
                                                                 Config config) {

    V1beta2StatefulSet statefulSet = new V1beta2StatefulSet();
    statefulSet.setApiVersion("apps/v1beta2");
    statefulSet.setKind("StatefulSet");

    // construct metadata and set for jobName setting
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(jobName);
    statefulSet.setMetadata(meta);

    // construct JobSpec and set
    V1beta2StatefulSetSpec setSpec = new V1beta2StatefulSetSpec();
    setSpec.serviceName(createServiceName(jobName));
    // pods will be started in parallel
    // by default they are started sequentially
    setSpec.setPodManagementPolicy("Parallel");

    // number of containers has to be divisible by the containersPerPod
    // all pods will have equal number of containers
    // all pods will be identical
    int containersPerPod = KubernetesContext.containersPerPod(config);
    if (resourceRequest.getNoOfContainers() % containersPerPod != 0) {
      LOG.log(Level.SEVERE, "Number of containers has to be divisible by containersPerPod.\n"
          + "Number of containers: " + resourceRequest.getNoOfContainers() + "\n"
          + "containersPerPod: " + containersPerPod + "\n"
          + "Aborting submission.");
      return null;
    }

    int numberOfPods = resourceRequest.getNoOfContainers() / containersPerPod;
    setSpec.setReplicas(numberOfPods);

    // add selector for the job
    V1LabelSelector selector = new V1LabelSelector();
    String serviceLabel = createServiceLabel(jobName);
    selector.putMatchLabelsItem("app", serviceLabel);
    setSpec.setSelector(selector);

    // construct the pod template
    V1PodTemplateSpec template = constructPodTemplate(
        resourceRequest.getContainer(), serviceLabel, jobFileSize, config);
    setSpec.setTemplate(template);

    statefulSet.setSpec(setSpec);

    return statefulSet;
  }

  /**
   * construct pod template
   * @param reqContainer
   * @param serviceLabel
   * @param jobFileSize
   * @param config
   * @return
   */
  public static V1PodTemplateSpec constructPodTemplate(ResourceContainer reqContainer,
                                                       String serviceLabel,
                                                       long jobFileSize,
                                                       Config config) {

    V1PodTemplateSpec template = new V1PodTemplateSpec();
    V1ObjectMeta templateMetaData = new V1ObjectMeta();
    HashMap<String, String> labels = new HashMap<String, String>();
    labels.put("app", serviceLabel);
    templateMetaData.setLabels(labels);
    template.setMetadata(templateMetaData);

    V1PodSpec podSpec = new V1PodSpec();
    podSpec.setTerminationGracePeriodSeconds(0L);

    V1Volume volume = new V1Volume();
    volume.setName(KubernetesConstants.POD_SHARED_VOLUME_NAME);
    V1EmptyDirVolumeSource volumeSource = new V1EmptyDirVolumeSource();
    volumeSource.setMedium("Memory");
    volume.setEmptyDir(volumeSource);
    ArrayList<V1Volume> volumes = new ArrayList<>();
    volumes.add(volume);

    String persistentJobDir = null;

    if (SchedulerContext.persistentVolumeRequested(config)) {
      V1Volume persistentVolume = new V1Volume();
      persistentVolume.setName(KubernetesConstants.PERSISTENT_VOLUME_NAME);
      V1PersistentVolumeClaimVolumeSource perVolSource = new V1PersistentVolumeClaimVolumeSource();
      String claimName = createStorageClaimName(SchedulerContext.jobName(config));
      perVolSource.setClaimName(claimName);
      persistentVolume.setPersistentVolumeClaim(perVolSource);

      volumes.add(persistentVolume);

      persistentJobDir = createPersistentJobDirName(SchedulerContext.jobName(config));
    }

    podSpec.setVolumes(volumes);

    int containersPerPod = KubernetesContext.containersPerPod(config);
    int basePort = KubernetesContext.workerBasePort(config);
    ArrayList<V1Container> containers = new ArrayList<V1Container>();
    for (int i = 0; i < containersPerPod; i++) {
      containers.add(constructContainer(
          i, reqContainer, jobFileSize, basePort + 1, persistentJobDir, config));
    }
    podSpec.setContainers(containers);

    template.setSpec(podSpec);
    return template;
  }

  /**
   * construct a container
   * @param containerIndex
   * @param reqContainer
   * @param jobFileSize
   * @param containerPort
   * @param config
   * @return
   */
  public static V1Container constructContainer(int containerIndex,
                                               ResourceContainer reqContainer,
                                               long jobFileSize,
                                               int containerPort,
                                               String persistentJobDir,
                                               Config config) {
    // construct container and add it to podSpec
    V1Container container = new V1Container();
    String containerName = createContainerName(containerIndex);
    container.setName(containerName);
    container.setImage(KubernetesConstants.TWISTER2_DOCKER_IMAGE);
    // by default: IfNotPresent
    // can be set to Always from client.yaml
    container.setImagePullPolicy(KubernetesContext.imagePullPolicy(config));

//        container.setArgs(Arrays.asList("1000000")); parameter to the main method
//    container.setCommand(Arrays.asList("java", TWISTER2_WORKER_CLASS));
    V1EnvVar var1 = new V1EnvVar()
        .name(KubernetesField.JOB_PACKAGE_FILENAME + "")
        .value(SchedulerContext.jobPackageFileName(config));
    V1EnvVar var2 = new V1EnvVar()
        .name(KubernetesField.JOB_PACKAGE_FILE_SIZE + "").value(jobFileSize + "");
    V1EnvVar var3 = new V1EnvVar()
        .name(KubernetesField.CONTAINER_NAME + "").value(containerName);
    V1EnvVar var4 = new V1EnvVar()
        .name(KubernetesField.USER_JOB_JAR_FILE + "")
        .value(SchedulerContext.userJobJarFile(config));
    V1EnvVar var5 = new V1EnvVar()
        .name(KubernetesField.JOB_DESCRIPTION_FILE + "")
        .value(SchedulerContext.jobDescriptionFile(config));

    // POD_IP with downward API
    V1ObjectFieldSelector fieldSelector = new V1ObjectFieldSelector();
    fieldSelector.setFieldPath("status.podIP");
    V1EnvVarSource varSource = new V1EnvVarSource();
    varSource.setFieldRef(fieldSelector);

    V1EnvVar var6 = new V1EnvVar()
        .name(KubernetesField.POD_IP + "")
        .valueFrom(varSource);

    V1EnvVar var7 = new V1EnvVar()
        .name(KubernetesField.PERSISTENT_JOB_DIR + "").value(persistentJobDir);

    container.setEnv(Arrays.asList(var1, var2, var3, var4, var5, var6, var7));

    V1ResourceRequirements resReq = new V1ResourceRequirements();
    resReq.putRequestsItem("cpu", reqContainer.getNoOfCpus() + "");
    resReq.putRequestsItem("memory", reqContainer.getMemoryMegaBytes() + "Mi");
    container.setResources(resReq);

    ArrayList<V1VolumeMount> volumeMounts = new ArrayList<>();
    V1VolumeMount volumeMount = new V1VolumeMount();
    volumeMount.setName(KubernetesConstants.POD_SHARED_VOLUME_NAME);
    volumeMount.setMountPath(KubernetesConstants.POD_SHARED_VOLUME);
    volumeMounts.add(volumeMount);

    if (SchedulerContext.persistentVolumeRequested(config)) {
      V1VolumeMount persVolumeMount = new V1VolumeMount();
      persVolumeMount.setName(KubernetesConstants.PERSISTENT_VOLUME_NAME);
      persVolumeMount.setMountPath(KubernetesConstants.PERSISTENT_VOLUME_MOUNT);
      volumeMounts.add(persVolumeMount);
    }

    container.setVolumeMounts(volumeMounts);

    V1ContainerPort port = new V1ContainerPort().name("port1").containerPort(containerPort);
    port.setProtocol("TCP");
    container.setPorts(Arrays.asList(port));

    return container;
  }

  public static V1Service createServiceObject(String serviceName,
                                              String serviceLabel,
                                              int port,
                                              int targetPort) {
    V1Service service = new V1Service();
    service.setKind("Service");
    service.setApiVersion("v1");

    // construct and set metadata
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(serviceName);
    service.setMetadata(meta);

    // construct and set service spec
    V1ServiceSpec serviceSpec = new V1ServiceSpec();
    // ClusterIP needs to be None for headless service
    serviceSpec.setClusterIP("None");
    // set selector
    HashMap<String, String> selectors = new HashMap<String, String>();
    selectors.put("app", serviceLabel);
    serviceSpec.setSelector(selectors);

    ArrayList<V1ServicePort> ports = new ArrayList<V1ServicePort>();
    V1ServicePort servicePort = new V1ServicePort();
    servicePort.setPort(port);
    servicePort.setTargetPort(new IntOrString(targetPort));
    servicePort.setProtocol("TCP");
    ports.add(servicePort);
    serviceSpec.setPorts(ports);

    service.setSpec(serviceSpec);

    return service;
  }

  public static V1PersistentVolume createPersistentVolumeObject(Config config, String pvName) {
    V1PersistentVolume pv = new V1PersistentVolume();
    pv.setApiVersion("v1");

    // set pv name
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(pvName);
    pv.setMetadata(meta);

    String volumeSize = SchedulerContext.persistentVolumeTotal(config);
    V1PersistentVolumeSpec pvSpec = new V1PersistentVolumeSpec();
    HashMap<String, String> capacity = new HashMap<>();
    capacity.put("storage", volumeSize);
    pvSpec.setCapacity(capacity);

    String storageClass = KubernetesContext.persistentStorageClass(config);
    String accessMode = KubernetesContext.storageAccessMode(config);
    String reclaimPolicy = KubernetesContext.storageReclaimPolicy(config);
    pvSpec.setStorageClassName(storageClass);
    pvSpec.setAccessModes(Arrays.asList(accessMode));
    pvSpec.setPersistentVolumeReclaimPolicy(reclaimPolicy);
//    pvSpec.setMountOptions(Arrays.asList("hard", "nfsvers=4.1"));

    V1NFSVolumeSource nfsVolumeSource = new V1NFSVolumeSource();
    nfsVolumeSource.setServer(SchedulerContext.nfsServerAddress(config));
    nfsVolumeSource.setPath(SchedulerContext.nfsServerPath(config));
    pvSpec.setNfs(nfsVolumeSource);

    pv.setSpec(pvSpec);

    return pv;
  }



  public static V1PersistentVolumeClaim createPersistentVolumeClaimObject(
      Config config, String pvcName) {

    V1PersistentVolumeClaim pvc = new V1PersistentVolumeClaim();
    pvc.setApiVersion("v1");

    // set pvc name
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(pvcName);
    pvc.setMetadata(meta);

    String storageClass = KubernetesContext.persistentStorageClass(config);
    String accessMode = KubernetesContext.storageAccessMode(config);
    V1PersistentVolumeClaimSpec pvcSpec = new V1PersistentVolumeClaimSpec();
    pvcSpec.setStorageClassName(storageClass);
    pvcSpec.setAccessModes(Arrays.asList(accessMode));

    V1ResourceRequirements resources = new V1ResourceRequirements();
    String storageSize = SchedulerContext.persistentVolumePerWorker(config);
    resources.putRequestsItem("storage", storageSize);
//    resources.putRequestsItem("storage", Quantity.fromString("1Gi"));
    pvcSpec.setResources(resources);

    pvc.setSpec(pvcSpec);
    return pvc;
  }



}
