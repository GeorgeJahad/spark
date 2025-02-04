/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.scheduler.cluster.armada

// import java.io.File

// import io.fabric8.kubernetes.client.Config
// import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.{SparkConf, SparkContext}
// import org.apache.spark.deploy.k8s.
//   {KubernetesConf, KubernetesUtils, SparkKubernetesClientFactory}
// import org.apache.spark.deploy.k8s.Config._
// import org.apache.spark.deploy.k8s.Constants.DEFAULT_EXECUTOR_CONTAINER_NAME
// import org.apache.spark.internal.LogKeys.MASTER_URL
// import org.apache.spark.internal.config.TASK_MAX_FAILURES
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
// import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.util.{ThreadUtils} // {Clock, SystemClock, ThreadUtils, Utils}

private[spark] class ArmadaClusterManager extends ExternalClusterManager with Logging {
  // import SparkMasterRegex._

  override def canCreate(masterURL: String): Boolean = masterURL.startsWith("armada")

  private def isLocal(conf: SparkConf): Boolean =
    true

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    val maxTaskFailures = 1
    logInfo("gbj3 cluster manager")
    new TaskSchedulerImpl(sc, maxTaskFailures, isLocal(sc.conf))
  }

  override def createSchedulerBackend(
      sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {
    // val wasSparkSubmittedInClusterMode = sc.conf.get(KUBERNETES_DRIVER_SUBMIT_CHECK)

    // TODO: Create Armada client here.
    /*
    val kubernetesClient = SparkKubernetesClientFactory.createKubernetesClient(
      apiServerUri,
      Some(sc.conf.get(KUBERNETES_NAMESPACE)),
      authConfPrefix,
      SparkKubernetesClientFactory.ClientType.Driver,
      sc.conf,
      defaultServiceAccountCaCrt)

    if (sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_FILE).isDefined) {
      KubernetesUtils.loadPodFromTemplate(
        kubernetesClient,
        sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_FILE).get,
        sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_CONTAINER_NAME),
        sc.conf)
    }
    */

    val schedulerExecutorService = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "kubernetes-executor-maintenance")

    /*
    ExecutorPodsSnapshot.setShouldCheckAllContainers(
      sc.conf.get(KUBERNETES_EXECUTOR_CHECK_ALL_CONTAINERS))
    val sparkContainerName = sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_CONTAINER_NAME)
      .getOrElse(DEFAULT_EXECUTOR_CONTAINER_NAME)
    ExecutorPodsSnapshot.setSparkContainerName(sparkContainerName)
    val subscribersExecutor = ThreadUtils
      .newDaemonThreadPoolScheduledExecutor(
        "kubernetes-executor-snapshots-subscribers", 2)
    val snapshotsStore = new ExecutorPodsSnapshotsStoreImpl(subscribersExecutor, conf = sc.conf)

    val executorPodsLifecycleEventHandler = new ExecutorPodsLifecycleManager(
      sc.conf,
      kubernetesClient,
      snapshotsStore)

    val executorPodsAllocator = makeExecutorPodsAllocator(sc, kubernetesClient, snapshotsStore)

    val podsWatchEventSource = new ExecutorPodsWatchSnapshotSource(
      snapshotsStore,
      kubernetesClient,
      sc.conf)

    val eventsPollingExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "kubernetes-executor-pod-polling-sync")
    val podsPollingEventSource = new ExecutorPodsPollingSnapshotSource(
      sc.conf, kubernetesClient, snapshotsStore, eventsPollingExecutor)
    */

    new ArmadaClusterSchedulerBackend(
      scheduler.asInstanceOf[TaskSchedulerImpl],
      sc,
      new ArmadaClient, // FIXME
      schedulerExecutorService,
      masterURL)
      // snapshotsStore,
      // executorPodsAllocator,
      // executorPodsLifecycleEventHandler,
      // podsWatchEventSource,
      // podsPollingEventSource)
  }

  /*
  private[armada] def makeExecutorPodsAllocator(
      sc: SparkContext, kubernetesClient: KubernetesClient,
      snapshotsStore: ExecutorPodsSnapshotsStore) = {
    val executorPodsAllocatorName = sc.conf.get(KUBERNETES_ALLOCATION_PODS_ALLOCATOR) match {
      case "statefulset" =>
        classOf[StatefulSetPodsAllocator].getName
      case "direct" =>
        classOf[ExecutorPodsAllocator].getName
      case fullClass =>
        fullClass
    }

    val cls = Utils.classForName[AbstractPodsAllocator](executorPodsAllocatorName)
    val cstr = cls.getConstructor(
      classOf[SparkConf], classOf[org.apache.spark.SecurityManager],
      classOf[KubernetesExecutorBuilder], classOf[KubernetesClient],
      classOf[ExecutorPodsSnapshotsStore], classOf[Clock])
    cstr.newInstance(
      sc.conf,
      sc.env.securityManager,
      new KubernetesExecutorBuilder(),
      kubernetesClient,
      snapshotsStore,
      new SystemClock())
  }
  */

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}
