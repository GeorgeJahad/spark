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
package org.apache.spark.scheduler.cluster.k8s

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.HashMap
import scala.concurrent.Future



import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.SparkContext
import org.apache.spark.deploy.k8s.{KubernetesConf}
import org.apache.spark.deploy.k8s.Config._


import org.apache.spark.deploy.security.HadoopDelegationTokenManager


import org.apache.spark.internal.config.SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{RpcAddress, RpcCallContext}
import org.apache.spark.scheduler.{ExecutorDecommission, ExecutorDecommissionInfo, ExecutorKilled, ExecutorLossReason,
  TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RegisterExecutor
import org.apache.spark.util.{Utils}


private[spark] class KubernetesClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    kubernetesClient: KubernetesClient)
    extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {
  private val appId = KubernetesConf.getKubernetesAppId()

  protected override val minRegisteredRatio =
    if (conf.get(SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO).isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

  private val shouldDeleteDriverService = conf.get(KUBERNETES_DRIVER_SERVICE_DELETE_ON_TERMINATION)

  private val shouldDeleteExecutors = conf.get(KUBERNETES_DELETE_EXECUTORS)

  private val defaultProfile = scheduler.sc.resourceProfileManager.defaultResourceProfile

  private val namespace = conf.get(KUBERNETES_NAMESPACE)

  // Allow removeExecutor to be accessible by ExecutorPodsLifecycleEventHandler
  private[k8s] def doRemoveExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    removeExecutor(executorId, reason)
  }

  // private def setUpExecutorConfigMap(driverPod: Option[Pod]): Unit = {
  //   val configMapName = KubernetesClientUtils.configMapNameExecutor
  //   val resolvedExecutorProperties =
  //     Map(KUBERNETES_NAMESPACE.key -> namespace)
  //   val confFilesMap = KubernetesClientUtils
  //     .buildSparkConfDirFilesMap(configMapName, conf, resolvedExecutorProperties) ++
  //     resolvedExecutorProperties
  //   val labels =
  //     Map(SPARK_APP_ID_LABEL -> applicationId(), SPARK_ROLE_LABEL -> SPARK_POD_EXECUTOR_ROLE)
  //   val configMap = KubernetesClientUtils.buildConfigMap(configMapName, confFilesMap, labels)
  //   KubernetesUtils.addOwnerReference(driverPod.orNull, Seq(configMap))
  //   kubernetesClient.configMaps().inNamespace(namespace).resource(configMap).create()
  // }

  /**
   * Get an application ID associated with the job.
   * This returns the string value of spark.app.id if set, otherwise
   * the locally-generated ID.
   *
   * @return The application ID
   */
  override def applicationId(): String = {
    conf.getOption("spark.app.id").getOrElse(appId)
  }

  override def start(): Unit = {
    super.start()
    logInfo("gbj2 start")
    // Must be called before setting the executors
    val initExecs = Map(defaultProfile -> initialExecutors)
  }

  override def stop(): Unit = {
    // When `CoarseGrainedSchedulerBackend.stop` throws `SparkException`,
    // K8s cluster scheduler should log and proceed in order to delete the K8s cluster resources.
    Utils.tryLogNonFatalError {
      super.stop()
    }

    Utils.tryLogNonFatalError {
      kubernetesClient.close()
    }
  }

  override def doRequestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] = {
    Future.successful(true)
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
  }

  override def getExecutorIds(): Seq[String] = synchronized {
    super.getExecutorIds()
  }

  override def decommissionExecutors(
      executorsAndDecomInfo: Array[(String, ExecutorDecommissionInfo)],
      adjustTargetNumExecutors: Boolean,
      triggeredByExecutor: Boolean): Seq[String] = {
    super.decommissionExecutors(executorsAndDecomInfo, adjustTargetNumExecutors,
      triggeredByExecutor)
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    // If we've decided to remove some executors we should tell Kubernetes that we don't care.

    // Tell the executors to exit themselves.
    executorIds.foreach { id =>
      removeExecutor(id, ExecutorKilled)
    }

    // Give some time for the executors to shut themselves down, then forcefully kill any
    // remaining ones. This intentionally ignores the configuration about whether pods
    // should be deleted; only executors that shut down gracefully (and are then collected
    // by the ExecutorPodsLifecycleManager) will respect that configuration.
    val killTask = new Runnable() {
      override def run(): Unit = Utils.tryLogNonFatalError {
        Future.successful(true)
      }
    }

    // Return an immediate success, since we can't confirm or deny that executors have been
    // actually shut down without waiting too long and blocking the allocation thread, which
    // waits on this future to complete, blocking further allocations / deallocations.
    //
    // This relies a lot on the guarantees of Spark's RPC system, that a message will be
    // delivered to the destination unless there's an issue with the connection, in which
    // case the executor will shut itself down (and the driver, separately, will just declare
    // it as "lost"). Coupled with the allocation manager keeping track of which executors are
    // pending release, returning "true" here means that eventually all the requested executors
    // will be removed.
    //
    // The cleanup timer above is just an optimization to make sure that stuck executors don't
    // stick around in the k8s server. Normally it should never delete any pods at all.
    Future.successful(true)
  }

  override def createDriverEndpoint(): DriverEndpoint = {
    new KubernetesDriverEndpoint()
  }

  val execId = new AtomicInteger(0)

  override protected def createTokenManager(): Option[HadoopDelegationTokenManager] = {
    Some(new HadoopDelegationTokenManager(conf, sc.hadoopConfiguration, driverEndpoint))
  }

  override protected def isExecutorExcluded(executorId: String, hostname: String): Boolean = {
    true
  }

  private class KubernetesDriverEndpoint extends DriverEndpoint {

    protected val execIDRequester = new HashMap[RpcAddress, String]

    private def ignoreRegisterExecutorAtStoppedContext: PartialFunction[Any, Unit] = {
      case _: RegisterExecutor if sc.isStopped => // No-op
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] =
      ignoreRegisterExecutorAtStoppedContext.orElse(
        super.receiveAndReply(context))

    override def onDisconnected(rpcAddress: RpcAddress): Unit = {
      val execId = addressToExecutorId.get(rpcAddress)
      execId match {
        case Some(id) =>
          executorsPendingDecommission.get(id) match {
            case Some(host) =>
              // We don't pass through the host because by convention the
              // host is only populated if the entire host is going away
              // and we don't know if that's the case or just one container.
              removeExecutor(id, ExecutorDecommission(None))
            case _ =>
              // Don't do anything besides disabling the executor - allow the K8s API events to
              // drive the rest of the lifecycle decisions.
              // If it's disconnected due to network issues eventually heartbeat will clear it up.
              disableExecutor(id)
          }
        case _ =>
          val newExecId = execIDRequester.get(rpcAddress)
          newExecId match {
            case Some(id) =>
              execIDRequester -= rpcAddress
              // Expected, executors re-establish a connection with an ID
            case _ =>
              logDebug(s"No executor found for ${rpcAddress}")
          }
      }
    }
  }

}
