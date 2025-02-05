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

import java.util.concurrent.{ScheduledExecutorService}

import scala.collection.mutable.HashMap

import org.apache.spark.SparkContext
import org.apache.spark.rpc.{RpcAddress, RpcCallContext}
import org.apache.spark.scheduler.{ExecutorDecommission, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}

import io.grpc.ManagedChannelBuilder
import k8s.io.api.core.v1.generated.{Container, PodSpec, ResourceRequirements}
import k8s.io.api.core.v1.generated.{EnvVar, EnvVarSource, ObjectFieldSelector}
import k8s.io.apimachinery.pkg.api.resource.generated.Quantity
import api.submit.SubmitGrpc



// FIXME: Actually import ArmadaClient
class ArmadaClient {}

// TODO: Implement for Armada
private[spark] class ArmadaClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    armadaClient: ArmadaClient,
    executorService: ScheduledExecutorService,
    masterURL: String)
    extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

    // FIXME
    private val appId = "fake_app_id_FIXME"

    private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

    override def applicationId(): String = {
        conf.getOption("spark.app.id").getOrElse(appId)
    }


  def submitJob(): Unit = {

    val urlArray = masterURL.split(":")
    val host = urlArray(1)
    val port = urlArray(2).toInt

    val driverAddr = sys.env("SPARK_DRIVER_BIND_ADDRESS")


    val driverURL = s"spark://CoarseGrainedScheduler@${driverAddr}:7078"
    val source = new EnvVarSource().withFieldRef(new ObjectFieldSelector().withApiVersion("v1").withFieldPath("status.podIP"))
    val envVars = Seq(
      new EnvVar().withName("SPARK_EXECUTOR_ID").withValue("1"),
      new EnvVar().withName("SPARK_RESOURCE_PROFILE_ID").withValue("0"),
      new EnvVar().withName("SPARK_EXECUTOR_POD_NAME").withValue("test-pod-name"),
      new EnvVar().withName("SPARK_APPLICATION_ID").withValue("test_spark_app_id"),
      new EnvVar().withName("SPARK_EXECUTOR_CORES").withValue("1"),
      new EnvVar().withName("SPARK_EXECUTOR_MEMORY").withValue("512m"),
      new EnvVar().withName("SPARK_DRIVER_URL").withValue(driverURL),
      new EnvVar().withName("SPARK_EXECUTOR_POD_IP").withValueFrom(source),
    )
    val executorContainer = Container()
      .withName("spark-executor")
      .withImagePullPolicy("IfNotPresent")
      .withImage("spark:testing")
      .withEnv(envVars)
      .withCommand(Seq("/opt/entrypoint.sh"))
      .withArgs(
        Seq(
          "executor",
        )
      )
      .withResources(
        ResourceRequirements(
          limits = Map(
            "memory" -> Quantity(Option("1000Mi")),
            "cpu" -> Quantity(Option("100m"))
          ),
          requests = Map(
            "memory" -> Quantity(Option("1000Mi")),
            "cpu" -> Quantity(Option("100m"))
          )
        )
      )

    val podSpec = PodSpec()
      .withTerminationGracePeriodSeconds(0)
      .withRestartPolicy("Never")
      .withContainers(Seq(executorContainer))

    val testJob = api.submit
      .JobSubmitRequestItem()
      .withPriority(0)
      .withNamespace("default")
      .withPodSpec(podSpec)

    val testJobRequest = api.submit.JobSubmitRequest(
      queue = "test",
      jobSetId = "executor",
      jobRequestItems = Seq(testJob)
    )

    val channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
    val blockingStub = SubmitGrpc.blockingStub(channel)

    val jobSubmitResponse = blockingStub.submitJobs(testJobRequest)

    println(s"Job Submit Response")
    for (respItem <- jobSubmitResponse.jobResponseItems) {
      println(s"JobID: ${respItem.jobId}  Error: ${respItem.error} ")

    }
  }
    override def start(): Unit = {
      submitJob()
    }

    override def stop(): Unit = {}

    /*
    override def doRequestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] = {
        //podAllocator.setTotalExpectedExecutors(resourceProfileToTotalExecs)
        //Future.successful(true)
    }
    */

    override def sufficientResourcesRegistered(): Boolean = {
      totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
    }

    override def getExecutorIds(): Seq[String] = synchronized {
      super.getExecutorIds()
    }

    override def createDriverEndpoint(): DriverEndpoint = {
      logInfo("gbj12 driver endpoint")
      new ArmadaDriverEndpoint()
    }

    private class ArmadaDriverEndpoint extends DriverEndpoint {
      protected val execIDRequester = new HashMap[RpcAddress, String]

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] =
            super.receiveAndReply(context)
        /* generateExecID(context).orElse(
          ignoreRegisterExecutorAtStoppedContext.orElse(
            super.receiveAndReply(context))) */

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
