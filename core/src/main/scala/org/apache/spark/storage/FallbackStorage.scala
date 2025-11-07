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

package org.apache.spark.storage

import java.io.{DataInputStream, FileNotFoundException}
import java.nio.ByteBuffer

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP, STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH, STORAGE_DECOMMISSION_FALLBACK_STORAGE_REPLICATION_DELAY, STORAGE_DECOMMISSION_FALLBACK_STORAGE_REPLICATION_WAIT, STORAGE_DECOMMISSION_FALLBACK_STORAGE_SUBPATHS}
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleBlockInfo}
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.FallbackStorage.getPath
import org.apache.spark.storage.BlockManagerMessages.RemoveShuffle
import org.apache.spark.util.{Clock, SystemClock, Utils}

/**
 * A fallback storage used by storage decommissioners.
 */
private[storage] class FallbackStorage(conf: SparkConf) extends Logging {
  require(conf.contains("spark.app.id"))
  require(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined)

  private val fallbackPath = new Path(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).get)
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  private val fallbackFileSystem = FileSystem.get(fallbackPath.toUri, hadoopConf)
  private val appId = conf.getAppId

  val gbjFbs = "gbjFbs"
  // Visible for testing
  def copy(
      shuffleBlockInfo: ShuffleBlockInfo,
      bm: BlockManager): Unit = {
    logInfo("gbjf1")
    val shuffleId = shuffleBlockInfo.shuffleId
    val mapId = shuffleBlockInfo.mapId

    bm.migratableResolver match {
      case r: IndexShuffleBlockResolver =>
        val indexFile = r.getIndexFile(shuffleId, mapId)

        if (indexFile.exists()) {
          fallbackFileSystem.copyFromLocalFile(
            new Path(Utils.resolveURI(indexFile.getAbsolutePath)),
            getPath(conf, appId, shuffleId, indexFile.getName))

          val dataFile = r.getDataFile(shuffleId, mapId)
          if (dataFile.exists()) {
            fallbackFileSystem.copyFromLocalFile(
              new Path(Utils.resolveURI(dataFile.getAbsolutePath)),
              getPath(conf, appId, shuffleId, dataFile.getName))
          }

          // Report block statuses
          val reduceId = NOOP_REDUCE_ID
          val indexBlockId = ShuffleIndexBlockId(shuffleId, mapId, reduceId)
          FallbackStorage.reportBlockStatus(bm, indexBlockId, indexFile.length)
          if (dataFile.exists) {
            val dataBlockId = ShuffleDataBlockId(shuffleId, mapId, reduceId)
            FallbackStorage.reportBlockStatus(bm, dataBlockId, dataFile.length)
          }
        }
      case r =>
        logWarning(s"Unsupported Resolver: ${r.getClass.getName}")
    }
  }

  def exists(shuffleId: Int, filename: String): Boolean = {
    logInfo("gbjf2")
    fallbackFileSystem.exists(getPath(conf, appId, shuffleId, filename))
  }
}

private[storage] class FallbackStorageRpcEndpointRef(conf: SparkConf, hadoopConf: Configuration)
    extends RpcEndpointRef(conf) {
  // scalastyle:off executioncontextglobal
  import scala.concurrent.ExecutionContext.Implicits.global
  // scalastyle:on executioncontextglobal
  override def address: RpcAddress = null
  override def name: String = "fallback"
  override def send(message: Any): Unit = {}
  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    message match {
      case RemoveShuffle(shuffleId) =>
        logInfo("gbjf3")
        FallbackStorage.cleanUp(conf, hadoopConf, Some(shuffleId))
        Future{true.asInstanceOf[T]}
      case _ => Future{true.asInstanceOf[T]}
    }
  }
}

private[spark] object FallbackStorage extends Logging {
  /** We use one block manager id as a place holder. */
  val FALLBACK_BLOCK_MANAGER_ID: BlockManagerId = BlockManagerId("fallback", "remote", 7337)

  def getFallbackStorage(conf: SparkConf): Option[FallbackStorage] = {
    logInfo("gbjf4")
    if (conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined) {
      Some(new FallbackStorage(conf))
    } else {
      None
    }
  }

  /** Register the fallback block manager and its RPC endpoint. */
  def registerBlockManagerIfNeeded(master: BlockManagerMaster,
                                   conf: SparkConf,
                                   hadoopConf: Configuration): Unit = {
    logInfo("gbjf5")
    if (conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined) {
      master.registerBlockManager(
        FALLBACK_BLOCK_MANAGER_ID, Array.empty[String], 0, 0,
        new FallbackStorageRpcEndpointRef(conf, hadoopConf))
    }
  }

  /** Clean up the generated fallback location for this app. */
  /** Clean up the generated fallback location for this app (and shuffle id if given). */
  def cleanUp(conf: SparkConf, hadoopConf: Configuration, shuffleId: Option[Int] = None): Unit = {
    logInfo("gbjf6")
    if (conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined &&
        conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP) &&
        conf.contains("spark.app.id")) {
      val fallbackPath = shuffleId.foldLeft(
        new Path(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).get, conf.getAppId)
      ) { case (path, shuffleId) => new Path(path, shuffleId.toString) }
      val fallbackUri = fallbackPath.toUri
      val fallbackFileSystem = FileSystem.get(fallbackUri, hadoopConf)
      // The fallback directory for this app may not be created yet.
      if (fallbackFileSystem.exists(fallbackPath)) {
        logInfo(s"Attempt to clean up: $fallbackUri")
        if (fallbackFileSystem.delete(fallbackPath, true)) {
          logInfo(s"Succeed to clean up: $fallbackUri")
        } else {
          // Clean-up can fail due to the permission issues.
          logWarning(s"Failed to clean up: $fallbackUri")
        }
      }
    }
  }

  /** Report block status to block manager master and map output tracker master. */
  private def reportBlockStatus(blockManager: BlockManager, blockId: BlockId, dataLength: Long) = {
    logInfo("gbjf7")
    assert(blockManager.master != null)
    blockManager.master.updateBlockInfo(
      FALLBACK_BLOCK_MANAGER_ID, blockId, StorageLevel.DISK_ONLY, memSize = 0, dataLength)
  }

  /**
   * Provide the Path for a shuffle file.
   */
  private[storage] def getPath(conf: SparkConf,
                               appId: String,
                               shuffleId: Int,
                               filename: String): Path = {
    logInfo("gbjf8")
    val fallbackPath = new Path(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).get)
    val subPaths = conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_SUBPATHS)
    if (subPaths > 0) {
      val hash = JavaUtils.nonNegativeHash(filename) % subPaths
      new Path(fallbackPath, s"$appId/$shuffleId/$hash/$filename")
    } else {
      new Path(fallbackPath, s"$appId/$shuffleId/$filename")
    }
  }

  /**
   * Open the file, retry a FileNotFoundException for waitMs milliseconds,
   * unless this would exceed the deadline. In the latter case, rethrow the exception.
   */
  @tailrec
  private def open(filesystem: FileSystem,
                   path: Path,
                   deadlineMs: Long,
                   waitMs: Long,
                   clock: Clock) : FSDataInputStream = {
    logInfo("gbjf9")
    try {
      filesystem.open(path)
    } catch {
      case fnf: FileNotFoundException =>
        val waitTillMs = clock.getTimeMillis() + waitMs
        if (waitTillMs <= deadlineMs) {
          logInfo(f"File not found, waiting ${waitMs / 1000}s: $path")
          clock.waitTillTime(waitTillMs)
          open(filesystem, path, deadlineMs, waitMs, clock)
        } else {
          throw fnf
        }
    }
  }

  /**
   * Open the file and retry FileNotFoundExceptions according to
   * STORAGE_DECOMMISSION_FALLBACK_STORAGE_REPLICATION_DELAY and
   * STORAGE_DECOMMISSION_FALLBACK_STORAGE_REPLICATION_WAIT
   */
  // Visible for testing
  private[spark] def open(conf: SparkConf,
                          filesystem: FileSystem,
                          path: Path,
                          clock: Clock = new SystemClock()): FSDataInputStream = {
    logInfo("gbjf10")
    logInfo("gbj shuffle file open")
    val replicationDelay = conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_REPLICATION_DELAY)
    if (replicationDelay.isDefined) {
      val replicationDeadline = clock.getTimeMillis() + replicationDelay.get * 1000
      val replicationWait = conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_REPLICATION_WAIT)
      val replicationWaitMs = replicationWait * 1000
      try {
        open(filesystem, path, replicationDeadline, replicationWaitMs, clock)
      } catch {
        case fnf: FileNotFoundException =>
          logInfo(f"File not found, exceeded expected replication delay " +
            f"of ${replicationDelay.get}s: $path")
          throw fnf
      }
    } else {
      filesystem.open(path)
    }
  }

  /**
   * Read a ManagedBuffer.
   */
  def read(conf: SparkConf, blockId: BlockId): ManagedBuffer = {
    logInfo("gbjf11")
    logInfo(s"Read $blockId")
    val fallbackPath = new Path(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).get)
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val fallbackFileSystem = FileSystem.get(fallbackPath.toUri, hadoopConf)
    val appId = conf.getAppId

    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw SparkException.internalError(
          s"unexpected shuffle block id format: $blockId", category = "STORAGE")
    }

    val name = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
    val indexFile = getPath(conf, appId, shuffleId, name)
    val start = startReduceId * 8L
    val end = endReduceId * 8L
    Utils.tryWithResource(open(conf, fallbackFileSystem, indexFile)) { inputStream =>
      Utils.tryWithResource(new DataInputStream(inputStream)) { index =>
        index.skip(start)
        val offset = index.readLong()
        index.skip(end - (start + 8L))
        val nextOffset = index.readLong()
        val name = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
        val dataFile = getPath(conf, appId, shuffleId, name)
        val size = nextOffset - offset
        logDebug(s"To byte array $size")
        val array = new Array[Byte](size.toInt)
        val startTimeNs = System.nanoTime()
        Utils.tryWithResource(open(conf, fallbackFileSystem, dataFile)) { f =>
          f.seek(offset)
          f.readFully(array)
          logDebug(s"Took ${(System.nanoTime() - startTimeNs) / (1000 * 1000)}ms")
          logInfo("gbj shuffle fallback read")
        }
        new NioManagedBuffer(ByteBuffer.wrap(array))
      }
    }
  }
}
