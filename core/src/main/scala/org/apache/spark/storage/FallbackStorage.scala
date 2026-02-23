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

import java.io.{DataInputStream, InputStream}
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import io.netty.buffer.Unpooled
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP, STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP_THREADS, STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP_WAIT_ON_SHUTDOWN, STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH, STORAGE_DECOMMISSION_FALLBACK_STORAGE_PROACTIVE_ENABLED, STORAGE_DECOMMISSION_FALLBACK_STORAGE_PROACTIVE_RELIABLE}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.util.{JavaUtils, LimitedInputStream}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleBlockInfo}
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.BlockManagerMessages.RemoveShuffle
import org.apache.spark.storage.FallbackStorage.asyncCopyExecutionContext
import org.apache.spark.util.{ShutdownHookManager, ThreadUtils, Utils}

/**
 * A fallback storage used by storage decommissioners.
 */
private[storage] class FallbackStorage(
    conf: SparkConf,
    asyncCopies: ConcurrentMap[ShuffleBlockInfo, Future[Unit]]) extends Logging {
  require(conf.contains("spark.app.id"))
  require(FallbackStorage.isConfigured(conf))

  private val fallbackPath = FallbackStorage.getPath(conf)
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  private val fallbackFileSystem = FileSystem.get(fallbackPath.toUri, hadoopConf)
  private val appId = conf.getAppId

  // Visible for testing
  def copy(
      shuffleBlockInfo: ShuffleBlockInfo,
      bm: BlockManager,
      isAsyncCopy: Boolean = false,
      reportBlockStatus: Boolean = true): Unit = {
    val shuffleId = shuffleBlockInfo.shuffleId
    val mapId = shuffleBlockInfo.mapId

    // wait for the ongoing async copy to finish
    if (!isAsyncCopy) {
      Option(asyncCopies.get(shuffleBlockInfo)).foreach { asyncCopy =>
        logInfo("Waiting for the ongoing async copy to finish: $shuffleBlockInfo")
        ThreadUtils.awaitResult(asyncCopy, Duration.Inf)
      }
    }

    // only copy the files if they don't yet exist on the fallback fs
    // they might have been pro-actively copied
    bm.migratableResolver match {
      case r: IndexShuffleBlockResolver =>
        val indexFile = r.getIndexFile(shuffleId, mapId)
        val fallbackIndexFilePath = getFallbackFilePath(shuffleId, indexFile.getName)
        val fallbackIndexFileExists = fallbackFileSystem.exists(fallbackIndexFilePath)
        if (fallbackIndexFileExists || indexFile.exists()) {
          if (!fallbackIndexFileExists) {
            fallbackFileSystem.copyFromLocalFile(
              new Path(Utils.resolveURI(indexFile.getAbsolutePath)),
              fallbackIndexFilePath)
          }

          val dataFile = r.getDataFile(shuffleId, mapId)
          val fallbackDataFilePath = getFallbackFilePath(shuffleId, dataFile.getName)
          val fallbackDataFileExist = fallbackFileSystem.exists(fallbackDataFilePath)
          if (!fallbackDataFileExist && dataFile.exists()) {
            fallbackFileSystem.copyFromLocalFile(
              new Path(Utils.resolveURI(dataFile.getAbsolutePath)),
              fallbackDataFilePath)
          }

          // Report block statuses
          if (reportBlockStatus) {
            val reduceId = NOOP_REDUCE_ID
            val indexBlockId = ShuffleIndexBlockId(shuffleId, mapId, reduceId)
            FallbackStorage.reportBlockStatus(bm, indexBlockId, indexFile.length)
            if (fallbackDataFileExist || dataFile.exists) {
              val dataBlockId = ShuffleDataBlockId(shuffleId, mapId, reduceId)
              FallbackStorage.reportBlockStatus(bm, dataBlockId, dataFile.length)
            }
          }
        }
      case r =>
        logWarning(s"Unsupported Resolver: ${r.getClass.getName}")
    }
  }

  def copyAsync(
      shuffleBlockInfo: ShuffleBlockInfo,
      bm: BlockManager): Unit = {
    asyncCopies.computeIfAbsent(shuffleBlockInfo, _ => Future {
        logInfo(s"Starting copying shuffle block ${shuffleBlockInfo}")
        copy(shuffleBlockInfo, bm, isAsyncCopy = true, reportBlockStatus = false)
        logInfo(s"Finished copying shuffle block ${shuffleBlockInfo}")
      }(asyncCopyExecutionContext)
    ).andThen {
      case _ => asyncCopies.remove(shuffleBlockInfo)
    }(asyncCopyExecutionContext)
  }

  def getFallbackFilePath(shuffleId: Int, filename: String): Path =
    FallbackStorage.getFallbackFilePath(fallbackPath, appId, shuffleId, filename)

  private[storage] def exists(shuffleId: Int, mapId: Long): Boolean = {
    val indexName = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
    val indexFile = getFallbackFilePath(shuffleId, indexName)
    val dataName = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
    val dataFile = getFallbackFilePath(shuffleId, dataName)
    fallbackFileSystem.exists(indexFile) && fallbackFileSystem.exists(dataFile)
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
        FallbackStorage.cleanUpAsync(conf, hadoopConf, Some(shuffleId))
        Future{true.asInstanceOf[T]}
      case _ => Future{true.asInstanceOf[T]}
    }
  }
}

/**
 * Lazily reads a segment of an Hadoop FileSystem file, i.e. when createInputStream is called.
 * @param filesystem hadoop filesystem
 * @param file path of the file
 * @param offset offset of the segment
 * @param length size of the segmetn
 */
private[storage] class FileSystemSegmentManagedBuffer(
    filesystem: FileSystem,
    file: Path,
    offset: Long,
    length: Long) extends ManagedBuffer with Logging {

  override def size(): Long = length

  override def nioByteBuffer(): ByteBuffer = {
    Utils.tryWithResource(createInputStream()) { in =>
      ByteBuffer.wrap(in.readAllBytes())
    }
  }

  override def createInputStream(): InputStream = {
    val startTimeNs = System.nanoTime()
    try {
      val in = filesystem.open(file)
      in.seek(offset)
      new LimitedInputStream(in, length)
    } finally {
      logDebug(s"Took ${(System.nanoTime() - startTimeNs) / (1000 * 1000)}ms")
    }
  }

  override def retain(): ManagedBuffer = this

  override def release(): ManagedBuffer = this

  override def convertToNetty(): AnyRef = {
    Unpooled.wrappedBuffer(nioByteBuffer());
  }
}

private[spark] object FallbackStorage extends Logging {
  /** We use one block manager id as a place holder. */
  val FALLBACK_BLOCK_MANAGER_ID: BlockManagerId = BlockManagerId("fallback", "remote", 7337)
  /** Holds a future for each async copy in progress. Removed by the future on completion. */
  val FALLBACK_ASYNC_COPIES: ConcurrentMap[ShuffleBlockInfo, Future[Unit]] =
    new ConcurrentHashMap[ShuffleBlockInfo, Future[Unit]]()

  def isConfigured(conf: SparkConf): Boolean = {
    conf != null && conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined
  }

  def isProactive(conf: SparkConf): Boolean = {
    isConfigured(conf) &&
      conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PROACTIVE_ENABLED)
  }

  def isReliable(conf: SparkConf): Boolean =
    isProactive(conf) &&
      conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PROACTIVE_RELIABLE)

  def getPath(conf: SparkConf): Path =
    new Path(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).get)

  private val asyncCopyExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("fallback-storage-async-copy", 16))

  /** Shuffle data can be cleaned up asynchronously by adding them to cleanupShufflesQueue. */
  private case class CleanUp(
    conf: SparkConf, hadoopConf: Configuration, shuffleId: Option[Int] = None)

  private val stopped = new AtomicBoolean(false)

  // a daemon thread pool for shuffle cleanups
  private val cleanupShufflesNumThreads =
    SparkEnv.get.conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP_THREADS)
  private val cleanupShufflesThreadPool =
    ThreadUtils.newDaemonFixedThreadPool(cleanupShufflesNumThreads, "fallback-storage-cleanup")
  private val cleanupShufflesExecutionContext =
    ExecutionContext.fromExecutor(cleanupShufflesThreadPool)

  // Ensure cleanup work only blocks Spark shutdown when configured so
  private val cleanupShufflesWaitOnShutdown =
    SparkEnv.get.conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP_WAIT_ON_SHUTDOWN)

  ShutdownHookManager.addShutdownHook { () =>
    // indicate the cleanup thread to terminate once the queue is drained
    stopped.set(true)

    // only wait for cleanups to finish when configured so
    if (cleanupShufflesWaitOnShutdown) {
      cleanupShufflesThreadPool.shutdown()
      while (!cleanupShufflesThreadPool.awaitTermination(1L, TimeUnit.SECONDS)) {}
    } else {
      cleanupShufflesThreadPool.shutdownNow()
    }
  }

  def getFallbackStorage(conf: SparkConf): Option[FallbackStorage] = {
    if (isConfigured(conf)) {
      Some(new FallbackStorage(conf, FALLBACK_ASYNC_COPIES))
    } else {
      None
    }
  }

  /** Register the fallback block manager and its RPC endpoint. */
  def registerBlockManagerIfNeeded(master: BlockManagerMaster,
                                   conf: SparkConf,
                                   hadoopConf: Configuration): Unit = {
    if (isConfigured(conf)) {
      master.registerBlockManager(
        FALLBACK_BLOCK_MANAGER_ID, Array.empty[String], 0, 0,
        new FallbackStorageRpcEndpointRef(conf, hadoopConf))
    }
  }

  /**
   * Asynchronously clean up the generated fallback location for this app (and shuffle id if given).
   */
  def cleanUpAsync(
    conf: SparkConf, hadoopConf: Configuration, shuffleId: Option[Int] = None): Unit = {
    if (stopped.get()) {
      logInfo("Not queueing cleanup due to shutdown")
    } else {
      Future { cleanUp(conf, hadoopConf, shuffleId) }(cleanupShufflesExecutionContext)
    }
  }

  /** Clean up the generated fallback location for this app (and shuffle id if given). */
  def cleanUp(conf: SparkConf, hadoopConf: Configuration, shuffleId: Option[Int] = None): Unit = {
    if (isConfigured(conf) &&
        conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP) &&
        (shuffleId.isDefined ||
          conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP_WAIT_ON_SHUTDOWN)) &&
        conf.contains("spark.app.id")) {
      if (shuffleId.isDefined) {
        logInfo(s"Cleaning up shuffle ${shuffleId.get}")
      } else {
        logInfo(s"Cleaning up app shuffle data")
      }
      val fallbackPath = shuffleId.foldLeft(
        new Path(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).get, conf.getAppId)
      ) { case (path, shuffleId) => new Path(path, shuffleId.toString) }
      val fallbackUri = fallbackPath.toUri
      val fallbackFileSystem = FileSystem.get(fallbackUri, hadoopConf)
      // The fallback directory for this app may not be created yet.
      if (fallbackFileSystem.exists(fallbackPath)) {
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
    assert(blockManager.master != null)
    blockManager.master.updateBlockInfo(
      FALLBACK_BLOCK_MANAGER_ID, blockId, StorageLevel.DISK_ONLY, memSize = 0, dataLength)
  }

  private def getFallbackFilePath(
      fallbackPath: Path,
      appId: String,
      shuffleId: Int,
      filename: String): Path = {
    val hash = JavaUtils.nonNegativeHash(filename)
    new Path(fallbackPath, s"$appId/$shuffleId/$hash/$filename")
  }

  def getShuffleFiles(conf: SparkConf, blockId: BlockId): (FileSystem, Path, Path, Long, Long) = {
    val fallbackPath = FallbackStorage.getPath(conf)
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

    val indexName = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
    val indexFile = getFallbackFilePath(fallbackPath, appId, shuffleId, indexName)
    val dataName = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
    val dataFile = getFallbackFilePath(fallbackPath, appId, shuffleId, dataName)
    val start = startReduceId * 8L
    val end = endReduceId * 8L
    (fallbackFileSystem, indexFile, dataFile, start, end)
  }

  /**
   * Read a block as ManagedBuffer. This reads the index for offset and block size
   * but does not read the actual block data. Those data are later read when calling
   * createInputStream() on the returned ManagedBuffer.
   */
  def read(conf: SparkConf, blockId: BlockId): ManagedBuffer = {
    logInfo(s"Read $blockId")
    val (fallbackFileSystem, indexFile, dataFile, start, end) = getShuffleFiles(conf, blockId)
    Utils.tryWithResource(fallbackFileSystem.open(indexFile)) { inputStream =>
      Utils.tryWithResource(new DataInputStream(inputStream)) { index =>
        index.skip(start)
        val offset = index.readLong()
        index.skip(end - (start + 8L))
        val nextOffset = index.readLong()
        val size = nextOffset - offset
        new FileSystemSegmentManagedBuffer(fallbackFileSystem, dataFile, offset, size)
      }
    }
  }
}
