package cpslab.db

import java.util.concurrent.ExecutorService

import com.typesafe.config.Config

class EREWParititonedHTreeMap[K, V](conf: Config,
                                    tableId: Int,
                                    hasherName: String,
                                    workingDirectory: String,
                                    name: String,
                                    partitioner: Partitioner[K],
                                    closeEngine: Boolean,
                                    hashSalt: Int,
                                    keySerializer: Serializer[K],
                                    valueSerializer: Serializer[V],
                                    valueCreator: Fun.Function1[V, K],
                                    executor: ExecutorService,
                                    closeExecutor: Boolean,
                                    ramThreshold: Long)
  extends PartitionedHTreeMap[K, V](
    tableId,
    hasherName,
    workingDirectory,
    name,
    partitioner,
    closeEngine,
    hashSalt,
    keySerializer,
    valueSerializer,
    valueCreator,
    executor,
    closeExecutor,
    ramThreshold) {

    //1. set cpu core number
    //2. start corresponding thread and its message queue
    //3. start threads which continuously read the query target from message queue

    //4. for each thread, count start time for the first event and the end of the final event to
    //measure throughput


  }
