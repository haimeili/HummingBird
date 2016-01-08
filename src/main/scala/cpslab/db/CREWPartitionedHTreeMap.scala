package cpslab.db

import java.util.concurrent.ExecutorService

import com.typesafe.config.Config

class CREWPartitionedHTreeMap[K, V](conf: Config,
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

  
}
