package com.mwrobel.kafkastreams.example6.utils

object CloseableResource {
  def apply[T <: { def close() }, U](resource: T)(block: T => U): U = {
    try {
      block(resource)
    } finally {
      if (resource != null) resource.close()
    }
  }
}
