package org.vitrivr.cottontail.dbms.index.hnsw

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

class NamedThreadFactory(private val namingPattern: String) : ThreadFactory {
    private val counter: AtomicInteger = AtomicInteger(0)

    override fun newThread(r: Runnable): Thread {
        return Thread(r, String.format(namingPattern, counter.incrementAndGet()))
    }
}