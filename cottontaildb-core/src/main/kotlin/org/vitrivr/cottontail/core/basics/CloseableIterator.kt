package org.vitrivr.cottontail.core.basics

import java.io.Closeable

/**
 * An [Iterator] implementation that is [Closeable].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface CloseableIterator<T>: Closeable, Iterator<T>