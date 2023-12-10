package org.vitrivr.cottontail.math.knn

import org.apache.commons.math3.random.JDKRandomGenerator
import org.junit.jupiter.params.provider.Arguments
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.test.TestConstants
import java.util.stream.Stream

/**
 * Abstract class for test cases that test for correctness of some basic distance calculations.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
abstract class AbstractDistanceTest {
    companion object {
        /** A Random number generator used for the [AbstractDistanceTest]. */
        @JvmStatic
        protected val RANDOM = JDKRandomGenerator()

        /** Random set of dimensions used for generating test vectors. */
        @JvmStatic
        fun dimensions(): Stream<Arguments> = Stream.of(
                Arguments.of(TestConstants.SMALL_VECTOR_MAX_DIMENSION),
                Arguments.of(RANDOM.nextInt(1, TestConstants.SMALL_VECTOR_MAX_DIMENSION)),
                Arguments.of(TestConstants.MEDIUM_VECTOR_MAX_DIMENSION),
                Arguments.of(RANDOM.nextInt(1, TestConstants.MEDIUM_VECTOR_MAX_DIMENSION)),
                Arguments.of(RANDOM.nextInt(1, TestConstants.MEDIUM_VECTOR_MAX_DIMENSION)),
                Arguments.of(TestConstants.LARGE_VECTOR_MAX_DIMENSION),
                Arguments.of(RANDOM.nextInt(1, TestConstants.LARGE_VECTOR_MAX_DIMENSION)),
                Arguments.of(RANDOM.nextInt(1, TestConstants.LARGE_VECTOR_MAX_DIMENSION)),
                Arguments.of(RANDOM.nextInt(1, TestConstants.LARGE_VECTOR_MAX_DIMENSION))
        )

        /** Logger used for the tests. */
        @JvmStatic
        protected val LOGGER = LoggerFactory.getLogger(AbstractDistanceTest::class.java)
    }
}