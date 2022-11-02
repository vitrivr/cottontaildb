package org.vitrivr.cottontail.test

import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import org.vitrivr.cottontail.client.SimpleClient
import java.util.concurrent.TimeUnit
import kotlin.time.ExperimentalTime

/**
 * Provides a client with which to connect to the cottontail server, and some convenience-methods which are shared across tests
 */
@ExperimentalTime
abstract class AbstractClientTest {

    /** The [EmbeddedCottontailGrpcServer] used for this unit test. */
    private val embedded = EmbeddedCottontailGrpcServer(TestConstants.testConfig())

    /** The [ManagedChannel] used for this unit test. */
    private val channel: ManagedChannel = NettyChannelBuilder.forAddress("localhost", 1865).usePlaintext().build()

    /** The [SimpleClient] used for this unit test. */
    var client: SimpleClient = SimpleClient(this.channel)

    fun startAndPopulateCottontail() {
        this.cleanAndConnect()
        GrpcTestUtils.createTestSchema(client)
        GrpcTestUtils.createTestVectorEntity(client)
        GrpcTestUtils.createTestEntity(client)
        GrpcTestUtils.populateTestEntity(client)
        GrpcTestUtils.populateVectorEntity(client)
        assert(client.ping())
    }

    fun cleanAndConnect() {
        assert(client.ping())
        GrpcTestUtils.dropTestSchema(client)
        assert(client.ping())
    }

    fun cleanup() {
        GrpcTestUtils.dropTestSchema(this.client)

        /* Shutdown ManagedChannel. */
        this.channel.shutdown()
        this.channel.awaitTermination(5000, TimeUnit.MILLISECONDS)

        /* Stop embedded server. */
        this.embedded.shutdownAndWait()
    }

}