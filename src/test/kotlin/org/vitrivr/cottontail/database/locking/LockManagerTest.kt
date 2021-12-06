package org.vitrivr.cottontail.database.locking

import org.junit.jupiter.api.*
import java.util.*

/**
 * A UnitTest that test correct behaviour of the [LockManager] and [LockHolder] classes.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LockManagerTest {
    /** The [LockManager] to test. */
    private val lockManager = LockManager<Any>()

    /** Objects to lock on. */
    private val o1 = "o1"
    private val o2 = "o2"
    private val o3 = "o3"


    /** The [LockHolder]s to test. */
    private val tx1 = LockHolder<Any>(1)
    private val tx2 = LockHolder<Any>(2)
    private val random = SplittableRandom()

    /**
     * Checks the schedule for two concurrently executed transactions T1, T2
     */
    @RepeatedTest(10)
    fun testScheduleWithTwoTx() {
        val schedule = Collections.synchronizedList(mutableListOf<String>())
        val t1 =Thread { /* T1 */
            Thread.sleep(random.nextLong(1000))
            this.lockManager.lock(tx1, o1, LockMode.EXCLUSIVE)
            schedule.add("begin($tx1)")
            schedule.add("w($tx1,$o1)")
            Thread.sleep(random.nextLong(1000))

            this.lockManager.lock(tx1, o2, LockMode.EXCLUSIVE)
            schedule.add("w($tx1,$o2)")
            Thread.sleep(random.nextLong(1000))

            this.lockManager.lock(tx1, o3, LockMode.EXCLUSIVE)
            schedule.add("w($tx1,$o3)")
            Thread.sleep(random.nextLong(1000))

            /* Commit. */
            schedule.add("commit($tx1)")
            this.lockManager.unlock(tx1, o3)
            this.lockManager.unlock(tx1, o2)
            this.lockManager.unlock(tx1, o1)
        }

       val t2 = Thread { /* T2 */
            Thread.sleep(random.nextLong(1000))
            this.lockManager.lock(tx2, o1, LockMode.SHARED)
            schedule.add("begin($tx2)")
            schedule.add("r($tx2,$o1)")
            Thread.sleep(random.nextLong(1000))

            this.lockManager.lock(tx2, o3, LockMode.SHARED)
            schedule.add("r($tx2,$o3)")
            Thread.sleep(random.nextLong(1000))

            schedule.add("commit($tx2)")
            this.lockManager.unlock(tx2, o1)
            this.lockManager.unlock(tx2, o3)
       }

        /* Start transactions. */
        t1.start()
        t2.start()

        /* Wait for execution to finish. */
        t1.join()
        t2.join()

        /* Check schedule. */
        println("Schedule: ${schedule.joinToString(",")}")
        if (schedule[0] == "begin($tx1)") {
            Assertions.assertEquals("begin($tx1)", schedule[0])
            Assertions.assertEquals("w($tx1,$o1)", schedule[1])
            Assertions.assertEquals("w($tx1,$o2)", schedule[2])
            Assertions.assertEquals("w($tx1,$o3)", schedule[3])
            Assertions.assertEquals("commit($tx1)", schedule[4])
            Assertions.assertEquals("begin($tx2)", schedule[5])
            Assertions.assertEquals("r($tx2,$o1)", schedule[6])
            Assertions.assertEquals("r($tx2,$o3)", schedule[7])
            Assertions.assertEquals("commit($tx2)", schedule[8])
        } else if (schedule[0] == "begin($tx2)") {
            Assertions.assertEquals("begin($tx2)", schedule[0])
            Assertions.assertEquals("r($tx2,$o1)", schedule[1])
            Assertions.assertEquals("r($tx2,$o3)", schedule[2])
            Assertions.assertEquals("commit($tx2)", schedule[3])
            Assertions.assertEquals("begin($tx1)", schedule[4])
            Assertions.assertEquals("w($tx1,$o1)", schedule[5])
            Assertions.assertEquals("w($tx1,$o2)", schedule[6])
            Assertions.assertEquals("w($tx1,$o3)", schedule[7])
            Assertions.assertEquals("commit($tx1)", schedule[8])
        } else {
            Assertions.fail("Invalid schedule!")
        }
    }

    /**
     * Provokes a deadlock situation and tests for the respective exception to be thrown.
     */
    @Test
    fun testDeadlock() {
        var exc: DeadlockException? = null
        val t1 = Thread {
            try {
                println("w($tx1, $o1)")
                this.lockManager.lock(tx1, o1, LockMode.EXCLUSIVE)
                println("w($tx1, $o2)")
                this.lockManager.lock(tx1, o2, LockMode.EXCLUSIVE)
                println("w($tx1, $o3)")
                this.lockManager.lock(tx1, o3, LockMode.EXCLUSIVE)
            } catch (e: DeadlockException) {
                exc = e
            }
        }
        val t2 = Thread {
            try {
                println("w($tx2, $o3)")
                this.lockManager.lock(tx2, o3, LockMode.EXCLUSIVE)
                println("w($tx2, $o2)")
                this.lockManager.lock(tx2, o2, LockMode.EXCLUSIVE)
                println("w($tx2, $o1)")
                this.lockManager.lock(tx2, o1, LockMode.EXCLUSIVE)
            } catch (e: DeadlockException) {
                exc = e
            }
        }

        /* Start transactions. */
        t1.start()
        t2.start()

        /* Wait for execution to finish. */
        t1.join()
        t2.join()

        /* Check for exception. */
        Assertions.assertTrue(exc is DeadlockException)
    }
}