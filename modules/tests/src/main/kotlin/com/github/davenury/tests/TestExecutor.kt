//package com.github.davenury.tests
//
//import com.github.davenury.common.AddUserChange
//import com.github.davenury.common.Change
//import com.github.davenury.common.history.InitialHistoryEntry
//import kotlinx.coroutines.*
//import kotlinx.coroutines.channels.ReceiveChannel
//import kotlinx.coroutines.channels.ticker
//import java.time.Duration
//import java.util.concurrent.atomic.AtomicInteger
//import java.util.concurrent.atomic.AtomicReference
//
///*
//* TODOs
//*  read configuration from file
//*  somehow get peers addresses
//*  generate sequence of changes (remember about parent id)
//*  keep track of which change was committed to which peerset lastly (map peersetId to current parent id)
//*
//* within each request
//*   determine to which peerset/peer send message (do not send two messages to same peerset at the same time, if possible)
//*   send request with valid parent id
//* */
//
//fun main() {
//    val testExecutor = TestExecutor(listOf(listOf()), 1, 1, Duration.ofSeconds(1))
//    testExecutor.startTest()
//}
//
//class TestExecutor(
//    private val peersAddresses: List<List<String>>,
//    private val numberOfRequestsToSendToSinglePeerset: Int,
//    private val numberOfRequestsToSendToMultiplePeersets: Int,
//    private val timeOfSimulation: Duration,
//) {
//    private val overallNumberOfRequests = numberOfRequestsToSendToMultiplePeersets + numberOfRequestsToSendToSinglePeerset
//    private val sendRequestBreak = timeOfSimulation.dividedBy(overallNumberOfRequests.toLong())
//    private val changes = Changes()
//    private val channel: ReceiveChannel<Unit> = ticker(sendRequestBreak.toMillis(), 0)
//    private val channelCounter = AtomicInteger(0)
//
//    fun startTest() = runBlocking {
//        withContext(Dispatchers.IO) {
//            while(channelCounter.get() < overallNumberOfRequests) {
//                channel.receive()
//                launch {
//                    println(changes.getChange().toHistoryEntry())
//                    channelCounter.incrementAndGet()
//                }
//            }
//            channel.cancel()
//        }
//    }
//}
//
//class Changes(
//    private val peersets: Int
//) {
//    val changes = List(peersets) { OnePeersetChanges() }
//}
//
//class OnePeersetChanges {
//    private var parentId = AtomicReference(InitialHistoryEntry.getId())
//    private var counter = AtomicInteger(0)
//
//    fun getChange(peer: String? = null): Change =
//        AddUserChange(parentId.get(), "userName${counter.getAndIncrement()}", peer?.let { listOf(it) } ?: listOf())
//            .also { parentId.set(it.toHistoryEntry().getId()) }
//
//    fun getCurrentParentId(): String = parentId.get()
//}
