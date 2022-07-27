package github.davenury.ucac.consensus.ratis

import java.io.*
import java.nio.charset.Charset
import java.util.concurrent.CompletableFuture
import org.apache.ratis.proto.RaftProtos
import org.apache.ratis.protocol.Message
import org.apache.ratis.protocol.RaftGroupId
import org.apache.ratis.server.RaftServer
import org.apache.ratis.server.protocol.TermIndex
import org.apache.ratis.server.raftlog.RaftLog
import org.apache.ratis.server.storage.RaftStorage
import org.apache.ratis.statemachine.TransactionContext
import org.apache.ratis.statemachine.impl.BaseStateMachine
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo
import org.apache.ratis.util.JavaUtils

abstract class StateMachine<A>(open var state: A) : BaseStateMachine() {
    private val storage: SimpleStateMachineStorage = SimpleStateMachineStorage()

    /**
     * initialize the state machine by initilize the state machine storage and calling the load
     * method which reads the last applied command and restore it in counter object)
     *
     * @param server the current server information
     * @param groupId the cluster groupId
     * @param raftStorage the raft storage which is used to keep raft related stuff
     * @throws IOException if any error happens during load state
     */
    @Throws(IOException::class)
    override fun initialize(server: RaftServer?, groupId: RaftGroupId?, raftStorage: RaftStorage?) {
        super.initialize(server, groupId, raftStorage)
        storage.init(raftStorage)
        load(storage.latestSnapshot)
    }

    /**
     * very similar to initialize method, but doesn't initialize the storage system because the
     * state machine reinitialized from the PAUSE state and storage system initialized before.
     *
     * @throws IOException if any error happens during load state
     */
    @Throws(IOException::class)
    override fun reinitialize() {
        load(storage.latestSnapshot)
    }

    /**
     * Store the current state as an snapshot file in the stateMachineStorage.
     *
     * @return the index of the snapshot
     */
    override fun takeSnapshot(): Long {
        // get the last applied index
        val last: TermIndex = getLastAppliedTermIndex()

        // create a file with a proper name to store the snapshot
        val snapshotFile: File = storage.getSnapshotFile(last.term, last.index)

        // serialize the counter object and write it into the snapshot file
        try {
            ObjectOutputStream(BufferedOutputStream(FileOutputStream(snapshotFile))).use { out ->
                out.writeObject(serializeState())
            }
        } catch (ioe: IOException) {
            LOG.warn(
                    "Failed to write snapshot file \"" +
                            snapshotFile +
                            "\", last applied index=" +
                            last
            )
        }

        // return the index of the stored snapshot (which is the last applied one)
        return last.index
    }

    /**
     * Load the state of the state machine from the storage.
     *
     * @param snapshot to load
     * @return the index of the snapshot or -1 if snapshot is invalid
     * @throws IOException if any error happens during read from storage
     */
    @Throws(IOException::class)
    private fun load(snapshot: SingleFileSnapshotInfo?): Long {
        // check the snapshot nullity
        if (snapshot == null) {
            LOG.warn("The snapshot info is null.")
            return RaftLog.INVALID_LOG_INDEX
        }

        // check the existance of the snapshot file
        val snapshotFile: File = snapshot.file.path.toFile()
        if (!snapshotFile.exists()) {
            LOG.warn("The snapshot file {} does not exist for snapshot {}", snapshotFile, snapshot)
            return RaftLog.INVALID_LOG_INDEX
        }

        // load the TermIndex object for the snapshot using the file name pattern of
        // the snapshot
        val last: TermIndex = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile)

        // read the file and cast it to the AtomicInteger and set the counter
        try {
            ObjectInputStream(BufferedInputStream(FileInputStream(snapshotFile))).use { `in` ->
                // set the last applied termIndex to the termIndex of the snapshot
                lastAppliedTermIndex = last

                // read, cast and set the counter
                val newState = JavaUtils.cast<A>(`in`.readObject())
                loadState(newState)
            }
        } catch (e: ClassNotFoundException) {
            println("Casting error\n\n")
            throw IllegalStateException(e)
        }
        return last.index
    }

    /**
     * Handle GET command, which used by clients to get the counter value.
     *
     * @param request the GET message
     * @return the Message containing the current counter value
     */
    override fun query(request: Message): CompletableFuture<Message> {
        val msg: String = request.content.toString(Charset.defaultCharset())
        return queryOperation(msg).let { Message.valueOf(it) }.let {
            CompletableFuture.completedFuture(it)
        }
    }

    /**
     * Apply the INCREMENT command by incrementing the counter object.
     *
     * @param trx the transaction context
     * @return the message containing the updated counter value
     */
    override fun applyTransaction(trx: TransactionContext): CompletableFuture<Message> {
        val entry: RaftProtos.LogEntryProto = trx.logEntry

        // check if the command is valid
        val logData: String = entry.stateMachineLogEntry.logData.toString(Charset.defaultCharset())
        val operationError = applyOperation(logData)
        if (operationError != null) {
            return CompletableFuture.completedFuture(Message.valueOf(operationError))
        }
        // update the last applied term and index
        val index: Long = entry.index
        updateLastAppliedTermIndex(entry.term, index)

        // return the new value of the counter to the client
        val f: CompletableFuture<Message> =
                CompletableFuture.completedFuture(Message.valueOf(serializeState()))

        // if leader, log the incremented value and it's log index
        if (trx.serverRole === RaftProtos.RaftPeerRole.LEADER) {
            LOG.info("{}: state changed {}", index, toStringState())
        }
        return f
    }

    abstract fun serializeState(): String

    fun toStringState(): String = state.toString()

    fun loadState(newState: A) {
        state = newState
    }

    abstract fun applyOperation(operation: String): String?

    abstract fun queryOperation(operation: String): String
}
