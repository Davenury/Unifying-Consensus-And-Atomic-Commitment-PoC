package com.github.davenury.ucac.routing

import com.github.davenury.common.Changes
import com.github.davenury.common.CurrentLeaderDto
import com.github.davenury.common.PeerId
import com.github.davenury.ucac.common.ChangeNotifier
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.consensus.raft.domain.ConsensusElectMe
import com.github.davenury.ucac.consensus.raft.domain.ConsensusHeartbeat
import com.github.davenury.ucac.consensus.raft.domain.ConsensusProposeChange
import com.github.davenury.ucac.consensus.raft.domain.RaftConsensusProtocol
import io.ktor.application.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import kotlinx.coroutines.future.await

fun Application.consensusProtocolRouting(protocol: RaftConsensusProtocol) {
    routing {
        // głosujemy na leadera
        post("/consensus/request_vote") {
            val message: ConsensusElectMe = call.receive()
            val response = protocol.handleRequestVote(message.peerId, message.term, message.lastEntryId)
            call.respond(response)
        }

        post("/consensus/heartbeat") {
            val message: ConsensusHeartbeat = call.receive()
            val heartbeatResult = protocol.handleHeartbeat(message)
            call.respond(heartbeatResult)
        }

        post("/consensus/request_apply_change") {
            val message: ConsensusProposeChange = call.receive()
            val result = protocol.handleProposeChange(message).await()
            call.respond(result)
        }

        get("/consensus/current-leader") {
            call.respond(CurrentLeaderDto(protocol.getLeaderId()))
        }

        get("/consensus/proposed_changes") {
            call.respond(Changes(protocol.getProposedChanges()))
        }

        get("/consensus/accepted_changes") {
            call.respond(Changes(protocol.getAcceptedChanges()))
        }
    }
}

/*

Test case'y:

1. happy-path, wszyscy żyją i jeden zostaje wybrany jako leader
* peer 1 wysyła prośbę o głosowanie na niego
* peer 1 dostaje większość głosów
* peer 1 informuje że jest leaderem
* peer 1 proponuje zmianę (akceptowana)
* peer 2 proponuje zmianę (akceptowana)

weryfikujemy:
* czy każdy peer wie że peer 1 jest leaderem
* czy każdy peer ma poprawną historię

2. dostajemy mniej niż połowę głosów

3. fail leadera

3.5. fail mniej niż połowy

3.75. fail więcej niż połowy

4. podział sieci na dwie i merge
* peer 1 jest leaderem
* następuje podział sieci: [1, 2] [3, 4, 5] - podziały nie mogą się komunikować między sobą
* peer 3 zostaje leaderem nr 2
* peer 1 chce zaaplikować zmianę ale nie może bo nie ma większości
* peer 3 chce zaaplikować zmianę i mu się udaje
* sieć się łączy
* peer 1,2 musi zaciągnąć zmiany z reszty, a peer 1 musi sfailować zmianę


Podział na taski:
1. implementacja 1 testu - Dawid
2. implementacja reszty testów równolegle - Kamil

3. implementacja głosowania na leadera - Dawid
4. implementacja protokołu zakładając że ma się leadera — Radek

 */
