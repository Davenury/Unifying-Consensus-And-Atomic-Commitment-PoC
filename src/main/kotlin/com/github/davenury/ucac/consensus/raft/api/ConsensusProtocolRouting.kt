package com.github.davenury.ucac.consensus.raft.api

import com.github.davenury.ucac.consensus.raft.domain.ConsensusElectMe
import com.github.davenury.ucac.consensus.raft.domain.ConsensusImTheLeader
import com.github.davenury.ucac.consensus.raft.domain.RaftConsensusProtocol
import io.ktor.application.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*


fun Application.consensusProtocolRouting(protocol: RaftConsensusProtocol) {

    routing {
        // głosujemy na leadera
        post("/consensus/request_vote") {
            val message: ConsensusElectMe = call.receive()
            call.respond(protocol.handleRequestVote(message))
        }

        // potwierdzenie że mamy leadera
        post("/consensus/leader") {
            val message: ConsensusImTheLeader = call.receive()
            protocol.handleLeaderElected(message)
            call.respond("OK")
        }

        post("/consensus/heartbeat") {
            // TODO
        }

        // leader proponuje zmianę bez pewności czy zostanie zaaplikowana
        post("/consensus/propose_change") {
            // TODO
        }

        // leader ma potwierdzenie że większość zaakceptowała zmianę i potwierdza reszcie
        post("/consensus/apply_change") {
            // TODO
        }

        // kiedy nie jesteś leaderem to prosisz leadera o zmianę
        post("/consensus/request_apply_change") {
            // TODO
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
