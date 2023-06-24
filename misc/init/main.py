import requests
import os
import time


def is_consensus_leader_present(uri, peerset):
    try:
        print(f"Asking {uri} for consensus leader in {peerset}")
        response = requests.get(f"http://{uri}/peerset-information?peerset={peerset}").json()
        print(f"Response: {response}")
        return response["currentConsensusLeaderId"] is not None
    except Exception:
        return False


def get_peers(peers, peersets):
    peersets = peersets.split(';')
    peersets = {peers.split('=')[0]: peers.split("=")[1].split(',') for peers in peersets}

    peers = peers.split(';')
    peers = {peer.split('=')[0]: peer.split('=')[1] for peer in peers}

    return peers, peersets


def wait():
    peers = os.environ["TEST_PEERS"]
    peersets = os.environ["TEST_PEERSETS"]

    peers, peersets = get_peers(peers, peersets)

    present_leaders = {}

    while len(present_leaders.items()) < len(peersets.items()):
        for peerset, peers_in_peerset in peersets.items():
            if is_consensus_leader_present(peers[peers_in_peerset[0]], peerset):
                present_leaders[peerset] = True
        time.sleep(0.5)

if __name__ == "__main__":
    wait()