from typing import List, Dict, Tuple
import random
import math
import numpy as np

NUMBER_OF_PEERS = 50
NUMBER_OF_PEERSETS = 35

MINIMAL_NUMBER_OF_PEERS_IN_PEERSETS = 3
MAXIMAL_NUMBER_OF_PEERS_IN_PEERSETS = 6

# The less the value, the further peers are allowed to be in peerset, cannot be more than 1
LIKELINESS_SKEW = 0.2


class PeersetsGenerator:
    def __init__(self):
        self.peers = self._create_peers()
        self.map = self.generate_random_map()
        self.peersets = self.generate_peersets()


    def get_peersets(self):
        return self.peersets

    def get_peersets_in_string(self):
        return self.join_peersets_to_strings()

    def generate_peersets(self) -> List[List[str]]:
        return [self._generate_single_peerset() for _ in range(NUMBER_OF_PEERSETS)]

    def generate_random_map(self) -> Dict[str, Tuple[int, int]]:
        return {peer: self._get_random_coordinates() for peer in self.peers}

    def join_peersets_to_strings(self):
        elements = [f"peerset{i}={','.join(peers)}" for i, peers in enumerate(self.peersets)]
        return ";".join(elements)

    def _create_peers(self) -> List[str]:
        return [f"peer{i}" for i in range(NUMBER_OF_PEERS)]

    def _generate_single_peerset(self) -> Dict[str, float]:
        coords = self._get_random_coordinates()
        distances = {peer: self._get_distance(peer_coords, coords) for peer, peer_coords in self.map.items()}
        distances = self._sort_distances(distances)

        number_of_peers_in_peerset = self._get_number_of_peers_in_peerset()

        chosen_peers = []

        for i in range(number_of_peers_in_peerset):
            random_value = abs(np.random.normal(loc=0, scale=(1 - LIKELINESS_SKEW)))
            index = min(int(min(random_value, 1) * (NUMBER_OF_PEERS - i)), NUMBER_OF_PEERS - 1 - i)
            chosen_peers.append(distances[index][0])
            del distances[index]

        return chosen_peers

    def _get_number_of_peers_in_peerset(self) -> int:
        return random.randint(MINIMAL_NUMBER_OF_PEERS_IN_PEERSETS, MAXIMAL_NUMBER_OF_PEERS_IN_PEERSETS)

    @staticmethod
    def _get_distance(peer_coords: Tuple[int, int], peerset_point_coords: Tuple[int, int]) -> float:
        return math.sqrt(math.pow(peer_coords[0] - peerset_point_coords[0], 2) + math.pow(peer_coords[1] - peerset_point_coords[1], 2))

    @staticmethod
    def _sort_distances(distances: Dict[str, float]) -> Tuple[str, float]:
        # Note - works only with python 3.7+
        return sorted(distances.items(), key=lambda item: item[1])

    @staticmethod
    def _get_random_coordinates():
        return random.randint(0, 50), random.randint(0, 50)


if __name__ == "__main__":
    generator = PeersetsGenerator()
    print(generator.get_peersets_in_string())
