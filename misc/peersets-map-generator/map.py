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
    def __init__(self,
                 number_of_peers=NUMBER_OF_PEERS,
                 number_of_peersets=NUMBER_OF_PEERSETS,
                 minimal_number_of_peers_in_peersets=MINIMAL_NUMBER_OF_PEERS_IN_PEERSETS,
                 maximal_number_of_peers_in_peersets=MAXIMAL_NUMBER_OF_PEERS_IN_PEERSETS,
                 skew=LIKELINESS_SKEW
                 ):
        self.number_of_peers = number_of_peers
        self.number_of_peersets = number_of_peersets
        self.minimal = minimal_number_of_peers_in_peersets
        self.maximal = maximal_number_of_peers_in_peersets
        self.skew = 1 - skew
        self.peers = self._create_peers()
        self.map = self.generate_random_map()
        self.peersets = self.generate_peersets()

    def get_peersets(self):
        return self.peersets

    def get_peersets_in_string(self):
        return self.join_peersets_to_strings()

    def generate_peersets(self) -> Dict[str, List[str]]:
        return {f"peerset{i}": self._generate_single_peerset() for i in range(self.number_of_peersets)}

    def generate_random_map(self) -> Dict[str, Tuple[int, int]]:
        return {peer: self._get_random_coordinates() for peer in self.peers}

    def join_peersets_to_strings(self):
        elements = [f"{peerset_id}={','.join(peers)}" for peerset_id, peers in self.peersets.items()]
        return ";".join(elements)

    def _create_peers(self) -> List[str]:
        return [f"peer{i}" for i in range(self.number_of_peers)]

    def _generate_single_peerset(self) -> List[str]:
        coords = self._get_random_coordinates()
        distances = {peer: self._get_distance(peer_coords, coords) for peer, peer_coords in self.map.items()}
        distances = self._sort_distances(distances)

        number_of_peers_in_peerset = self._get_number_of_peers_in_peerset()

        chosen_peers = []

        for i in range(number_of_peers_in_peerset):
            random_value = abs(np.random.normal(loc=0, scale=self.skew))
            current_list_size = self.number_of_peers - 1 -i
            calculated_index = int(random_value * (self.number_of_peers - i))
            correct_index = min(calculated_index, current_list_size)
            chosen_peers.append(distances[correct_index][0])
            del distances[correct_index]

        return chosen_peers

    def _get_number_of_peers_in_peerset(self) -> int:
        return random.randint(self.minimal, self.maximal)

    @staticmethod
    def _get_distance(peer_coords: Tuple[int, int], peerset_point_coords: Tuple[int, int]) -> float:
        return math.sqrt(
            math.pow(peer_coords[0] - peerset_point_coords[0], 2) + math.pow(peer_coords[1] - peerset_point_coords[1],
                                                                             2))

    @staticmethod
    def _sort_distances(distances: Dict[str, float]) -> Tuple[str, float]:
        # Note - works only with python 3.7+
        return sorted(distances.items(), key=lambda item: item[1])

    @staticmethod
    def _get_random_coordinates():
        return random.random(), random.random()


if __name__ == "__main__":
    generator = PeersetsGenerator()
    print(generator.get_peersets_in_string())
