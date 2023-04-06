from map import PeersetsGenerator
import unittest

class PeersetGeneratorSpec(unittest.TestCase):

    def test_distance_calculation_the_same_point(self):
        generator = PeersetsGenerator()
        calculated = generator._get_distance((3, 4), (3, 4))
        self.assertEqual(calculated, 0, "Should be 0")

    def test_distance_calculation(self):
        generator = PeersetsGenerator()
        calculated = generator._get_distance((0, 0), (3, 4))
        self.assertEqual(calculated, 5)

    def test_sort_distances(self):
        distances = {
            "peer0": 30,
            "peer1": 25,
            "peer2": 68
        }

        generator = PeersetsGenerator()
        calculated = generator._sort_distances(distances)
        self.assertEqual(calculated, [("peer1", 25), ("peer0", 30), ("peer2", 68)])

if __name__ == "__main__":
    unittest.main()
