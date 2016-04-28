#!/usr/bin/env python

import unittest

from .context import pyrps

class PyRPSTestCase(unittest.TestCase):
    def setUp(self):
        namespace = "pyrpstestcase"

        self.pyrps = pyrps.PyRPS(namespace)

        # Clean up redis namespace.
        for key in self.pyrps.redis.keys("%s.*" % namespace):
            self.pyrps.redis.delete(key)

    def testConnectionWorking(self):
        self.pyrps.redis.ping()

    def testPublishWithoutReader(self):
        self.pyrps.publish("test", "test message content")

        sub = self.pyrps.subscribe("test", "consumer")
        assert sub.consume(block=False) is None

    def testPublishWithReader(self):
        sub = self.pyrps.subscribe("test", "consumer")

        self.pyrps.publish("test", "test message content")

        assert sub.consume(block=False) == "test message content"



if __name__ == "__main__":
    unittest.main()
