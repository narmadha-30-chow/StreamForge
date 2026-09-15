# StreamForge - Week 3
# RocksDB Persistent State Store

import json
import os
from rocksdict import Rdict


class StateStore:
    """
    Persistent state store using RocksDB.

    Aggregation state is stored locally so that it can
    be recovered after a worker restart.
    """

    def __init__(self, path="rocksdb_state"):
        self.path = path

        # Create the directory if it does not exist
        os.makedirs(self.path, exist_ok=True)

        # Open RocksDB
        self.db = Rdict(self.path)

    def save(self, key, value):
        """
        Save aggregation state to RocksDB.
        """

        self.db[str(key)] = json.dumps(value)

        # Make sure the latest state is persisted
        self.db.flush()

    def get(self, key):
        """
        Retrieve a state value from RocksDB.
        """

        value = self.db.get(str(key))

        if value is None:
            return None

        return json.loads(value)

    def get_all(self):
        """
        Retrieve all aggregation states.
        """

        states = {}

        for key, value in self.db.items():
            states[str(key)] = json.loads(value)

        return states

    def delete(self, key):
        """
        Delete a state entry.
        """

        key = str(key)

        if key in self.db:
            del self.db[key]
            self.db.flush()

    def close(self):
        """
        Close the RocksDB database.
        """

        self.db.close()
# StreamForge - Week 3
# RocksDB Persistent State Store
#
# Provides local persistent storage for stream-processing state.
# Aggregation windows are saved to RocksDB so that state
# can be recovered after worker restart or failure.