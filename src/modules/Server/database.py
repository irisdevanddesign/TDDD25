# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 24 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Implementation of a simple database class."""

import random


class Database(object):

    """Class containing a database implementation."""

    def __init__(self, db_file):
        self.db_file = db_file
        self.rand = random.Random()
        self.rand.seed()

        file = open(self.db_file)
        self.data = file.read().split('"\n" + "%" + "\n"')
        del self.data[len(self.data)-1]

    def read(self):
        """Read a random fortune in the database."""
        if not len(self.data):
            return

        randomFortune = self.rand.randint(0, len(self.data)-1)
        return self.data[randomFortune]

    def write(self, fortune):
        """Write a new fortune to the database."""

        self.data.append(fortune)
        with open(self.db_file, "a") as file:
            file.write(fortune + "\n%\n")

        return
