# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

import threading
import socket
import time
import json
"""Object Request Broker

This module implements the infrastructure needed to transparently create
objects that communicate via networks. This infrastructure consists of:

--  Strub ::
        Represents the image of a remote object on the local machine.
        Used to connect to remote objects. Also called Proxy.
--  Skeleton ::
        Used to listen to incoming connections and forward them to the
        main object.
--  Peer ::
        Class that implements basic bidirectional (Stub/Skeleton)
        communication. Any object wishing to transparently interact with
        remote objects should extend this class.
"""


def create_request(method, args):
    if not args:
        return json.dumps({"method": method, "args": []})
    else:
        return json.dumps({"method": method, "args": [args]})


def read_response(response):
        try:
            result = json.loads(response)
            if "result" in result:
                return result["result"]
            elif "error" in result:
                exception = type(result["error"]["name"], (Exception, ), {})
                raise exception(result["error"]["args"])
            else:
                raise CommunicationError("ProtocolError", ["Protocol not followed"])
        except CommunicationError as e:
            print(e)


def process_request(owner, request):
    message = json.loads(request)

    try:
        method = getattr(owner, message["method"])
        result = method(*message["args"])
        return ''.join((json.dumps({'result': result}), '\n'))

    except AttributeError as e:
        return ''.join(result, '\n')


class CommunicationError(Exception):
    """ Class for throwing CommunicationErrors related to protocol or unknown errors."""

    def __init__(self, type, args):
        self.type = type
        self.args = args

    def __str__(self):
        return self.type


class Stub(object):

    """ Stub for generic objects distributed over the network.

    This is  wrapper object for a socket.

    """

    def __init__(self, address):
        self.address = tuple(address)

    def _rmi(self, method, *args):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.address)

        request = ''.join((json.dumps({'method': method, 'args': args}), '\n'))

        worker = sock.makefile(mode="rw")
        worker.write(request)
        worker.flush()
        response = worker.readline()
        sock.close()
        return read_response(response)

    def __getattr__(self, attr):
        """Forward call to name over the network at the given address."""
        def rmi_call(*args):
            return self._rmi(attr, *args)
        return rmi_call


class Request(threading.Thread):

    """Run the incoming requests on the owner object of the skeleton."""

    def __init__(self, owner, conn, addr):
        threading.Thread.__init__(self)
        self.addr = addr
        self.conn = conn
        self.owner = owner
        self.daemon = True

    def run(self):
        worker = self.conn.makefile(mode="rw")
        request = worker.readline()
        result = process_request(self.owner, request)
        worker.write(result)
        worker.flush()


class Skeleton(threading.Thread):

    """ Skeleton class for a generic owner.

    This is used to listen to an address of the network, manage incoming
    connections and forward calls to the generic owner class.

    """

    def __init__(self, owner, address):
        threading.Thread.__init__(self)
        self.address = address
        self.owner = owner
        self.daemon = True

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(self.address)
        self.server_socket.listen(1)

    def run(self):
        while True:
            try:
                conn, addr = self.server_socket.accept()
                request = Request(self.owner, conn, addr)
                print("Serving request from {0}".format(addr))
                request.start()
            except socket.error:
                continue


class Peer:

    """Class, extended by objects that communicate over the network."""

    def __init__(self, l_address, ns_address, ptype):
        self.type = ptype
        self.hash = ""
        self.id = -1
        self.address = self._get_external_interface(l_address)
        self.skeleton = Skeleton(self, self.address)
        self.name_service_address = self._get_external_interface(ns_address)
        self.name_service = Stub(self.name_service_address)

    # Private methods

    def _get_external_interface(self, address):
        """ Determine the external interface associated with a host name.

        This function translates the machine's host name into its the
        machine's external address, not into '127.0.0.1'.

        """

        addr_name = address[0]
        if addr_name != "":
            addrs = socket.gethostbyname_ex(addr_name)[2]
            if len(addrs) == 0:
                raise CommunicationError("Invalid address to listen to")
            elif len(addrs) == 1:
                addr_name = addrs[0]
            else:
                al = [a for a in addrs if a != "127.0.0.1"]
                addr_name = al[0]
        addr = list(address)
        addr[0] = addr_name
        return tuple(addr)

    # Public methods

    def start(self):
        """Start the communication interface."""

        self.skeleton.start()
        self.id, self.hash = self.name_service.register(self.type,
                                                        self.address)

    def destroy(self):
        """Unregister the object before removal."""

        self.name_service.unregister(self.id, self.type, self.hash)

    def check(self):
        """Checking to see if the object is still alive."""

        return (self.id, self.type)
