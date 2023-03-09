#!/usr/bin/env python3
import asyncio
import pathlib
import socket
import sys
import time

import nacl.signing

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from cluster import start_cluster
from peer import connect, create_peer_request, run_handshake

import utils


@pytest.mark.flaky(reruns=0)
@pytest.mark.timeout(2000)
@pytest.mark.asyncio
async def test_peers_requests():
    """
    PeersRequest

    Start one real node. Create a connection (conn0) to real node, send PeersRequest and wait for the response.
    Create a new connection (conn1) to real node, send PeersRequest and wait for the response. In the latter
    response there must exist an entry with information from the first connection that was established.
    """
    test_name = utils.get_test_name()
    nodes = start_cluster(1, 0, 4, None, [], {}, prefix=test_name)
    key_pair_0 = nacl.signing.SigningKey.generate()
    conn0 = await connect(nodes[0].addr())
    await run_handshake(conn0,
                        nodes[0].node_key.pk,
                        key_pair_0,
                        listen_port=12345)
    peer_request = create_peer_request()
    await conn0.send(peer_request)
    response = await conn0.recv('PeersResponse')
    assert response.enum == 'PeersResponse', utils.obj_to_string(response)

    key_pair_1 = nacl.signing.SigningKey.generate()
    conn1 = await connect(nodes[0].addr())
    await run_handshake(conn1,
                        nodes[0].node_key.pk,
                        key_pair_1,
                        listen_port=12346)
    peer_request = create_peer_request()
    await conn1.send(peer_request)
    response = await conn1.recv('PeersResponse')

    assert response.enum == 'PeersResponse', utils.obj_to_string(response)
    assert any(
        peer_info.addr.V4[1] == 12345
        for peer_info in response.PeersResponse), utils.obj_to_string(response)
