#!/usr/bin/env python3
# Patch contract states in a sandbox node

import base64
import datetime
import pathlib
import sys
import time

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from transaction import sign_deploy_contract_tx, sign_function_call_tx

import utils

from . import test_data

NODE_CONFIG = utils.figure_out_sandbox_binary()
MIN_BLOCK_PROD_TIME = 1  # seconds
MAX_BLOCK_PROD_TIME = 2  # seconds
EPOCH_LENGTH = 100
BLOCKS_TO_FASTFORWARD = 4 * EPOCH_LENGTH


@pytest.mark.flaky(reruns=1)
def test_patch_state():
    test_name = utils.get_test_name()
    account_id = f"{test_name}0"
    # start node
    nodes = start_cluster(1,
                          0,
                          1,
                          NODE_CONFIG, [["epoch_length", 10]], {},
                          prefix=test_name)

    # deploy contract
    hash_ = nodes[0].get_latest_block().hash_bytes
    tx = sign_deploy_contract_tx(nodes[0].signer_key,
                                 utils.load_test_contract(), 10, hash_)
    nodes[0].send_tx(tx)
    time.sleep(3)

    # store a key value
    hash_ = nodes[0].get_latest_block().hash_bytes
    k = (10).to_bytes(8, byteorder="little")
    v = (20).to_bytes(8, byteorder="little")
    tx2 = sign_function_call_tx(nodes[0].signer_key,
                                nodes[0].signer_key.account_id,
                                'write_key_value', k + v, 1000000000000, 0, 20,
                                hash_)
    res = nodes[0].send_tx_and_wait(tx2, 20)
    assert ('SuccessValue' in res['result']['status'])
    res = nodes[0].call_function(account_id,
                                 "read_value",
                                 base64.b64encode(k).decode('ascii'),
                                 timeout=5)

    assert (res['result']['result'] == list(v))

    # patch it
    new_v = (30).to_bytes(8, byteorder="little")
    res = nodes[0].json_rpc('sandbox_patch_state',
                            test_data.get_patch_data(account_id, k, new_v))

    # patch should succeed
    res = nodes[0].call_function(account_id,
                                 "read_value",
                                 base64.b64encode(k).decode('ascii'),
                                 timeout=5)
    assert res['result']['result'] == list(new_v)


@pytest.mark.flaky(reruns=1)
def test_fast_forward():
    test_name = utils.get_test_name()
    node_config = NODE_CONFIG.copy()
    node_config.update({
        "consensus": {
            "min_block_production_delay": {
                "secs": MIN_BLOCK_PROD_TIME,
                "nanos": 0,
            },
            "max_block_production_delay": {
                "secs": MAX_BLOCK_PROD_TIME,
                "nanos": 0,
            },
        }
    })

    nodes = start_cluster(1,
                          0,
                          1,
                          node_config, [["epoch_length", EPOCH_LENGTH]], {},
                          prefix=test_name)
    sync_info = nodes[0].get_status()['sync_info']
    pre_forward_block_hash = sync_info['latest_block_hash']

    # request to fast forward
    nodes[0].json_rpc('sandbox_fast_forward',
                      {"delta_height": BLOCKS_TO_FASTFORWARD},
                      timeout=60)

    # wait a little for it to fast forward
    # if this call times out, then the fast_forward failed somewhere
    utils.wait_for_blocks(nodes[0],
                          target=BLOCKS_TO_FASTFORWARD + 10,
                          timeout=10)

    # Assert that we're within the bounds of fast forward timestamp between range of min and max:
    sync_info = nodes[0].get_status()['sync_info']
    earliest = datetime.datetime.strptime(sync_info['earliest_block_time'][:-4],
                                          '%Y-%m-%dT%H:%M:%S.%f')
    latest = datetime.datetime.strptime(sync_info['latest_block_time'][:-4],
                                        '%Y-%m-%dT%H:%M:%S.%f')

    min_forwarded_secs = datetime.timedelta(
        0, BLOCKS_TO_FASTFORWARD * MIN_BLOCK_PROD_TIME)
    max_forwarded_secs = datetime.timedelta(
        0, BLOCKS_TO_FASTFORWARD * MAX_BLOCK_PROD_TIME)
    min_forwarded_time = earliest + min_forwarded_secs
    max_forwarded_time = earliest + max_forwarded_secs

    assert min_forwarded_time < latest < max_forwarded_time

    # Check to see that the epoch height has been updated correctly:
    epoch_height = nodes[0].get_validators()['result']['epoch_height']
    assert epoch_height == BLOCKS_TO_FASTFORWARD // EPOCH_LENGTH

    # Check if queries aren't failing after fast forwarding:
    resp = nodes[0].json_rpc("block", {"finality": "optimistic"})
    assert resp['result']['chunks'][0]['height_created'] > BLOCKS_TO_FASTFORWARD
    resp = nodes[0].json_rpc("block", {"finality": "final"})
    assert resp['result']['chunks'][0]['height_created'] > BLOCKS_TO_FASTFORWARD

    # Not necessarily a requirement, but current implementation should be able to retrieve
    # one of the blocks before fast-forwarding:
    resp = nodes[0].json_rpc("block", {"block_id": pre_forward_block_hash})
    assert resp['result']['chunks'][0]['height_created'] < BLOCKS_TO_FASTFORWARD

    # do one more fast forward request just so we make sure consecutive requests
    # don't crash anything on the node
    nodes[0].json_rpc('sandbox_fast_forward',
                      {"delta_height": BLOCKS_TO_FASTFORWARD},
                      timeout=60)
    resp = nodes[0].json_rpc("block", {"finality": "optimistic"})
    assert resp['result']['chunks'][0][
        'height_created'] > 2 * BLOCKS_TO_FASTFORWARD

    assert True


@pytest.mark.flaky(reruns=1)
def test_fast_forward_epoch_boundary():
    test_name = utils.get_test_name()
    node_config = NODE_CONFIG.copy()
    node_config.update({
        "consensus": {
            "min_block_production_delay": {
                "secs": MIN_BLOCK_PROD_TIME,
                "nanos": 0,
            },
            "max_block_production_delay": {
                "secs": MAX_BLOCK_PROD_TIME,
                "nanos": 0,
            },
        }
    })

    # startup a RPC node
    nodes = start_cluster(1,
                          0,
                          1,
                          node_config, [["epoch_length", EPOCH_LENGTH]], {},
                          prefix=test_name)

    # start at block_height = 10
    utils.wait_for_blocks(nodes[0], target=10)
    # fast forward to about block_height=190 and then test for boundaries
    nodes[0].json_rpc('sandbox_fast_forward', {"delta_height": 180}, timeout=60)
    for i in range(20):
        utils.wait_for_blocks(nodes[0], target=190 + i)
        block_height = nodes[0].get_latest_block().height
        epoch_height = nodes[0].get_validators()['result']['epoch_height']
        assert epoch_height == 2 if block_height > 200 else 1

    # check that we still have correct epoch heights after consecutive fast forwards:
    utils.wait_for_blocks(nodes[0], target=220)
    nodes[0].json_rpc('sandbox_fast_forward', {"delta_height": 70}, timeout=60)
    for i in range(20):
        utils.wait_for_blocks(nodes[0], target=290 + i)
        block_height = nodes[0].get_latest_block().height
        epoch_height = nodes[0].get_validators()['result']['epoch_height']
        assert epoch_height == 3 if block_height > 300 else 2