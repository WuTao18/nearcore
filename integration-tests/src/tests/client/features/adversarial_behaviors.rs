use std::{collections::HashSet, path::Path, sync::Arc};

use near_chain::{ChainGenesis, Provenance, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_o11y::testonly::init_test_logger;
use near_primitives::{
    runtime::config_store::RuntimeConfigStore,
    shard_layout::ShardLayout,
    sharding::PartialEncodedChunk,
    types::{AccountId, EpochId, ShardId},
};
use near_store::test_utils::create_test_store;
use nearcore::{config::GenesisExt, TrackedConfig};
use tracing::log::debug;

struct AdversarialBehaviorTestData {
    num_validators: usize,
    env: TestEnv,
}

const EPOCH_LENGTH: u64 = 20;

impl AdversarialBehaviorTestData {
    fn new() -> AdversarialBehaviorTestData {
        let num_clients = 8;
        let num_validators = 8 as usize;
        let num_block_producers = 4;
        let epoch_length = EPOCH_LENGTH;

        let accounts: Vec<AccountId> =
            (0..num_clients).map(|i| format!("test{}", i).parse().unwrap()).collect();
        let mut genesis = Genesis::test(accounts, num_validators as u64);
        {
            let config = &mut genesis.config;
            config.epoch_length = epoch_length;
            config.shard_layout = ShardLayout::v1_test();
            config.num_block_producer_seats_per_shard = vec![
                num_block_producers as u64,
                num_block_producers as u64,
                num_block_producers as u64,
                num_block_producers as u64,
            ];
            config.num_block_producer_seats = num_block_producers as u64;
            // Configure kickout threshold at 50%.
            config.block_producer_kickout_threshold = 50;
            config.chunk_producer_kickout_threshold = 50;
        }
        let chain_genesis = ChainGenesis::new(&genesis);
        let runtimes: Vec<Arc<dyn RuntimeAdapter>> = (0..num_clients)
            .map(|_| {
                Arc::new(nearcore::NightshadeRuntime::test_with_runtime_config_store(
                    Path::new("."),
                    create_test_store(),
                    &genesis,
                    TrackedConfig::AllShards,
                    RuntimeConfigStore::test(),
                )) as Arc<dyn RuntimeAdapter>
            })
            .collect();
        let env = TestEnv::builder(chain_genesis)
            .clients_count(num_clients)
            .validator_seats(num_validators as usize)
            .runtime_adapters(runtimes)
            .build();

        AdversarialBehaviorTestData { num_validators, env }
    }

    fn process_one_peer_message(&mut self, client_id: usize, requests: NetworkRequests) {
        match requests {
            NetworkRequests::PartialEncodedChunkRequest { .. } => {
                self.env.process_partial_encoded_chunk_request(
                    client_id,
                    PeerManagerMessageRequest::NetworkRequests(requests),
                );
            }
            NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
                self.env
                    .client(&account_id)
                    .process_partial_encoded_chunk(
                        PartialEncodedChunk::from(partial_encoded_chunk).into(),
                        Arc::new(|_| {}),
                    )
                    .unwrap();
            }
            NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
                match self
                    .env
                    .client(&account_id)
                    .process_partial_encoded_chunk_forward(forward, Arc::new(|_| {}))
                {
                    Ok(_) | Err(near_client::Error::Chunk(near_chunks::Error::UnknownChunk)) => {}
                    Err(e) => {
                        panic!("Unexpected error from chunk forward: {:?}", e)
                    }
                }
            }
            NetworkRequests::Challenge(_) => {
                // challenges not enabled.
            }
            _ => {
                panic!("Unexpected network request: {:?}", requests);
            }
        }
    }

    fn process_all_actor_messages(&mut self) {
        loop {
            let mut any_message_processed = false;
            for i in 0..self.num_validators {
                if let Some(msg) = self.env.network_adapters[i].pop() {
                    any_message_processed = true;
                    match msg {
                        PeerManagerMessageRequest::NetworkRequests(requests) => {
                            self.process_one_peer_message(i, requests);
                        }
                        _ => {
                            panic!("Unexpected message: {:?}", msg);
                        }
                    }
                }
            }
            if !any_message_processed {
                break;
            }
        }
    }
}

#[test]
fn test_non_adversarial_case() {
    init_test_logger();
    let mut test = AdversarialBehaviorTestData::new();
    let runtime_adapter = test.env.clients[0].runtime_adapter.clone();
    for height in 1..=EPOCH_LENGTH * 4 + 5 {
        debug!(target: "test", "======= Height {} ======", height);
        test.process_all_actor_messages();
        let epoch_id = runtime_adapter
            .get_epoch_id_from_prev_block(
                &test.env.clients[0].chain.head().unwrap().last_block_hash,
            )
            .unwrap();
        let block_producer = runtime_adapter.get_block_producer(&epoch_id, height).unwrap();

        let block = test.env.client(&block_producer).produce_block(height).unwrap().unwrap();
        assert_eq!(block.header().height(), height);

        if height > 1 {
            assert_eq!(block.header().prev_height().unwrap(), height - 1);
            let prev_block =
                test.env.clients[0].chain.get_block(&block.header().prev_hash()).unwrap();
            for i in 0..4 {
                // TODO: mysteriously we might miss a chunk around epoch boundaries.
                // Figure out why...
                assert!(
                    block.chunks()[i].height_created() == prev_block.header().height() + 1
                        || (height % EPOCH_LENGTH == 1
                            && block.chunks()[i].chunk_hash()
                                == prev_block.chunks()[i].chunk_hash())
                );
            }
        }

        for i in 0..test.num_validators {
            debug!(target: "test", "Processing block {} as validator #{}", height, i);
            let _ = test.env.clients[i].start_process_block(
                block.clone().into(),
                if i == 0 { Provenance::PRODUCED } else { Provenance::NONE },
                Arc::new(|_| {}),
            );
            let mut accepted_blocks =
                test.env.clients[i].finish_block_in_processing(block.header().hash());
            // Process any chunk part requests that this client sent. Note that this would also
            // process other network messages (such as production of the next chunk) which is OK.
            test.process_all_actor_messages();
            accepted_blocks.extend(test.env.clients[i].finish_blocks_in_processing());

            assert_eq!(
                accepted_blocks.len(),
                1,
                "Processing of block {} failed at validator #{}",
                height,
                i
            );
            assert_eq!(&accepted_blocks[0], block.header().hash());
            assert_eq!(test.env.clients[i].chain.head().unwrap().height, height);
        }
    }

    // Sanity check that the final chain head is what we expect
    assert_eq!(test.env.clients[0].chain.head().unwrap().height, EPOCH_LENGTH * 4 + 5);
    let final_prev_block_hash = test.env.clients[0].chain.head().unwrap().prev_block_hash;
    let final_epoch_id =
        runtime_adapter.get_epoch_id_from_prev_block(&final_prev_block_hash).unwrap();
    let final_block_producers = runtime_adapter
        .get_epoch_block_producers_ordered(&final_epoch_id, &final_prev_block_hash)
        .unwrap();
    // No producers should be kicked out.
    assert_eq!(final_block_producers.len(), 4);
    let final_chunk_producers = runtime_adapter.get_epoch_chunk_producers(&final_epoch_id).unwrap();
    assert_eq!(final_chunk_producers.len(), 8);
}

// Not marking this with test_features, because it's good to ensure this compiles, and also
// if we mark this with features we'd also have to mark a bunch of imports as features.
#[allow(dead_code)]
fn test_banning_chunk_producer_when_seeing_invalid_chunk_base(
    mut test: AdversarialBehaviorTestData,
) {
    let runtime_adapter = test.env.clients[0].runtime_adapter.clone();
    let bad_chunk_producer =
        test.env.clients[7].validator_signer.as_ref().unwrap().validator_id().clone();
    let mut epochs_seen_invalid_chunk: HashSet<EpochId> = HashSet::new();
    let mut last_block_skipped = false;
    for height in 1..=EPOCH_LENGTH * 4 + 5 {
        debug!(target: "test", "======= Height {} ======", height);
        test.process_all_actor_messages();
        let epoch_id = runtime_adapter
            .get_epoch_id_from_prev_block(
                &test.env.clients[0].chain.head().unwrap().last_block_hash,
            )
            .unwrap();
        let block_producer = runtime_adapter.get_block_producer(&epoch_id, height).unwrap();

        let block = test.env.client(&block_producer).produce_block(height).unwrap().unwrap();
        assert_eq!(block.header().height(), height);

        let mut invalid_chunks_in_this_block: HashSet<ShardId> = HashSet::new();
        let mut this_block_should_be_skipped = false;
        if height > 1 {
            if last_block_skipped {
                assert_eq!(block.header().prev_height().unwrap(), height - 2);
            } else {
                assert_eq!(block.header().prev_height().unwrap(), height - 1);
            }
            for shard_id in 0..4 {
                let chunk_producer = runtime_adapter
                    .get_chunk_producer(
                        &epoch_id,
                        block.header().prev_height().unwrap() + 1,
                        shard_id,
                    )
                    .unwrap();
                if &chunk_producer == &bad_chunk_producer {
                    invalid_chunks_in_this_block.insert(shard_id);
                    if !epochs_seen_invalid_chunk.contains(&epoch_id) {
                        this_block_should_be_skipped = true;
                        epochs_seen_invalid_chunk.insert(epoch_id.clone());
                    }
                }
            }
        }
        debug!(target: "test", "Epoch id of new block: {:?}", epoch_id);
        debug!(target: "test", "Block should be skipped: {}; previous block skipped: {}",
            this_block_should_be_skipped, last_block_skipped);

        if height > 1 {
            let prev_block =
                test.env.clients[0].chain.get_block(&block.header().prev_hash()).unwrap();
            for i in 0..4 {
                if invalid_chunks_in_this_block.contains(&(i as ShardId))
                    && !this_block_should_be_skipped
                {
                    assert_eq!(block.chunks()[i].chunk_hash(), prev_block.chunks()[i].chunk_hash());
                } else {
                    // TODO: mysteriously we might miss a chunk around epoch boundaries.
                    // Figure out why...
                    assert!(
                        block.chunks()[i].height_created() == prev_block.header().height() + 1
                            || (height % EPOCH_LENGTH == 1
                                && block.chunks()[i].chunk_hash()
                                    == prev_block.chunks()[i].chunk_hash())
                    );
                }
            }
        }

        // The block producer of course has the complete block so we can process that.
        for i in 0..test.num_validators {
            debug!(target: "test", "Processing block {} as validator #{}", height, i);
            let _ = test.env.clients[i].start_process_block(
                block.clone().into(),
                if i == 0 { Provenance::PRODUCED } else { Provenance::NONE },
                Arc::new(|_| {}),
            );
            let mut accepted_blocks =
                test.env.clients[i].finish_block_in_processing(block.header().hash());
            // Process any chunk part requests that this client sent. Note that this would also
            // process other network messages (such as production of the next chunk) which is OK.
            test.process_all_actor_messages();
            accepted_blocks.extend(test.env.clients[i].finish_blocks_in_processing());

            if this_block_should_be_skipped {
                assert_eq!(
                    accepted_blocks.len(),
                    0,
                    "Processing of block {} should have failed due to invalid chunk",
                    height
                );
            } else {
                assert_eq!(
                    accepted_blocks.len(),
                    1,
                    "Processing of block {} failed at validator #{}",
                    height,
                    i
                );
                assert_eq!(&accepted_blocks[0], block.header().hash());
                assert_eq!(test.env.clients[i].chain.head().unwrap().height, height);
            }
        }
        last_block_skipped = this_block_should_be_skipped;
    }

    // Sanity check that the final chain head is what we expect
    assert_eq!(test.env.clients[0].chain.head().unwrap().height, EPOCH_LENGTH * 4 + 5);
    // Bad validator should've been kicked out in the third epoch, so it only had two chances
    // to produce bad chunks. Other validators should not be kicked out.
    assert_eq!(epochs_seen_invalid_chunk.len(), 2);
    let final_prev_block_hash = test.env.clients[0].chain.head().unwrap().prev_block_hash;
    let final_epoch_id =
        runtime_adapter.get_epoch_id_from_prev_block(&final_prev_block_hash).unwrap();
    let final_block_producers = runtime_adapter
        .get_epoch_block_producers_ordered(&final_epoch_id, &final_prev_block_hash)
        .unwrap();
    assert!(final_block_producers.len() >= 3); // 3 validators if the bad validator was a block producer
    let final_chunk_producers = runtime_adapter.get_epoch_chunk_producers(&final_epoch_id).unwrap();
    assert_eq!(final_chunk_producers.len(), 7);
}

#[test]
#[cfg(feature = "test_features")]
fn test_banning_chunk_producer_when_seeing_invalid_chunk() {
    init_test_logger();
    let mut test = AdversarialBehaviorTestData::new();
    test.env.clients[7].produce_invalid_chunks = true;
    test_banning_chunk_producer_when_seeing_invalid_chunk_base(test);
}

#[test]
#[cfg(feature = "test_features")]
fn test_banning_chunk_producer_when_seeing_invalid_tx_in_chunk() {
    init_test_logger();
    let mut test = AdversarialBehaviorTestData::new();
    test.env.clients[7].produce_invalid_tx_in_chunks = true;
    test_banning_chunk_producer_when_seeing_invalid_chunk_base(test);
}
