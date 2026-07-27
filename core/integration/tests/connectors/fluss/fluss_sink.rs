// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::connectors::create_test_messages;
use crate::connectors::fixtures::FlussSinkFixture;
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::seeds;
use integration::iggy_harness;

const TEST_MESSAGE_COUNT: usize = 50;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/fluss/sink.toml")),
    seed = seeds::connector_stream
)]
async fn sink_should_create_test_table(harness: &TestHarness, fixture: FlussSinkFixture) {
    let client = harness.root_client().await.unwrap();

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let messages_data = create_test_messages(1);
    let mut messages: Vec<IggyMessage> = messages_data
        .iter()
        .enumerate()
        .map(|(i, msg)| {
            let payload = serde_json::to_vec(msg).expect("Failed to serialize message");
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(payload))
                .build()
                .expect("Failed to build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    let test_table = fixture.wait_for_test_table(10).await.is_ok();

    assert!(
        test_table,
        "Expected test table to exist after sending messages"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/fluss/sink.toml")),
    seed = seeds::connector_stream
)]
async fn sink_should_write_message_to_test_table(harness: &TestHarness, fixture: FlussSinkFixture) {
    let client = harness.root_client().await.unwrap();

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let messages_data = create_test_messages(TEST_MESSAGE_COUNT);
    let mut messages: Vec<IggyMessage> = messages_data
        .iter()
        .enumerate()
        .map(|(i, msg)| {
            let payload = serde_json::to_vec(msg).expect("Failed to serialize message");
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(payload))
                .build()
                .expect("Failed to build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Failed to send messages");

    fixture
        .wait_for_test_table(10)
        .await
        .expect("Table has not found in time");

    let messages = fixture
        .read_from_test_table(10)
        .await
        .expect("read messages");

    assert_eq!(messages.len(), TEST_MESSAGE_COUNT);
}
