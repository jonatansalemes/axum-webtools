use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::Value;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionStatus {
    pub partition_id: i32,
    pub low_watermark: i64,
    pub high_watermark: i64,
    pub committed_offset: i64,
    pub lag: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunStatusResult {
    pub topic: String,
    pub consumer_group: String,
    pub partition_count: usize,
    pub partitions: Vec<PartitionStatus>,
    pub total_lag: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRedriveResult {
    pub source_topic: String,
    pub target_topic: String,
    pub poison_topic: String,
    pub max_age_days: i64,
    pub redrove_count: i64,
    pub poison_count: i64,
    pub processed: i64,
}

#[derive(Parser)]
#[command(name = "dlq-redrive")]
#[command(about = "A Kafka DLQ redrive tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Redrive messages from DLQ to target topic
    #[command(name = "redrive")]
    Redrive {
        /// Kafka bootstrap servers
        #[arg(short = 'b', long = "bootstrap-server")]
        bootstrap_server: String,

        /// Source DLQ topic
        #[arg(short = 's', long = "source")]
        source_topic: String,

        /// Target topic
        #[arg(short = 't', long = "target")]
        target_topic: String,

        /// Poison topic for old messages
        #[arg(short = 'p', long = "poison")]
        poison_topic: String,

        /// Consumer group for DLQ processing
        #[arg(short = 'g', long = "group")]
        consumer_group: String,

        /// Maximum age in days for messages (older messages go to poison)
        #[arg(short = 'm', long = "max-age-days", default_value = "5")]
        max_age_days: i64,

        /// Maximum number of messages to process (0 = all)
        #[arg(long = "max-messages", default_value = "0")]
        max_messages: i64,
    },

    /// Check DLQ topic status
    #[command(name = "status")]
    Status {
        /// Kafka bootstrap servers
        #[arg(short = 'b', long = "bootstrap-server")]
        bootstrap_server: String,

        /// Source DLQ topic
        #[arg(short = 's', long = "source")]
        source_topic: String,

        /// Consumer group for DLQ processing
        #[arg(short = 'g', long = "group")]
        consumer_group: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Redrive {
            bootstrap_server,
            source_topic,
            target_topic,
            poison_topic,
            consumer_group,
            max_age_days,
            max_messages,
        } => {
            let result = run_redrive(
                &bootstrap_server,
                &source_topic,
                &target_topic,
                &poison_topic,
                &consumer_group,
                max_age_days,
                max_messages,
            )
            .await?;
            print_redrive(&result);
        }
        Commands::Status {
            bootstrap_server,
            source_topic,
            consumer_group,
        } => {
            let result = run_status(&bootstrap_server, &source_topic, &consumer_group).await?;
            print_status(&result);
        }
    }

    Ok(())
}

pub async fn run_status(
    bootstrap_server: &str,
    source_topic: &str,
    consumer_group: &str,
) -> Result<RunStatusResult, Box<dyn std::error::Error>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("group.id", consumer_group)
        .set("enable.auto.commit", "false")
        .create()?;

    let metadata = consumer
        .fetch_metadata(Some(source_topic), Duration::from_secs(10))
        .map_err(|e| format!("Failed to fetch metadata: {}", e))?;

    let topic_metadata = metadata
        .topics()
        .iter()
        .find(|t| t.name() == source_topic)
        .ok_or_else(|| format!("Topic '{}' not found", source_topic))?;

    let partition_count = topic_metadata.partitions().len();

    let mut total_lag: i64 = 0;
    let mut partitions = Vec::new();

    for partition in topic_metadata.partitions() {
        let partition_id = partition.id();
        let (low, high) = consumer
            .fetch_watermarks(source_topic, partition_id, Duration::from_secs(5))
            .unwrap_or((0, 0));

        // Get committed offset for this partition
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(source_topic, partition_id);

        let committed = consumer
            .committed_offsets(tpl, Duration::from_secs(5))
            .ok()
            .and_then(|tpl| {
                tpl.elements()
                    .iter()
                    .find(|e| e.partition() == partition_id)
                    .and_then(|e| match e.offset() {
                        Offset::Invalid => None,
                        Offset::Beginning => Some(low),
                        Offset::End => Some(high),
                        Offset::Stored => None,
                        Offset::Offset(o) => Some(o),
                        _ => None,
                    })
            })
            .unwrap_or(low);

        let lag = high - committed;
        total_lag += lag;

        partitions.push(PartitionStatus {
            partition_id,
            low_watermark: low,
            high_watermark: high,
            committed_offset: committed,
            lag,
        });
    }

    Ok(RunStatusResult {
        topic: source_topic.to_string(),
        consumer_group: consumer_group.to_string(),
        partition_count,
        partitions,
        total_lag,
    })
}

fn print_status(result: &RunStatusResult) {
    println!("Checking DLQ status...");
    println!("Source topic: {}", result.topic);
    println!("Consumer group: {}", result.consumer_group);
    println!(
        "Found {} partitions in {}",
        result.partition_count, result.topic
    );

    for partition in &result.partitions {
        println!(
            "Partition {}: Low={}, High={}, Committed={}, Lag={}",
            partition.partition_id,
            partition.low_watermark,
            partition.high_watermark,
            partition.committed_offset,
            partition.lag
        );
    }

    if result.total_lag == 0 {
        println!("No messages to redrive.");
    } else {
        println!("Total lag: {} messages", result.total_lag);
    }
}

fn print_redrive(result: &RunRedriveResult) {
    println!("DLQ redrive process completed successfully!");
    println!(
        "Redrove {} messages from {} to {}",
        result.redrove_count, result.source_topic, result.target_topic
    );
    println!(
        "Sent {} messages to {} (older than {} days)",
        result.poison_count, result.poison_topic, result.max_age_days
    );
    println!("Messages will be automatically deleted based on topic retention policies");
}

pub async fn run_redrive(
    bootstrap_server: &str,
    source_topic: &str,
    target_topic: &str,
    poison_topic: &str,
    consumer_group: &str,
    max_age_days: i64,
    max_messages: i64,
) -> Result<RunRedriveResult, Box<dyn std::error::Error>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("group.id", consumer_group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[source_topic])?;

    let target_producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("message.timeout.ms", "5000")
        .create()?;

    let poison_producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("message.timeout.ms", "5000")
        .create()?;

    let poll_timeout = Duration::from_millis(500);
    let max_idle_polls = 10; // 5s total
    let mut idle_polls = 0;

    let current_time = Utc::now();
    let max_age_seconds = max_age_days * 86400;

    let mut redrove_count = 0;
    let mut poison_count = 0;
    let mut processed = 0;

    consumer.poll(Duration::from_secs(1));

    loop {
        match consumer.poll(poll_timeout) {
            Some(Ok(msg)) => {
                idle_polls = 0;

                let payload = match msg.payload() {
                    Some(p) => p,
                    None => {
                        consumer.commit_message(&msg, CommitMode::Async)?;
                        continue;
                    }
                };

                let should_poison = serde_json::from_slice::<Value>(payload)
                    .ok()
                    .and_then(|json| {
                        json.get("created_at")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<DateTime<Utc>>().ok())
                    })
                    .map(|created_at| (current_time - created_at).num_seconds() > max_age_seconds)
                    .unwrap_or(false);

                let (producer, target) = if should_poison {
                    (&poison_producer, poison_topic)
                } else {
                    (&target_producer, target_topic)
                };

                let mut record = FutureRecord::to(target).payload(payload);

                if let Some(key) = msg.key() {
                    record = record.key(key);
                }

                match producer.send(record, Duration::from_secs(5)).await {
                    Ok(_) => {
                        consumer.commit_message(&msg, CommitMode::Async)?;
                        if should_poison {
                            poison_count += 1;
                        } else {
                            redrove_count += 1;
                        }
                        processed += 1;
                    }
                    Err((e, _)) => {
                        eprintln!("Produce failed, offset NOT committed: {}", e);
                    }
                }

                if max_messages > 0 && processed >= max_messages {
                    break;
                }
            }

            Some(Err(KafkaError::PartitionEOF(_))) => {
                println!("Partition EOF");
            }

            Some(Err(e)) => {
                eprintln!("Kafka error: {}", e);
            }

            None => {
                idle_polls += 1;

                if idle_polls >= max_idle_polls {
                    println!("No messages received for {} polls, exiting", max_idle_polls);
                    break;
                }
            }
        }
    }

    if let Err(e) = target_producer.flush(Duration::from_secs(5)) {
        eprintln!("Failed to flush target producer: {}", e);
    }
    if let Err(e) = poison_producer.flush(Duration::from_secs(5)) {
        eprintln!("Failed to flush poison producer: {}", e);
    };

    Ok(RunRedriveResult {
        source_topic: source_topic.to_string(),
        target_topic: target_topic.to_string(),
        poison_topic: poison_topic.to_string(),
        max_age_days,
        redrove_count,
        poison_count,
        processed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::distr::{Alphanumeric, SampleString};
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;

    fn random_topic_name(prefix: &str) -> String {
        let random_suffix = Alphanumeric.sample_string(&mut rand::rng(), 8);
        format!("{}-{}", prefix, random_suffix)
    }

    fn random_consumer_group() -> String {
        let random_suffix = Alphanumeric.sample_string(&mut rand::rng(), 8);
        format!("test-group-{}", random_suffix)
    }

    async fn create_test_topic(
        bootstrap_server: &str,
        prefix: &str,
        num_partitions: i32,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let topic_name = random_topic_name(prefix);
        let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_server)
            .create()?;

        let new_topic = NewTopic::new(&topic_name, num_partitions, TopicReplication::Fixed(1));
        let opts = AdminOptions::new();

        admin_client
            .create_topics(&[new_topic], &opts)
            .await
            .map_err(|e| format!("Failed to create topic {}: {:?}", topic_name, e))?;

        tokio::time::sleep(Duration::from_secs(5)).await;

        Ok(topic_name)
    }

    async fn send_test_messages(
        bootstrap_server: &str,
        topic: &str,
        messages: Vec<(&str, &str)>, // (key, payload)
    ) -> Result<(), Box<dyn std::error::Error>> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_server)
            .set("message.timeout.ms", "5000")
            .create()?;

        for (key, payload) in messages {
            let record = FutureRecord::to(topic).key(key).payload(payload);
            producer
                .send(record, Duration::from_secs(5))
                .await
                .map_err(|(e, _)| format!("Failed to send message: {}", e))?;
        }

        producer.flush(Duration::from_secs(5))?;
        Ok(())
    }

    fn create_message_with_timestamp(days_ago: i64, content: &str) -> String {
        let timestamp = Utc::now() - chrono::Duration::days(days_ago);
        format!(
            r#"{{"created_at":"{}","content":"{}"}}"#,
            timestamp.to_rfc3339(),
            content
        )
    }

    #[tokio::test]
    async fn test_run_status_with_kafka() {
        let bootstrap_server = "kafka:9092";
        let source_topic = create_test_topic(bootstrap_server, "status-test", 1)
            .await
            .expect("Failed to create test topic");
        let consumer_group = random_consumer_group();

        let msg1 = create_message_with_timestamp(1, "test1");
        let msg2 = create_message_with_timestamp(2, "test2");
        let messages = vec![("key1", msg1.as_str()), ("key2", msg2.as_str())];
        send_test_messages(bootstrap_server, &source_topic, messages)
            .await
            .expect("Failed to send test messages");

        let result = run_status(bootstrap_server, &source_topic, &consumer_group)
            .await
            .expect("Failed to get status");

        assert_eq!(result.topic, source_topic);
        assert_eq!(result.consumer_group, consumer_group);
        assert!(result.partition_count > 0, "Expected at least 1 partition");
        assert_eq!(
            result.partitions.len(),
            result.partition_count,
            "Partitions vector length should match partition_count"
        );

        for partition in &result.partitions {
            assert!(
                partition.partition_id >= 0,
                "Partition ID should be non-negative"
            );
            assert!(
                partition.high_watermark >= partition.low_watermark,
                "High watermark should be >= low watermark"
            );
            assert!(
                partition.committed_offset >= partition.low_watermark,
                "Committed offset should be >= low watermark"
            );
            assert!(partition.lag >= 0, "Lag should be non-negative");
            assert_eq!(
                partition.lag,
                partition.high_watermark - partition.committed_offset,
                "Lag should equal high - committed"
            );
        }

        let calculated_total_lag: i64 = result.partitions.iter().map(|p| p.lag).sum();
        assert_eq!(
            result.total_lag, calculated_total_lag,
            "Total lag should equal sum of partition lags"
        );
    }

    #[tokio::test]
    async fn test_run_status_multiple_topics() {
        let bootstrap_server = "kafka:9092";
        let consumer_group = random_consumer_group();

        let topic_prefixes = vec!["multi-test-dlq", "multi-test-main", "multi-test-poison"];
        let mut topics = Vec::new();

        for prefix in topic_prefixes {
            let topic = create_test_topic(bootstrap_server, prefix, 1)
                .await
                .expect(&format!("Failed to create topic with prefix: {}", prefix));
            topics.push(topic);
        }

        for topic in &topics {
            let msg = create_message_with_timestamp(1, "content1");
            let messages = vec![("key1", msg.as_str())];
            send_test_messages(bootstrap_server, topic, messages)
                .await
                .expect(&format!("Failed to send messages to topic: {}", topic));

            let result = run_status(bootstrap_server, topic, &consumer_group)
                .await
                .expect(&format!("Failed to get status for topic: {}", topic));

            assert_eq!(result.topic, *topic);
            assert_eq!(result.consumer_group, consumer_group);
            assert!(result.partition_count > 0);
            assert_eq!(result.partitions.len(), result.partition_count);
        }
    }

    #[tokio::test]
    async fn test_run_status_with_empty_topic() {
        let bootstrap_server = "kafka:9092";
        let source_topic = create_test_topic(bootstrap_server, "empty-test", 1)
            .await
            .expect("Failed to create test topic");
        let consumer_group = random_consumer_group();

        let result = run_status(bootstrap_server, &source_topic, &consumer_group)
            .await
            .expect("Failed to get status for empty topic");

        assert_eq!(result.topic, source_topic);
        assert_eq!(result.consumer_group, consumer_group);
        assert!(result.partition_count > 0, "Expected at least 1 partition");

        for partition in &result.partitions {
            assert!(
                partition.partition_id >= 0,
                "Partition ID should be non-negative"
            );
            assert!(
                partition.high_watermark >= partition.low_watermark,
                "High watermark should be >= low watermark"
            );
            assert!(partition.lag >= 0, "Lag should be non-negative");
        }
    }

    #[tokio::test]
    async fn test_run_redrive_with_kafka() {
        let bootstrap_server = "kafka:9092";
        let source_topic = create_test_topic(bootstrap_server, "redrive-source", 1)
            .await
            .expect("Failed to create source topic");
        let target_topic = create_test_topic(bootstrap_server, "redrive-target", 1)
            .await
            .expect("Failed to create target topic");
        let poison_topic = create_test_topic(bootstrap_server, "redrive-poison", 1)
            .await
            .expect("Failed to create poison topic");
        let consumer_group = random_consumer_group();
        let max_age_days = 5;
        let max_messages = 2;

        let msg1 = create_message_with_timestamp(1, "recent message");
        let msg2 = create_message_with_timestamp(10, "old message");
        let messages = vec![("key1", msg1.as_str()), ("key2", msg2.as_str())];
        send_test_messages(bootstrap_server, &source_topic, messages)
            .await
            .expect("Failed to send test messages");

        let result = run_redrive(
            bootstrap_server,
            &source_topic,
            &target_topic,
            &poison_topic,
            &consumer_group,
            max_age_days,
            max_messages,
        )
        .await
        .expect("Failed to run redrive");

        assert_eq!(result.source_topic, source_topic);
        assert_eq!(result.target_topic, target_topic);
        assert_eq!(result.poison_topic, poison_topic);
        assert_eq!(result.max_age_days, max_age_days);
        assert!(
            result.processed >= 0,
            "Processed count should be non-negative"
        );
        assert!(
            result.redrove_count >= 0,
            "Redrove count should be non-negative"
        );
        assert!(
            result.poison_count >= 0,
            "Poison count should be non-negative"
        );
        assert_eq!(
            result.processed,
            result.redrove_count + result.poison_count,
            "Processed should equal redrove + poison"
        );
        assert!(
            result.processed <= max_messages,
            "Should not process more than max_messages"
        );
    }

    #[tokio::test]
    async fn test_run_redrive_empty_topic() {
        let bootstrap_server = "kafka:9092";
        let source_topic = create_test_topic(bootstrap_server, "redrive-empty-source", 1)
            .await
            .expect("Failed to create source topic");
        let target_topic = create_test_topic(bootstrap_server, "redrive-empty-target", 1)
            .await
            .expect("Failed to create target topic");
        let poison_topic = create_test_topic(bootstrap_server, "redrive-empty-poison", 1)
            .await
            .expect("Failed to create poison topic");
        let consumer_group = random_consumer_group();
        let max_age_days = 5;
        let max_messages = 1;

        let result = run_redrive(
            bootstrap_server,
            &source_topic,
            &target_topic,
            &poison_topic,
            &consumer_group,
            max_age_days,
            max_messages,
        )
        .await
        .expect("Failed to run redrive on empty topic");

        assert_eq!(
            result.processed, 0,
            "Should process 0 messages from empty topic"
        );
        assert_eq!(result.processed, result.redrove_count + result.poison_count);
    }

    #[tokio::test]
    async fn test_run_redrive_result_structure() {
        let bootstrap_server = "kafka:9092";
        let source_topic = create_test_topic(bootstrap_server, "redrive-struct-source", 1)
            .await
            .expect("Failed to create source topic");
        let target_topic = create_test_topic(bootstrap_server, "redrive-struct-target", 1)
            .await
            .expect("Failed to create target topic");
        let poison_topic = create_test_topic(bootstrap_server, "redrive-struct-poison", 1)
            .await
            .expect("Failed to create poison topic");
        let consumer_group = random_consumer_group();
        let max_age_days = 3;
        let max_messages = 2;

        let msg1 = create_message_with_timestamp(1, "recent");
        let msg2 = create_message_with_timestamp(7, "old");
        let messages = vec![("key1", msg1.as_str()), ("key2", msg2.as_str())];
        send_test_messages(bootstrap_server, &source_topic, messages)
            .await
            .expect("Failed to send test messages");

        let result = run_redrive(
            bootstrap_server,
            &source_topic,
            &target_topic,
            &poison_topic,
            &consumer_group,
            max_age_days,
            max_messages,
        )
        .await
        .expect("Failed to run redrive");

        assert_eq!(result.source_topic, source_topic);
        assert_eq!(result.target_topic, target_topic);
        assert_eq!(result.poison_topic, poison_topic);
        assert_eq!(result.max_age_days, max_age_days);

        assert!(result.processed >= 0);
        assert!(result.redrove_count >= 0);
        assert!(result.poison_count >= 0);
        assert_eq!(result.processed, result.redrove_count + result.poison_count);
    }

    #[tokio::test]
    async fn test_run_redrive_advance_offset() {
        let bootstrap_server = "kafka:9092";
        let source_topic = create_test_topic(bootstrap_server, "redrive-offset-source", 1)
            .await
            .expect("Failed to create source topic");
        let target_topic = create_test_topic(bootstrap_server, "redrive-offset-target", 1)
            .await
            .expect("Failed to create target topic");
        let poison_topic = create_test_topic(bootstrap_server, "redrive-offset-poison", 1)
            .await
            .expect("Failed to create poison topic");

        // Fixed consumer group name to test offset advancement
        let consumer_group = format!(
            "fixed-group-{}",
            Alphanumeric.sample_string(&mut rand::rng(), 8)
        );
        let max_age_days = 5;

        // Send first batch of 3 messages
        let msg1 = create_message_with_timestamp(1, "message 1");
        let msg2 = create_message_with_timestamp(1, "message 2");
        let msg3 = create_message_with_timestamp(1, "message 3");
        let messages_batch1 = vec![
            ("key1", msg1.as_str()),
            ("key2", msg2.as_str()),
            ("key3", msg3.as_str()),
        ];
        send_test_messages(bootstrap_server, &source_topic, messages_batch1)
            .await
            .expect("Failed to send first batch of messages");

        // Run redrive for first batch
        let result1 = run_redrive(
            bootstrap_server,
            &source_topic,
            &target_topic,
            &poison_topic,
            &consumer_group,
            max_age_days,
            0, // Process all messages
        )
        .await
        .expect("Failed to run first redrive");

        assert_eq!(
            result1.processed, 3,
            "First redrive should process 3 messages"
        );
        assert_eq!(
            result1.redrove_count, 3,
            "All 3 messages should be redrove (not poison)"
        );
        assert_eq!(result1.poison_count, 0, "No messages should be poisoned");

        // Send second batch of 3 messages
        let msg4 = create_message_with_timestamp(1, "message 4");
        let msg5 = create_message_with_timestamp(1, "message 5");
        let msg6 = create_message_with_timestamp(1, "message 6");
        let messages_batch2 = vec![
            ("key4", msg4.as_str()),
            ("key5", msg5.as_str()),
            ("key6", msg6.as_str()),
        ];
        send_test_messages(bootstrap_server, &source_topic, messages_batch2)
            .await
            .expect("Failed to send second batch of messages");

        // Run redrive again with same consumer group
        let result2 = run_redrive(
            bootstrap_server,
            &source_topic,
            &target_topic,
            &poison_topic,
            &consumer_group,
            max_age_days,
            0, // Process all messages
        )
        .await
        .expect("Failed to run second redrive");

        // Verify second redrive continues from last committed offset
        assert_eq!(
            result2.processed, 3,
            "Second redrive should process only 3 new messages"
        );
        assert_eq!(
            result2.redrove_count, 3,
            "All 3 new messages should be redrove"
        );
        assert_eq!(result2.poison_count, 0, "No messages should be poisoned");

        // Verify total messages in target topic is 6
        let status = run_status(bootstrap_server, &target_topic, &random_consumer_group())
            .await
            .expect("Failed to get target topic status");

        let total_messages_in_target: i64 = status
            .partitions
            .iter()
            .map(|p| p.high_watermark - p.low_watermark)
            .sum();
        assert_eq!(
            total_messages_in_target, 6,
            "Target topic should have 6 messages total"
        );
    }
}
