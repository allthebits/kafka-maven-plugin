/*
 * Copyright 2015 Charith Ellawala
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.charithe.kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public enum KafkaStandalone implements AutoCloseable {
	INSTANCE;
	
	public static final Integer KAFKA_TESTING_PORT = 59092;
	public static final Integer ZOOKEEPER_TESTING_PORT = 52181;
	
	private static final Logger LOG = org.apache.log4j.LogManager.getLogger(KafkaStandalone.class);

	private TestingServer zookeeper;
	private KafkaServerStartable kafka;
	
	private Path kafkaLogDir;
	private String kafkaHostname = "localhost";
	private Integer kafkaPort;
	private Integer zookeeperPort;
	private final Properties overrideProps = new Properties();
	
	public void configure(int zookeeperPort, int kafkaPort) {
		synchronized (this) {
			if (this.kafkaPort != null) {
				return;
			}
		}
		this.kafkaPort = kafkaPort;
		this.zookeeperPort = zookeeperPort;
		
	}

	public void start() throws Exception {
		synchronized (this) {
			if (this.kafka != null) {
				return;
			}
		}
		
		this.zookeeper = getZookeeper(this.zookeeperPort);
		this.kafka = getKafka(this.zookeeper, this.kafkaPort);
	}
	
	public void setProperty(String key, String value) {
		this.overrideProps.setProperty(key, value);
	}
	public String getKafkaConnectString() {
		return this.kafkaHostname + ":" + this.kafkaPort;
	}
	public String getZookeeperConnectString() {
		return this.zookeeper.getConnectString();
	}

	public int getZookeeperPort() {
		return this.zookeeper.getPort();
	}

	public int getKafkaPort() {
		return this.kafkaPort;
	}

	public KafkaServerStartable getKafka() {
		return this.kafka;
	}
	public TestingServer getZookeeper() {
		return this.zookeeper;
	}
	
	public boolean createTopic(String topic) {
		return createTopic(topic, 1);
	}
	@SuppressWarnings("unused")
	public boolean createTopic(String topic, int partitions) {
		try (AdminClient ac = getAdminClient()) {
			NewTopic theTopic = new NewTopic(topic, partitions, (short) 1);
			CreateTopicsResult result  = ac.createTopics(Collections.singleton(theTopic));
			
			//** Wait for the asynchronous call to complete
			Object o = result.values().get(topic).get();
			return true;
		} catch (Exception ex) {
			Throwable cause = ex.getCause();
			if (cause instanceof TopicExistsException) {
				String msg = String.format("Topic %s already exists.", topic);
				LOG.warn(msg);
			} else {
				throw new RuntimeException(ex);
			}
		}
		return false;
	}
	
	public boolean topicExists(String topic) {
		KafkaConsumer<String,String> consumer = createConsumer();
		
    	Map<String, List<PartitionInfo>> result = consumer.listTopics();
    	
    	for (String key : result.keySet()) {
    		if (key.equals(topic)) return true;
    	}
    	
    	return false;
	}
	
	public KafkaProducer<String, String> createProducer() {
		Class<? extends Serializer<String>> clazz = StringSerializer.class;
		return createProducer(clazz, clazz);
	}
	
	public <K,V> KafkaProducer<K,V> createProducer(Class<? extends Serializer<K>> keySer, Class<? extends Serializer<V>> valueSer) {
        final Map<String, Object> cfg = new java.util.HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConnectString());
        cfg.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        cfg.put(ProducerConfig.RETRIES_CONFIG, 5);
        
        String clientid;
        synchronized (this) {
        	clientid = getClass().getSimpleName() + "-Producer-" + System.currentTimeMillis();
		}
        cfg.put(ProducerConfig.CLIENT_ID_CONFIG, clientid);
        cfg.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSer);
        
        KafkaProducer<K,V> prod = new KafkaProducer<>(cfg);
        return prod;
	}
	
	public KafkaConsumer<String,String> createConsumer(){
		Class<? extends Deserializer<String>> clazz = StringDeserializer.class;
		KafkaConsumer<String,String> consumer = createConsumer(clazz, clazz);
    	
    	return consumer;
	}
	
	public <K,V> KafkaConsumer<K, V> createConsumer(Class<? extends Deserializer<K>> keyDeser, Class<? extends Deserializer<V>> valueDeser) {
        // Build config
        Map<String, Object> cfg = buildClientConfig();
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeser);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeser);
        cfg.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        
        String clientid;
        String grpid;
    	synchronized (this) {
    		grpid = "test-consumer-groupid" + System.currentTimeMillis();
    		clientid = getClass().getSimpleName() + "-Consumer-" + System.currentTimeMillis();
		}
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, grpid);
        cfg.put(ConsumerConfig.CLIENT_ID_CONFIG, clientid);
        
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        return new KafkaConsumer<>(cfg);
	}
	
	@Override
	public void close() throws Exception {
		this.stop();
	}
	
	public void stop() {
		LOG.info("Shutting down Kafka Standalone Server");
		try {
			if (this.kafka != null) {
				this.kafka.shutdown();
			}

			if (this.zookeeper != null) {
				this.zookeeper.close();
			}

			if (Files.exists(this.kafkaLogDir)) {
				Files.walkFileTree(this.kafkaLogDir, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
						Files.deleteIfExists(file);
						return FileVisitResult.CONTINUE;
					}
					@Override
					public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
						Files.deleteIfExists(dir);
						return FileVisitResult.CONTINUE;
					}
				});
			}
		} catch (Exception e) {
			LOG.error(e);
		}
		this.kafka = null;
		this.zookeeper = null;
		this.kafkaPort = null;
	}
	
    private AdminClient getAdminClient() {
        return KafkaAdminClient.create(buildClientConfig());
    }
    
    /**
     * Internal helper method to build a default configuration.
     */
    private Map<String, Object> buildClientConfig() {
    	String id;
    	synchronized (this) {
			id = "test-consumer-id" + System.currentTimeMillis();
		}
    	
        final Map<String, Object> cfg = new java.util.HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConnectString());
        cfg.put(ConsumerConfig.CLIENT_ID_CONFIG, id);
        
        return cfg;
    }
	
	private KafkaConfig buildKafkaConfig(String zookeeperQuorum, int kafkaPort) {
		String logdirname = "kafkaMaven";
		try {
			this.kafkaLogDir = Files.createTempDirectory(logdirname);
		} catch (IOException e) {
			String msg = String.format("Failed to create log directory %s for Kafka Server.", logdirname);
			LOG.error(msg, e);
		}

		Properties props = new Properties();
		props.put("broker.id", "1");
		props.put("log.dirs", this.kafkaLogDir.toAbsolutePath().toString());
		props.put("zookeeper.connect", zookeeperQuorum);
		
		
		props.put("auto.offset.reset", "earliest");
		props.put("zookeeper.session.timeout.ms", "30000");
		props.put("auto.create.topics.enable", "true");
		
		props.put("host.name", this.kafkaHostname);
		props.put("port", kafkaPort + "");
		props.put("advertised.host.name", this.kafkaHostname);
		props.put("advertised.port", kafkaPort);
		props.put("advertised.listeners", "PLAINTEXT://" + this.kafkaHostname + ":" + kafkaPort);
		props.put("listeners", "PLAINTEXT://" + this.kafkaHostname + ":" + kafkaPort);
		
		
        // Lower active threads.
		props.put("num.io.threads", "2");
		props.put("num.network.threads", "2");
		props.put("log.flush.interval.messages", "1");
        
        // Define replication factor for internal topics to 1
		props.put("offsets.topic.num.partitions", "1");
		props.put("offsets.topic.replication.factor", "1");
		props.put("offset.storage.replication.factor", "1");
		props.put("transaction.state.log.replication.factor", "1");
		props.put("transaction.state.log.min.isr", "1");
		props.put("transaction.state.log.num.partitions", "4");
		props.put("config.storage.replication.factor", "1");
		props.put("status.storage.replication.factor", "1");
		props.put("default.replication.factor", "1");

		
		if (this.overrideProps.size() > 0) {
			props.putAll(this.overrideProps);
		}
		
		return new KafkaConfig(props);
	}
	
	private KafkaServerStartable getKafka(TestingServer zook, int port) {
		KafkaConfig kafkaConfig = buildKafkaConfig(zook.getConnectString(), port);

		KafkaServerStartable kafka = new KafkaServerStartable(kafkaConfig);
		kafka.startup();
		
		return kafka;
	}
	
	private TestingServer getZookeeper(int port) throws Exception {
		InstanceSpec is = new InstanceSpec(null, port, -1, -1, true, -1, -1, 1000);
		TestingServer ts = new TestingServer(is, true);
		return ts;
	}
}
