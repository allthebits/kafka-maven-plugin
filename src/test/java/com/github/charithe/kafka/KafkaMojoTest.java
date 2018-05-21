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

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.maven.plugin.Mojo;
import org.apache.maven.plugin.MojoExecution;
import org.apache.maven.plugin.testing.MojoRule;
import org.codehaus.plexus.configuration.xml.XmlPlexusConfiguration;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.junit.Rule;
import org.junit.Test;


public class KafkaMojoTest {
	private static final String ARTIFACT_ID = "kafka-maven-plugin";
	private static final String GROUP_ID = "com.github.charithe";

    private static final String TOPIC = "topicX";
    private static final String KEY = "keyX";
    private static final String VALUE = "valueX";
    
    private final File pom = new File("src/test/resources/unit/project-to-test/pom.xml");
    
    @Rule
    public MojoRule rule = new MojoRule() {
        @Override
        protected void before() throws Throwable {
        }

        @Override
        protected void after() {
        }
    };
    
    @Test
    public void testCreateTopic() throws Exception {
        assertThat(this.pom, is(notNullValue()));
        assertTrue(this.pom.exists());
        
        Mojo startMojo = MojoHelper.findMojo(this.pom, this.rule, ARTIFACT_ID, GROUP_ID, "start-kafka-broker");
        assertThat(startMojo, is(notNullValue()));
        startMojo.execute();
        
        Mojo createMojo = MojoHelper.findMojo(this.pom, this.rule, ARTIFACT_ID, GROUP_ID, "create-kafka-topic");
        assertThat(createMojo, is(notNullValue()));
        createMojo.execute();
        
        final String[] sa = new String[] { "defaultTestTopic", "defaultTestTopic2", "defaultTestTopic3" };
        
      //** TOPIC CREATE SHOULD BE FALSE BECAUSE IT SHOULD ALREADY BE CREATED BY CREATEMOJO
        for (String s : sa) {
            boolean created = KafkaStandalone.INSTANCE.createTopic(s);
            String msg = String.format("Result %b for topic %s should have been FALSE", created, s);
            assertFalse(msg, created);
            
            produceAndConsumeMessages(s);
        }

        
        Mojo stopMojo = MojoHelper.findMojo(this.pom, this.rule, ARTIFACT_ID, GROUP_ID, "stop-kafka-broker");
        assertThat(stopMojo, is(notNullValue()));
        stopMojo.execute();
    }

    
    public void testBrokerStartupCreateTopicAndShutdown() throws Exception {
        assertThat(this.pom, is(notNullValue()));
        assertTrue(this.pom.exists());

        Mojo startMojo = MojoHelper.findMojo(this.pom, this.rule, "start-kafka-broker");
        assertThat(startMojo, is(notNullValue()));

        Mojo stopMojo = MojoHelper.findMojo(this.pom, this.rule, "stop-kafka-broker");
        assertThat(stopMojo, is(notNullValue()));

        
        startMojo.execute();
        
        KafkaStandalone.INSTANCE.createTopic(TOPIC);
        
        stopMojo.execute();
    }
    
    public void testBrokerStartupCreateTopicProduceMessagesAndConsume() throws Exception {
        assertThat(this.pom, is(notNullValue()));
        assertTrue(this.pom.exists());

        Mojo startMojo = MojoHelper.findMojo(this.pom, this.rule, "start-kafka-broker");
        assertThat(startMojo, is(notNullValue()));

        Mojo stopMojo = MojoHelper.findMojo(this.pom, this.rule, "stop-kafka-broker");
        assertThat(stopMojo, is(notNullValue()));

        
        startMojo.execute();
        
        KafkaStandalone.INSTANCE.createTopic(TOPIC);
        
        produceAndConsumeMessages(TOPIC);
        
        stopMojo.execute();
    }
    
    private static void produceAndConsumeMessages(String topic) throws Exception {
        Producer<String, String> producer = MojoHelper.createProducer();
        ProducerRecord<String,String> pr = new ProducerRecord<String,String>(topic, KEY, VALUE);
        
        //** Send same record multiple times
        producer.send(pr).get();
        producer.send(pr).get();
        RecordMetadata rmd = producer.send(pr).get();
        
        assertNotNull(rmd.topic());
        assertTrue(rmd.topic().length() > 0);
        
        producer.flush();
        producer.close();
        
        
        Consumer<String,String> consumer = MojoHelper.createConsumer(topic);
        ConsumerRecords<String, String> cr = consumer.poll(Long.MAX_VALUE);
        
        assertNotNull(cr);
        
        Iterator<ConsumerRecord<String,String>> iter = cr.iterator();
        assertTrue(iter.hasNext());
        
        for ( ; iter.hasNext() ; ) {
        	ConsumerRecord record = iter.next();
        	assertNotNull(record.key());
        	assertNotNull(record.value());
        	assertTrue(VALUE.equals(record.value()));
        }
        
        consumer.commitAsync();
        consumer.close();
    }
}
