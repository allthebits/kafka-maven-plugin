/**
 * 
 */
package com.github.charithe.kafka;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/**
 * @author vagrant
 * Created On: 2018-05-21 15:46:08May 21, 2018
 *
 * com.github.charithe.kafka.ConsumeKafkaMessage
**/
@Mojo (name = "consume-kafka-message", defaultPhase=LifecyclePhase.INTEGRATION_TEST)
public class ConsumeKafkaMessage extends AbstractMojo {
    @Parameter (name="topic", defaultValue = "defaultTopic")
    private String topic;
    
    @Parameter (name="messageRegex", defaultValue="")
    private String messageRegex;
    
    @Parameter (name="pollTime", defaultValue = "MAX_VALUE")
    private String pollTime;
    
    @Parameter( name="project", defaultValue = "${project}", readonly = true, required=true )
    private MavenProject project;

	@Override
	public void execute() throws MojoExecutionException, MojoFailureException {
		Consumer<String, String> consumer = KafkaStandalone.INSTANCE.createConsumer();
		
		consumer.subscribe(Collections.singletonList(this.topic));
		ConsumerRecords<String, String> cr = consumer.poll(getPollTime());
		
        Iterator<ConsumerRecord<String,String>> iter = cr.iterator();
        
        boolean containsMessage = false;
        
        for ( ; iter.hasNext() ; ) {
        	ConsumerRecord record = iter.next();
        	
        	String recordValue = record.value().toString();
        	if (! containsMessage) {
        		containsMessage = matches(recordValue);
        	}
        }
        
        Properties props = this.project.getProperties();
        props.setProperty("consume-kafka-message.success", "" + containsMessage);
        if (! containsMessage) {
        	String msg = String.format("The topic %s did not contain a message matching the regex %s.", this.topic, getMessageRegex());
        	//throw new MojoFailureException(msg);
        	props.setProperty("consume-kafka-message.message", msg);
        }
        consumer.commitAsync();
	}
	
	private String getMessageRegex() {
		if (this.messageRegex == null && this.messageRegex.trim().length() < 1) return "null";
		return this.messageRegex.trim();
	}
	private boolean matches(String value) {
		if (this.messageRegex == null && this.messageRegex.trim().length() < 1) return true;
		return value.matches(this.messageRegex.trim());
	}
	
	private long getPollTime() {
		try {
			long pollTime = Long.parseLong(this.pollTime.trim());
			return pollTime;
		} catch (Exception ex) {
		}
		return Long.MAX_VALUE;
	}
}
