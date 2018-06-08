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
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
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
	private static final String MVN_SUCCESS = "consume-kafka-message.success";
	private static final String MVN_MESSAGE = "consume-kafka-message.message";
	
    @Parameter (name="topic", defaultValue = "defaultTopic")
    private String topic;
    
    @Parameter (name="messageRegex", defaultValue="")
    private String messageRegex;
    
    @Parameter (name="resultId",required=true)
    private String resultId;
    
    @Parameter (name="pollTime", defaultValue = "MAX_VALUE")
    private String pollTime;
    
    @Parameter( name="project", defaultValue = "${project}", readonly = true, required=true )
    private MavenProject project;
    
    public static void main(String... sa) throws Exception {
    	ConsumeKafkaMessage mojo = new ConsumeKafkaMessage();
    }

	@Override
	public void execute() throws MojoExecutionException, MojoFailureException {
		/**
		if (project != null && project.getExecutionProject() != null && project.getExecutionProject().getProperties() != null) {
			getLog().info("ExecutionProject ID: " + project.getExecutionProject().getId());
			project.getExecutionProject().get
			getLog().info("ExecutionProject Properties: " + project.getExecutionProject().getProperties());
		} else {
			getLog().info("ExecutionProject Properties: NONE");
		}
		**/
		Consumer<String, String> consumer = KafkaStandalone.INSTANCE.createConsumer();
		
		consumer.subscribe(Collections.singletonList(this.topic));
		ConsumerRecords<String, String> cr = consumer.poll(getPollTime());
		
        Iterator<ConsumerRecord<String,String>> iter = cr.iterator();
        
        boolean containsMessage = false;
        String foundMessage = null;
        
        for ( ; iter.hasNext() ; ) {
        	ConsumerRecord record = iter.next();
        	
        	String recordValue = record.value().toString();
        	if (! containsMessage) {
        		containsMessage = matches(recordValue);
        		if (containsMessage) {
        			foundMessage = recordValue;
        		}
        	}
        }
        
        Properties props = this.project.getProperties();
        props.setProperty(MVN_SUCCESS + "." + this.resultId, "" + containsMessage);
        
        if (! containsMessage) {
        	String msg = String.format("The topic %s did not contain a message matching the regex %s.", this.topic, getMessageRegex());
        	setMessage(msg);
        } else {
        	setMessage(foundMessage);
        }
        consumer.commitAsync();
	}
	
	private void setMessage(String message) {
		Properties props = this.project.getProperties();
		final String key = MVN_MESSAGE + "." + this.resultId;
		
		String existing = props.getProperty(key);
		if (existing == null) {
			props.setProperty(key, message);
		} else {
			props.setProperty(key, existing + "\n\n" + message);
		}
	}
	
	private String getMessageRegex() {
		if (this.messageRegex == null && this.messageRegex.trim().length() < 1) return "null";
		return this.messageRegex.trim();
	}
	private boolean matches(String value) {
		if (this.messageRegex == null && this.messageRegex.trim().length() < 1) return true;
		boolean matches = value.matches(this.messageRegex.trim());
		String msg = String.format("Topic = %s, Regex = %s, String = %s, Matches = %b", this.topic, this.messageRegex, value, matches);
		getLog().info(msg);
		return matches;
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
