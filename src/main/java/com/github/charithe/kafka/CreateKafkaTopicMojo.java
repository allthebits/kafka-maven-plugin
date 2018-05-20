package com.github.charithe.kafka;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

@Mojo (name = "create-kafka-topic", defaultPhase=LifecyclePhase.INTEGRATION_TEST)
public class CreateKafkaTopicMojo extends AbstractMojo {
	
    @Parameter (name="topic", defaultValue = "defaultTopic")
    private String topic;
    
    private int noPartitions = 1;
    
    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
    	String[] sa = this.topic.split(",");
    	for (String oneTopic : sa) {
    		if (oneTopic.trim().length() < 1) continue;
    		KafkaStandalone.INSTANCE.createTopic(oneTopic, this.noPartitions);
    		
        	if (! KafkaStandalone.INSTANCE.topicExists(this.topic)) {
        		String msg = String.format("The topic %s did not get created.", this.topic);
        		throw new MojoFailureException(msg);
        	}
        	
        	String msg = String.format("Successfully Created topic %s.", oneTopic);
        	getLog().info(msg);
    	}
    }
}
