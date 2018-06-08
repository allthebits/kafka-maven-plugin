/**
 * 
 */
package com.github.charithe.kafka;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/**
 * @author vagrant
 * Created On: 2018-05-21 12:05:18May 21, 2018
 *
 * com.github.charithe.kafka.ProduceKafkaMessages
**/

@Mojo (name = "produce-kafka-message", defaultPhase=LifecyclePhase.INTEGRATION_TEST)
public class ProduceKafkaMessages extends AbstractMojo {
	private static final String MVN_PROPERTY = "produce-kafka-message.keys";
	
    @Parameter (name="topic", defaultValue = "defaultTopic")
    private String topic;
    
    @Parameter (name="message", required = true)
    private String message;
    
    @Parameter (name="key", required = true)
    private String key;
    
    @Parameter( name="project", defaultValue = "${project}", readonly = true, required=true )
    private MavenProject project;
	
    public static void main(String... sa) throws Exception {
    	ProduceKafkaMessages mojo = new ProduceKafkaMessages();
    	mojo.topic = "al.raw";
    	mojo.key = "testMessage002";
    	mojo.message = "/home/vagrant/git/martel-java/kafka-integration/src/test/resources/testData01.json";
    	mojo.project = new MavenProject();
    	//** Make sure Properties are not NULL
    	mojo.project.getProperties().getProperty("test");
    	
    	KafkaStandalone.INSTANCE.configure(KafkaStandalone.ZOOKEEPER_TESTING_PORT, KafkaStandalone.KAFKA_TESTING_PORT);
    	
    	mojo.execute();
    }
    
	@Override
	public void execute() throws MojoExecutionException, MojoFailureException {
		String key = getKey();
		
		try {
			String msg = getMessage();
			Producer<String, String> prod = KafkaStandalone.INSTANCE.createProducer();
			
			ProducerRecord<String,String> pr = new ProducerRecord<String,String>(this.topic, key, msg);
			prod.send(pr).get();
			
	        prod.flush();
	        prod.close();
		} catch (InterruptedException | ExecutionException | IOException e) {
			throw new MojoExecutionException(e.getMessage(), e);
		}
        Properties props = this.project.getProperties();
        String val = props.getProperty(MVN_PROPERTY, "");
        if (val.length() > 0) {
        	val += ",";
        }
        val += key;
        props.setProperty(MVN_PROPERTY, val);
	}
	
	private String getKey() {
		if (this.key != null && this.key.trim().length() > 0) return this.key.trim();
		
		String key = System.currentTimeMillis() + "";
		return key;
	}
    
	private String getMessage() throws IOException {
		String message;
		if (this.message.startsWith("http")) {
			URL url = new URL(this.message);
			message = IOUtils.toString(url, Charset.forName("UTF-8"));
		} else {
			File f = new File(this.message);
			if (f.exists()) {
				message = FileUtils.readFileToString(f, Charset.forName("UTF-8"));
			} else {
				message = this.message;
			}
		}
		if (message == null || message.length() < 1) {
			throw new IOException("No message was provided");
		}
		return message;
	}
}
