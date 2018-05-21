/**
 * 
 */
package com.github.charithe.kafka;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.maven.plugin.Mojo;
import org.apache.maven.plugin.MojoExecution;
import org.apache.maven.plugin.testing.MojoRule;
import org.codehaus.plexus.configuration.xml.XmlPlexusConfiguration;
import org.codehaus.plexus.util.ReaderFactory;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.codehaus.plexus.util.xml.Xpp3DomBuilder;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

/**
 * @author vagrant Created On: 2018-05-21 08:38:15May 21, 2018
 *
 *         com.github.charithe.kafka.MojoHelper
 **/

final class MojoHelper {
	private MojoHelper() {
	}

	public static Consumer<String, String> createConsumer(String topic) {
		Consumer<String, String> consumer = KafkaStandalone.INSTANCE.createConsumer();

		consumer.subscribe(Collections.singletonList(topic));
		return consumer;
	}

	public static Producer<String, String> createProducer() {
		return KafkaStandalone.INSTANCE.createProducer();
	}
	
	public static Xpp3Dom findFirstChild(Xpp3Dom dom, String childname, Map<String, String> withValues) {
		if (dom == null || dom.getChildCount() < 1) return null;
		Xpp3Dom[] children = dom.getChildren();
		for (Xpp3Dom child : children) {
			String _childName = child.getName();
			if (_childName.equals(childname)) {
				if (withValues == null || withValues.size() < 1) return child;
				if (hasProperties(child, withValues)) {
					return child;
				}
			}
		}
		for (Xpp3Dom child : children) {
			Xpp3Dom found = findFirstChild(child, childname, withValues);
			if (found != null) return found;
		}
		return null;
	}
	
	public static Mojo findMojo(File pom, MojoRule rule, String artifactId, String groupId, String goal) 
			throws Exception {
		Mojo mojo = rule.lookupEmptyMojo(goal, pom);
		
		Xpp3Dom plugin = loadPlugin(pom, groupId, artifactId);
		
		Xpp3Dom execConfig = getExecutionConfiguration(plugin, goal);
		
		XmlPlexusConfiguration xmlCfg;
		if (execConfig != null) {
			xmlCfg = new XmlPlexusConfiguration(execConfig);
		} else {
			MojoExecution exec = rule.newMojoExecution(goal);
			Xpp3Dom dom = exec.getConfiguration();
		    xmlCfg = new XmlPlexusConfiguration(dom);
		}
		
		rule.configureMojo(mojo, xmlCfg);
		
		return mojo;
	}
	
	public static Mojo findMojo(File pom, MojoRule rule, String goal) throws Exception {
		Mojo mojo = rule.lookupEmptyMojo(goal, pom);
		MojoExecution exec = rule.newMojoExecution(goal);
		Xpp3Dom dom = exec.getConfiguration();
		XmlPlexusConfiguration xmlCfg = new XmlPlexusConfiguration(dom);

		rule.configureMojo(mojo, xmlCfg);

		return mojo;
	}
	
	public static Xpp3Dom getExecutionConfiguration(Xpp3Dom plugin, String goalname) {
		Xpp3Dom executions = plugin.getChild("executions");
		if (executions == null || executions.getChildCount() < 1) return null;
		
		Xpp3Dom[] children = executions.getChildren();
		for (Xpp3Dom child : children) {
			Xpp3Dom configuration = child.getChild("configuration");
			if (configuration == null) continue;
			
			Xpp3Dom goals = child.getChild("goals");
			if (goals == null) continue;
			
			Xpp3Dom goal = goals.getChild("goal");
			if (goal == null) continue;
			if (! goal.getValue().equals(goalname)) continue;
			
			return configuration;
		}
		return null;
	}
	
	/**
	 * If <code>withValues</code> is NULL or Empty, this will return TRUE
	 * Otherwise, <code>dom</code> must have a child properties the same name as the keys in <code>withValues</code>
	 * and the text value within the property must match the value for the same key in <code>withValues</code> 
	 * 
	 * @param dom
	 * @param withValues
	 * @return
	 */
	public static boolean hasProperties(Xpp3Dom dom, Map<String, String> withValues) {
		if (withValues == null || withValues.size() < 1) return true;
		
		for (String key : withValues.keySet()) {
			Xpp3Dom child = dom.getChild(key);
			if (child == null) return false;
			
			String matchValue = withValues.get(key);
			String childValue = child.getValue();
			
			if (! matchValue.equals(childValue)) return false;
		}
		return true;
	}
	
	public static Xpp3Dom loadPlugin(File pom, String groupId, String artifactId) throws IOException {
		Reader reader = ReaderFactory.newXmlReader( pom );
		Xpp3Dom pomDom = null;
		try {
			pomDom = Xpp3DomBuilder.build( reader );
		} catch (XmlPullParserException e) {
			throw new IOException(e);
		}
		
		Map<String,String> coords = new java.util.HashMap<>();
		coords.put("groupId", groupId);
		coords.put("artifactId", artifactId);
		
		Xpp3Dom plugin = findFirstChild(pomDom, "plugin", coords);
		return plugin;
	}
}
