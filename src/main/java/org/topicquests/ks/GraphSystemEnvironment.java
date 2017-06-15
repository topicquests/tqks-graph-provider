/*
 * Copyright 2015, TopicQuests
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.topicquests.ks;

import org.topicquests.support.RootEnvironment;
import org.topicquests.backside.kafka.KafkaBacksideEnvironment;
import org.topicquests.ks.graph.GraphEnvironment;
import org.topicquests.ks.graph.api.IGraphProvider;
import org.topicquests.ks.kafka.KafkaProducer;
import org.topicquests.node.provider.ProviderEnvironment;

/**
 * @author park
 *
 */
public class GraphSystemEnvironment extends RootEnvironment  {
	private ProviderEnvironment provider;
	private GraphEnvironment graphEnvironment;
	private KafkaBacksideEnvironment kafka;
	private KafkaProducer kProducer;
	private StatisticsUtility stats;
	private IGraphProvider database;

	/**
	 * 
	 */
	public GraphSystemEnvironment() {
		super("graph-props.xml", "logger.properties");
		provider = ProviderEnvironment.getInstance();
		if (provider == null)
			provider = new ProviderEnvironment();
		graphEnvironment = new GraphEnvironment(this);
		stats = new StatisticsUtility();
		database = graphEnvironment.getDatabase();
		try {
			kafka = new KafkaBacksideEnvironment();
			//kProducer = new KafkaProducer(kafka);
		} catch (Exception e) {
			logError(e.getMessage(), e);
			e.printStackTrace();
			shutDown();
			System.exit(1);
		}
		graphEnvironment = new GraphEnvironment(this);
		logDebug("GraphEnvironment Started");
	}
	
	public IGraphProvider getGraphProvider() {
		return database;
	}

	
	public KafkaProducer getkafkaProducer() {
		return kProducer;
	}
	
	public ProviderEnvironment getProvider() {
		return provider;
	}
	
	public GraphEnvironment getGraphEnvironment() {
		return graphEnvironment;
	}
	
	public StatisticsUtility getStats() {
		return stats;
	}
	
	public void replaceStatisticsUtility(StatisticsUtility util) {
		stats = util;
	}

	public void shutDown() {
		stats.saveStats();
	}
}
