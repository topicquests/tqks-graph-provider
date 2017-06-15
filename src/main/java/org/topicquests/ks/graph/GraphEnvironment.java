/**
 * 
 */
package org.topicquests.ks.graph;

import org.topicquests.ks.graph.api.IGraphProvider;
import org.topicquests.ks.GraphSystemEnvironment;
import org.topicquests.node.provider.ProviderEnvironment;
import org.topicquests.support.util.LoggingPlatform;

/**
 * @author park
 *
 */
public class GraphEnvironment {
	private GraphUtility util;
	private GraphSystemEnvironment envirnoment;
	private ProviderEnvironment provider;
	private IGraphProvider database;
	
	/**
	 * 
	 */
	public GraphEnvironment(GraphSystemEnvironment env) {
		envirnoment = env;
		init();
	}

	private void init() {
		provider = envirnoment.getProvider();
		database = new GraphProvider(this, provider.getClient());
		util = new GraphUtility(this, database);
	}
	
	public ProviderEnvironment getProvider() {
		return provider;
	}
	
	public IGraphProvider getDatabase() {
		return database;
	}
	
	public void logDebug(String msg) {
		envirnoment.logDebug(msg);

	}

	public void logError(String msg, Exception e) {
		envirnoment.logError(msg, e);
	}

}
