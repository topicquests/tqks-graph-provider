/**
 * 
 */
package org.topicquests.ks.kafka;

import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.topicquests.backside.kafka.KafkaBacksideEnvironment;
import org.topicquests.backside.kafka.apps.AbstractKafkaApp;
import org.topicquests.backside.kafka.consumer.MessageConsumer;
import org.topicquests.backside.kafka.consumer.api.IMessageConsumerListener;
import org.topicquests.backside.kafka.producer.MessageProducer;

/**
 * @author Admin
 *
 */
public class KafkaProducer extends AbstractKafkaApp implements IMessageConsumerListener {
	private MessageProducer producer;
	private MessageConsumer consumer;
	private String name;

	/**
	 * @param env
	 */
	public KafkaProducer(KafkaBacksideEnvironment env) {
		super(env);
		name = "TQElasticKSProducer"; //TODO make into a config value
		String clientId = Long.toString(System.currentTimeMillis());
		producer = new MessageProducer(environment, name, clientId, false);
		consumer = new MessageConsumer(environment, name, this);
		producer.start();
		consumer.start();
	}

	/**
	 * This will be a JSON string of the form {verb:<verb>,cargo:<cargo>)
	 * @param msg
	 * @param partition
	 */
	public void sendMessage(String msg, Integer partition) {
		environment.logDebug("TQElasticKSProducer "+msg);
		producer.sendMessage(msg, partition);
	}

	/* (non-Javadoc)
	 * @see org.topicquests.backside.kafka.consumer.api.IMessageConsumerListener#acceptRecords(org.apache.kafka.clients.consumer.ConsumerRecords)
	 */
	@Override
	public void acceptRecords(ConsumerRecords<String, String> records) {
		System.out.println("TQElasticKSProducer "+records.count());
		Iterator<ConsumerRecord<String, String>>itr = records.records(name).iterator();
		ConsumerRecord<String, String>cr;
		String val;
		while (itr.hasNext()) {
			cr = itr.next();
			val = cr.value();
			//for DEBUG
			System.out.println("V "+val);
			//TODO Do we need to do anything with these?
		}
	}

	/* (non-Javadoc)
	 * @see org.topicquests.backside.kafka.apps.AbstractKafkaApp#close()
	 */
	@Override
	public void close() {
		producer.close();
		consumer.close();
	}

}
