package io.confluent.firehose;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class Configs {
	public static final String ENV_PROPERTIES_PREFIX="CONFLUENT_";	
	
	public static final Configuration ConsumerDefaults;
	public static final Configuration PrometheusGatewayDefaults;
	
	public static final String CONSUMER_PREFIX = "consumer";
	public static final String PROMETHEUS_PREFIX = "prometheus";
	
	public static final String CONFIG_CONSUMER_TOPIC = CONSUMER_PREFIX+".topic";
	public static final String CONFIG_CONSUMER_GROUP_ID = CONSUMER_PREFIX+"."+ConsumerConfig.GROUP_ID_CONFIG;
	
	public static final String CONFIG_PROM_GATEWAY = PROMETHEUS_PREFIX + ".gateway";
	public static final String CONFIG_PROM_GATEWAY_JOB = PROMETHEUS_PREFIX + ".job";
	
	public static final String CONFIG_PROM_PULLER_LISTENER = PROMETHEUS_PREFIX + ".listener";

	
	static {
		ConsumerDefaults = new BaseConfiguration();
		// Kafka Consumer Defaults
		ConsumerDefaults.addProperty(CONFIG_CONSUMER_TOPIC, "metrics.v1");
		ConsumerDefaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
		ConsumerDefaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		ConsumerDefaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		ConsumerDefaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		ConsumerDefaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
		ConsumerDefaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
		ConsumerDefaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);
		ConsumerDefaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 16000);
		ConsumerDefaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 500);
		
		PrometheusGatewayDefaults = new BaseConfiguration();
	}
}
