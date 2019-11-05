package io.confluent.firehose;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.EnvironmentConfiguration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.SystemConfiguration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Splitter;
import com.google.common.base.Splitter.MapSplitter;
import com.google.common.collect.Lists;

import io.confluent.config.ConfigUtils;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Gauge.Builder;
import io.prometheus.client.exporter.PushGateway;

public class PrometheusPusher implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(PrometheusPusher.class);

	public static final String CONSUMER_PREFIX = "consumer";
	public static final String PROMETHEUS_PREFIX = "prometheus";
	public static final String CONFIG_TOPIC = "firehose.topic";
	public static final String CONFIG_PROM_GATEWAY = PROMETHEUS_PREFIX + ".gateway";
	public static final String CONFIG_PROM_JOB = PROMETHEUS_PREFIX + ".job";
	public static final String CONFIG_PROM_JOB_DEFAULT = PrometheusPusher.class.getSimpleName();

	public static final Options options = new Options();
	public static final Configuration Defaults;

	static {
		options.addOption("c", "config", true, "Comma Seperated key=value pairs of configs");
		options.addOption("f", "config-file", true, "Configs from a file");
		options.addOption("g", "gateway", true, "Prometheus PushGateway URL");
		options.addOption("h", "help", false, "Print Help");

		Defaults = new BaseConfiguration();
		// Prometheus Gateway Defaults
		Defaults.addProperty(CONFIG_PROM_JOB, CONFIG_PROM_JOB_DEFAULT);

		// Kafka Consumer Defaults
		Defaults.addProperty(CONFIG_TOPIC, "metrics.v1");
		Defaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.GROUP_ID_CONFIG, CONFIG_PROM_JOB_DEFAULT);
		Defaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
		Defaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		Defaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		Defaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		Defaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
		Defaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
		Defaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);
		Defaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 16000);
	}

	private final Configuration config;
	private Consumer<byte[], byte[]> consumer;
	private Thread consumerThread;
	private final AtomicBoolean stop = new AtomicBoolean(false);
	private final JsonFactory factory = new JsonFactory();

	private PushGateway gateway;

	protected PrometheusPusher(Configuration config) {
		this.config = config;
	}

	public void start() {
		final Map<String, Object> consumerConfigs = ConfigUtils.toMap(config.subset(CONSUMER_PREFIX));
		LOG.info("Consumer Configuration:");
		ConfigUtils.printProperties(consumerConfigs, (k, v) -> {
			LOG.info(String.format("\t%-60s = %s", k, v));
		});
		
		consumer = new KafkaConsumer<>(consumerConfigs, new ByteArrayDeserializer(), new ByteArrayDeserializer());
		consumer.subscribe(Lists.newArrayList(config.getString(CONFIG_TOPIC, "metrics.v1")));

		consumerThread = new Thread(new ConsumerRunnable(), "KafkaConsumerThread");
		consumerThread.start();

		gateway = new PushGateway(config.getString(CONFIG_PROM_GATEWAY));
	}

	public void attach() throws InterruptedException {
		if (consumerThread != null && consumerThread.isAlive()) {
			consumerThread.join();
		}
	}

	@Override
	public void close() throws IOException {
		LOG.warn("Recieved Close. Stoping everything.");
		stop.set(true);
		if (this.consumer != null) {
			this.consumer.wakeup();
		}
	}
	
	protected Gauge.Builder parseMetricToGauge(byte[] value) throws JsonParseException, IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("Parsing JSON: {}", new String(value));
		}
		
		final JsonParser parser = factory.createParser(value);
		final Builder gBuilder = Gauge.build();
		while(parser.nextToken() != JsonToken.END_OBJECT) {
			final String tok = parser.getCurrentName();
			
			if("name".equals(tok)) {
				gBuilder.name(parser.nextTextValue());
				
				if(LOG.isDebugEnabled()) {
					LOG.debug("Got Name: {}", parser.getText());
				}
			} else if("tags".equals(tok)) {
				parser.nextToken();
				LOG.debug("Parsing Tags");
				final Map<String, String> labels = new HashMap<>();
				while(parser.nextToken() != JsonToken.END_OBJECT) {
					LOG.debug("Sub Token: {}", parser.getCurrentName());
					String tagName = parser.getCurrentName();
					String tagValue = parser.nextTextValue();
					labels.put(tagName, tagValue);
				}
				LOG.debug("Tags/Labels: {}", labels);
				final Set<String> labelNames = labels.keySet();
				gBuilder.labelNames(labelNames.toArray(new String[labelNames.size()]));
			}
		}
		
		return gBuilder;
	}

	private class ConsumerRunnable implements Runnable {
		private final CollectorRegistry registry = new CollectorRegistry();
		private final String jobName = config.getString(CONFIG_PROM_JOB, CONFIG_PROM_JOB_DEFAULT);
		
		@Override
		public void run() {
			try {
				while (!stop.get()) {
					ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(60));
					for(ConsumerRecord<byte[], byte[]> rec: records) {
						try {
							Gauge g = parseMetricToGauge(rec.value()).register(registry);
						} catch (IOException e) {
							LOG.error("Failed to parse message", e);
						}
					}
					try {
						gateway.pushAdd(registry, jobName);
					} catch (IOException e) {
						LOG.error("Failed to push to gateway", e);
					}
				}
			} catch (WakeupException e) {
				LOG.info("Consumer wokeup, checking if I should stop");
				// Ignore exception if closing
				if (!stop.get())
					throw e;
			} finally {
				LOG.warn("Stopping/Closing Consumer");
				consumer.close();
			}
		}

	}

	protected static void printHelp() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(PrometheusPusher.class.getSimpleName(), options);
	}

	public static void main(String[] args) throws Exception {
		// create the parser
		final CommandLineParser parser = new DefaultParser();
		try {
			// parse the command line arguments
			final CommandLine line = parser.parse(options, args);
			if (line.hasOption('h')) {
				printHelp();
				System.exit(0);
			}

			CompositeConfiguration configs = new CompositeConfiguration();
			configs.addConfiguration(new SystemConfiguration());
			configs.addConfiguration(new EnvironmentConfiguration());
			configs.addConfiguration(Defaults);

			if (line.hasOption('f')) {
				final String file = line.getOptionValue('f');
				configs.addConfigurationFirst(ConfigUtils.newFileConfig(file));
			}

			if (line.hasOption('c')) {
				try {
					final MapSplitter split = Splitter.on(',').omitEmptyStrings().trimResults().withKeyValueSeparator('=');
					final Map<String, String> props = split.split(line.getOptionValue('c'));
					configs.addConfigurationFirst(new MapConfiguration(props));
				} catch (Exception e) {
					LOG.error("Failed to parse configs", e);
					printHelp();
					System.exit(1);
				}
			}

			LOG.info("PrometheusPusher Configuration:");
			ConfigUtils.printProperties(configs, (k, v) -> {
				LOG.info(String.format("\t%-60s = %s", k, v));
			});

			final PrometheusPusher pusher = new PrometheusPusher(configs);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					try {
						LOG.info("Shutting Down");
						pusher.close();
					} catch (IOException e) {
						LOG.error("Failed to shutdown currectly", e);
						System.err.println("Failed to shutdown currectly...");
						System.err.println(e);
					}
				}
			});

			LOG.info("Starting Pusher");
			pusher.start();
			pusher.attach();
		} catch (ParseException exp) {
			// oops, something went wrong
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
			printHelp();
			System.exit(1);
		}
	}
}
