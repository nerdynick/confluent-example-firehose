package io.confluent.firehose;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.base.Splitter;
import com.google.common.base.Splitter.MapSplitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import io.confluent.config.ConfigUtils;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
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
	
	private static final ObjectMapper objectMapper = new ObjectMapper();

	static {
		options.addOption("c", "config", true, "Comma Seperated key=value pairs of configs");
		options.addOption("f", "config-file", true, "Configs from a file");
		options.addOption("g", "gateway", true, "Prometheus PushGateway URL");
		options.addOption("j", "job", true, "Prometheus PushGateway Job Name");
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
		Defaults.addProperty(CONSUMER_PREFIX + "." + ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 500);
		
		
		//Setup Jackson
		objectMapper.registerModule(new AfterburnerModule());
	}

	private final Configuration config;
	private Consumer<byte[], byte[]> consumer;
	private Thread consumerThread;
	private final AtomicBoolean stop = new AtomicBoolean(false);
	private final Cache<String, Gauge> gauges = CacheBuilder.newBuilder()
		       .maximumSize(1000)
		       .expireAfterWrite(10, TimeUnit.MINUTES)
		       .build();
	

	private PushGateway gateway;

	protected PrometheusPusher(Configuration config) {
		this.config = config;
	}

	public void start() {
		final Map<String, Object> consumerConfigs = ConfigUtils.toMap(config.subset(CONSUMER_PREFIX));
		consumer = new KafkaConsumer<>(consumerConfigs, new ByteArrayDeserializer(), new ByteArrayDeserializer());
		consumer.subscribe(Lists.newArrayList(config.getString(CONFIG_TOPIC, "metrics.v1")));
		
		gateway = new PushGateway(config.getString(CONFIG_PROM_GATEWAY));

		consumerThread = new Thread(new ConsumerRunnable(), "KafkaConsumerThread");
		consumerThread.start();
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
	
	protected Callable<Gauge> createGauge(final FirehoseMetric metric, final CollectorRegistry registry) {
		return ()->{
			return Gauge.build()
				.name(metric.name)
				.namespace(metric.component)
				.labelNames(metric.labelsAsArray())
				.register(registry);
		};
	}
	
	protected FirehoseMetric parseMetric(byte[] metricJsonBytes) throws IOException {
		return objectMapper.readValue(metricJsonBytes, FirehoseMetric.class);
	}
	
	protected void handleMetric(byte[] value, final CollectorRegistry registry) throws IOException, ExecutionException{
		if(LOG.isDebugEnabled()) {
			LOG.debug("Parsing JSON: {}", new String(value));
		}
		
		final FirehoseMetric metric = parseMetric(value);
		LOG.debug("Metrics: {}", metric);
		
		final Gauge g = gauges.get(metric.name, this.createGauge(metric, registry));
		if(metric.labelValues().isEmpty()) {
			g.set(metric.value);
		} else {
			g.labels(metric.labelValuesAsArray())
				.set(metric.value);
		}
	}

	private class ConsumerRunnable implements Runnable {
		private final CollectorRegistry registry = new CollectorRegistry();
		private final String jobName = config.getString(CONFIG_PROM_JOB, CONFIG_PROM_JOB_DEFAULT);
		private final Map<TopicPartition, OffsetAndMetadata> commitQueue = new ConcurrentHashMap<>();
		
		@Override
		public void run() {
			try {
				while (!stop.get()) {
					ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(60));
					for(ConsumerRecord<byte[], byte[]> rec: records) {
						try {
							handleMetric(rec.value(), registry);
						} catch (IOException | ExecutionException e) {
							LOG.error("Failed to parse message", e);
						}
					}
					
					try {
						gateway.pushAdd(registry, jobName);
					} catch (IOException e) {
						LOG.error("Failed to push to gateway", e);
					}
					consumer.commitAsync();
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
		
		/*
		 * The bellow code is a sample (and Untested) approach to committing offsets
		 * in a controlled manor leveraging CallableFutures, with threads post KafkaConsumer.
		 */
		
		
		public void _run() {
			final Timer timer = new Timer(true);
	        timer.scheduleAtFixedRate(new TimerTask() {
				@Override
				public void run() {
					commitOffsets();
				}
	        }, 0, 10*1000);
			try {
		        
				while (!stop.get()) {
					ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(60));
					for(TopicPartition partition: records.partitions()){
						final List<ConsumerRecord<byte[], byte[]>> recs = records.records(partition);
						handleBundle(partition, recs)
							.thenApply((offset)->{
								//This runs after we've parsed all the data and done work with it.
								//In this case we want to publish to the Prometheus gateway
								try {
									gateway.pushAdd(registry, jobName);
								} catch (IOException e) {
									LOG.error("Failed to push to gateway", e);
								}
								return offset;
							})
							.whenComplete((offset,error)->{
								if(error != null) {
									//TODO: Handle Error
									LOG.error("Failed to handle partition batch", error);
								} else {
									LOG.info("Adding Offset to Commit Queue");
									commitQueue.compute(partition, (k,v)->{
										if(v == null) {
											return offset;
										} else {
											if(v.offset() <= offset.offset()) {
												return v;
											} else {
												return offset;
											}
										}
									});
								}
							});
					}
					
					LOG.info("Pausing for: {}", records.partitions());
					consumer.pause(records.partitions());
					
				}
			} catch (WakeupException e) {
				LOG.info("Consumer wokeup, checking if I should stop");
				// Ignore exception if closing
				if (!stop.get())
					throw e;
			} finally {
				timer.cancel();
				commitOffsets();
				LOG.warn("Stopping/Closing Consumer");
				consumer.close();
			}
		}
		
		private void commitOffsets() {
			LOG.debug("Startig commit");
			final Map<TopicPartition, OffsetAndMetadata> commit = new HashMap<>();
			
			commitQueue.replaceAll((topic,offset)->{
				commit.put(topic, offset);
				return null;
			});
			
			LOG.debug("Commiting: {}", commit);
			
			consumer.commitAsync(commit, new OffsetCommitCallback() {
				@Override
				public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
					if(exception != null) {
						//TODO: Handle commit exception
						LOG.error("Failed to commit offsets", exception);
					} else {
						LOG.info("Resuming for: {}", offsets.keySet());
						consumer.resume(offsets.keySet());
					}
				}
			});
		}
		
		public CompletableFuture<OffsetAndMetadata> handleBundle(TopicPartition partition, List<ConsumerRecord<byte[], byte[]>> records) {
			return CompletableFuture.supplyAsync(new Supplier<OffsetAndMetadata>(){
				@Override
				public OffsetAndMetadata get() {
					long offset = 0;
					for(ConsumerRecord<byte[], byte[]> rec: records) {
						if(rec.offset() > offset) {
							offset = rec.offset();
						}
						try {
							handleMetric(rec.value(), registry);
						} catch (IOException | ExecutionException e) {
							LOG.error("Failed to parse message", e);
						}
					}
					
					return new OffsetAndMetadata(offset);
				}
			});
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
			
			if(line.hasOption('g')) {
				configs.addProperty(CONFIG_PROM_GATEWAY, line.getOptionValue('g'));
			}
			
			if(line.hasOption('j')) {
				configs.addProperty(CONFIG_PROM_JOB, line.getOptionValue('j'));
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
