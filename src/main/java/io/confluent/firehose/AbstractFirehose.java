package io.confluent.firehose;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.clients.consumer.Consumer;
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
import com.google.common.collect.Lists;

import io.confluent.config.ConfigUtils;

/**
 * Abstract class to handle Consuming from the Confluent Cloud Metrics Firehose
 *  
 * @author Nikoleta Verbeck
 *
 */
public abstract class AbstractFirehose implements Closeable {
	private static final ObjectMapper objectMapper = new ObjectMapper();
	
	static {
		//Setup Jackson
		objectMapper.registerModule(new AfterburnerModule());
	}
	
	private final Logger LOG;
	
	private Consumer<byte[], byte[]> consumer;
	private Thread consumerThread;
	private final AtomicBoolean stop = new AtomicBoolean(false);
	
	protected final Configuration config;
	
	protected AbstractFirehose(Configuration config) {
		this.config = config;
		LOG = LoggerFactory.getLogger(this.getClass()+"."+AbstractFirehose.class.getSimpleName());
	}
	
	/**
	 * Start KafkConsumer and Thread (KafkaConsumerThread to handle consuming records.
	 * Call attach() right after to attache the Main Thread to the Consumer Thread.
	 */
	public void start() {
		final Map<String, Object> consumerConfigs = ConfigUtils.toMap(config.subset(Configs.CONSUMER_PREFIX));
		consumer = new KafkaConsumer<>(consumerConfigs, new ByteArrayDeserializer(), new ByteArrayDeserializer());
		consumer.subscribe(Lists.newArrayList(config.getString(Configs.CONFIG_CONSUMER_TOPIC)));
		
		consumerThread = new Thread(new ConsumerRunnable(), "KafkaConsumerThread");
		consumerThread.start();
	}
	
	/**
	 * Attaches current thread to the KafkaConsumer Thread.
	 * @throws InterruptedException
	 */
	public void attach() throws InterruptedException {
		if (consumerThread != null && consumerThread.isAlive()) {
			consumerThread.join();
		}
	}

	/**
	 * Trigger shutdown and close of KafkaConsumer and it's Consumer Thread
	 */
	@Override
	public void close() throws IOException {
		LOG.warn("Recieved Close. Stoping everything.");
		stop.set(true);
		if (this.consumer != null) {
			this.consumer.wakeup();
		}
	}
	
	protected FirehoseMetric parseMetric(byte[] metricJsonBytes) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("Parsing JSON: {}", new String(metricJsonBytes));
		}
		final FirehoseMetric metric = objectMapper.readValue(metricJsonBytes, FirehoseMetric.class);
		LOG.debug("Metrics: {}", metric);
		return metric;
	}
	
	protected abstract void handle(FirehoseMetric metric) throws Exception;
	protected abstract void endOfSet() throws Exception;
	
	private class ConsumerRunnable implements Runnable {
		
		@Override
		public void run() {
			try {
				while (!stop.get()) {
					ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(60));
					for(ConsumerRecord<byte[], byte[]> rec: records) {
						try {
							FirehoseMetric metric = parseMetric(rec.value());
							handle(metric);
						} catch (Exception e) {
							LOG.error("Failed to parse and handle firehose metric", e);
						}
					}
					
					try {
						endOfSet();
					} catch (Exception e) {
						LOG.error("Failed to handle end of set", e);
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
	}
	
	/**
	 * The bellow code is a sample (and Untested) approach to committing offsets
	 * in a controlled manor leveraging CallableFutures, with threads post KafkaConsumer.
	 */
	private class ConsumerRunnableAsync implements Runnable{
		private final Map<TopicPartition, OffsetAndMetadata> commitQueue = new ConcurrentHashMap<>();
		
		@Override
		public void run() {
			//This timer is to do Consumer Offset Regular commit at a given interval 
			final Timer timer = new Timer(true);
	        timer.scheduleAtFixedRate(new TimerTask() {
				@Override
				public void run() {
					commitOffsets();
				}
	        }, 0, TimeUnit.SECONDS.toMillis(1));
	        
			try {
		        //This is the main KafkaConsumer loop
				while (!stop.get()) {
					
					//Grab a batch of records
					ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(60));
					
					//Break each partition of records appart and send to background threads
					for(TopicPartition partition: records.partitions()){
						final List<ConsumerRecord<byte[], byte[]>> recs = records.records(partition);
						
						handleBundle(partition, recs) //Starts the processing chain
							.thenApply((offset)->{
								//This runs after we've parsed all the data and done work with it.
								//In this case we want to publish to the Prometheus gateway
								try {
									endOfSet();
								} catch (Exception e) {
									LOG.error("Failed to handle end of set", e);
								}
								return offset;
							})
							.whenComplete((offset,error)->{ //When done we need to add the offset to the queue of messages to commit
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
						//Record highest Offset we've got for this Partition so far
						if(rec.offset() > offset) {
							offset = rec.offset();
						}
						
						try {
							FirehoseMetric metric = parseMetric(rec.value());
							handle(metric);
						} catch (Exception e) {
							LOG.error("Failed to parse and handle firehose metric", e);
						}
					}
					
					return new OffsetAndMetadata(offset);
				}
			});
		}

	}
}
