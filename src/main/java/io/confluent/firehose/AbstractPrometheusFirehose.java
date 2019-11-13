package io.confluent.firehose;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;

public abstract class AbstractPrometheusFirehose extends AbstractFirehose {
	protected final CollectorRegistry registry = new CollectorRegistry();
	private final Cache<String, Gauge> gauges = CacheBuilder.newBuilder()
					.initialCapacity(1000)
					.maximumSize(10000)
					.expireAfterAccess(10, TimeUnit.MINUTES)
					.removalListener(new RemovalListener<String, Gauge>() {
						@Override
						public void onRemoval(RemovalNotification<String, Gauge> notification) {
							registry.unregister(notification.getValue());
						}
					})
					.build();
	
	private final Logger LOG;
	
	protected AbstractPrometheusFirehose(Configuration config) {
		super(config);
		
		LOG = LoggerFactory.getLogger(this.getClass()+"."+AbstractFirehose.class.getSimpleName());
	}

	@Override
	protected void handle(FirehoseMetric metric) throws Exception{
		final Gauge g = gauges.get(metric.name, this.createGauge(metric, registry));
		if(metric.labelValues().isEmpty()) {
			LOG.debug("Updating Single Metric: {}", metric);
			g.set(metric.value);
		} else {
			LOG.debug("Updating Child Metric: {}", metric);
			g.labels(LookupMapper.Lookup(metric.labelsAsArray(), metric.labelValuesAsArray()))
				.set(metric.value);
		}
	}

	protected Callable<Gauge> createGauge(final FirehoseMetric metric, final CollectorRegistry registry) {
		return ()->{
			LOG.debug("Creating new Gauge for Metric: {}", metric);
			return Gauge.build()
				.name(metric.name)
				.namespace(metric.component)
				.labelNames(metric.labelsAsArray())
				.help("Confluent Metric: "+ metric.name)
				.register(registry);
		};
	}
}
