package io.confluent.firehose;

import java.io.IOException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

public class FirehoseMetricTest {
	private static final Logger LOG = LoggerFactory.getLogger(FirehoseMetricTest.class);
	private static final String json = "{\"id\":\"2e2a44dc-9ccb-483c-9abb-2f18b43c7c8a\",\"name\":\"request_bytes_total\",\"timestamp\":1572402480,\"component\":\"kafka\",\"tags\":{\"request_type\":\"ApiVersions\",\"source\":\"kafka-3\",\"tenant\":\"lkc-loz7y\",\"unit\":\"bytes\",\"user\":\"1191\"},\"value\":47,\"window\":{\"from\":1572402420,\"interval\":60,\"to\":1572402480}}";
	private static final ObjectMapper objectMapper = new ObjectMapper();
	
	static {
		//Setup Jackson
		objectMapper.registerModule(new AfterburnerModule());
	}

	@Test
	public void testDeser() throws JsonParseException, JsonMappingException, IOException {
		final FirehoseMetric metric = objectMapper.readValue(json.getBytes(), FirehoseMetric.class);
		LOG.debug("Metrics: {}", metric);
	}

}
