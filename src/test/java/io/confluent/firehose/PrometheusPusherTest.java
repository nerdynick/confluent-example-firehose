package io.confluent.firehose;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusPusherTest {
	private static final Logger LOG = LoggerFactory.getLogger(PrometheusPusherTest.class);
	
	public static final String jsonSample1 = "{\n" + 
			"  \"id\": \"645caf5b-b278-4805-ab5e-81e287f07bd0\",\n" + 
			"  \"name\": \"response_bytes_total\",\n" + 
			"  \"timestamp\": 1528903380,\n" + 
			"  \"component\": \"kafka\",\n" + 
			"  \"tags\": {\n" + 
			"    \"request_type\": \"Fetch\",\n" + 
			"    \"source\": \"kafka-1\",\n" + 
			"    \"tenant\": \"lkc-foo\",\n" + 
			"    \"unit\": \"bytes\",\n" + 
			"    \"user\": \"42\"\n" + 
			"  },\n" + 
			"  \"value\": 25674,\n" + 
			"  \"window\": {\n" + 
			"    \"from\": 1528903320,\n" + 
			"    \"interval\": 60,\n" + 
			"    \"to\": 1528903380\n" + 
			"  }\n" + 
			"}\n";



}
