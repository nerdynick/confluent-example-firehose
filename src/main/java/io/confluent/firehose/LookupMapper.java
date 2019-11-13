package io.confluent.firehose;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LookupMapper {
	private static final LookupMapper _mapper = new LookupMapper();
	
	private final Map<String, Map<String, String>> lookups = new HashMap<>();
	
	private LookupMapper() {
		
	}
	
	public void addLookup(String label, Map<String, String> lookup) {
		this.lookups.put(label, lookup);
	}
	
	public String[] lookup(String[] labels, String[] labelValues) {
		final String[] newValues = new String[labels.length];
		
		for(int i=0; i<labels.length; i++) {
			final Map<String, String> lookup = lookups.getOrDefault(labels[i], Collections.emptyMap());
			newValues[i] = lookup.getOrDefault(labelValues[i], labelValues[i]);
		}
		
		return newValues;
	}
	
	public static LookupMapper I() {
		return _mapper;
	}
	
	public static void AddLookup(String label, Map<String, String> lookup) {
		I().addLookup(label, lookup);
	}
	
	public static String[] Lookup(String[] labels, String[] labelValues) {
		return I().lookup(labels, labelValues);
	}

}
