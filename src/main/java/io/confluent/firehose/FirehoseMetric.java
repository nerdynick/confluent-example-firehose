package io.confluent.firehose;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FirehoseMetric {
	public static Set<String> FilteredTags = new HashSet<>();
	
	static {
		FilteredTags.add("unit");
	}
	public String id;
	public String name;
	public long timestamp;
	public String component;
	public Map<String, String> tags;
	public double value;
	public Window window;

	public class Window {
		long from;
		long to;
		int interval;
	}
	
	private List<String> _labels = null;
	public List<String> labels(){
		if(_labels == null) {
			_labels = new LinkedList<>();
			for(String tag: tags.keySet()) {
				if(!FilteredTags.contains(tag)) {
					_labels.add(tag);
				}
			}
		}
		return _labels;
	}
	private String[] _labelsArray = null;
	public String[] labelsAsArray() {
		if(_labelsArray == null) {
			this._labelsArray = this.labels().toArray(new String[this.labels().size()]);
		}
		return this._labelsArray;
	}
	
	
	private List<String> _labelValues = null;
	public List<String> labelValues(){
		if(_labelValues == null) {
			_labelValues = new LinkedList<>();
			for(String tag: this.labels()) {
				_labelValues.add(this.tags.get(tag));
			}
		}
		return _labelValues;
	}
	private String[] _labelValuesArray = null;
	public String[] labelValuesAsArray() {
		if(_labelValuesArray == null) {
			this._labelValuesArray = this.labelValues().toArray(new String[this.labelValues().size()]);
		}
		return this._labelValuesArray;
	}
}