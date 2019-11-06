package io.confluent.firehose;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

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
		@JsonProperty("from")
		long from;
		@JsonProperty("to")
		long to;
		@JsonProperty("interval")
		int interval;
		
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("{")
				.append(this.from)
				.append(" to ")
				.append(this.to)
				.append(" at ")
				.append(this.interval)
				.append("}");
			return builder.toString();
		}
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(this.getClass().getSimpleName())
			.append("{")
			.append("id=").append(this.id)
			.append(" name=").append(this.name)
			.append(" component=").append(this.component)
			.append(" timestamp=").append(this.timestamp)
			.append(" value=").append(this.value)
			.append(" window=").append(this.window)
			.append(" tags=").append(this.tags)
			.append("}");
		
		return builder.toString();
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