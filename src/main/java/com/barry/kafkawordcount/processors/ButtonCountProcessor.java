package com.barry.kafkawordcount.processors;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.function.Function;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsStateStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ButtonCountProcessor {

    @Bean
	public Function<KStream<String, String>,KStream<String, ButtonCount>> process(){
		// TODO: using in memory Store
		// check this for not using RocksDB: https://kafka.apache.org/30/javadoc/org/apache/kafka/streams/state/Stores.html
		// KeyValueStore<String,String> storeSupplier = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("ButtonsCounts-1"),Serdes.String(),Serdes.String()).build();
		// KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("ButtonCounts-1");

        return input -> input
					.map((key, value) -> new KeyValue<>(value, value))
					.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
					.windowedBy(TimeWindows.of(Duration.ofMillis(3000)))
					// .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(Duration.ofMillis(5000),Duration.ofMillis(2000)))
					.count(Materialized.as("ButtonCounts-1"))
					//.count(Materialized.)
					.toStream()
					.map((key, value) -> new KeyValue<>(null, new ButtonCount(key.key(), value, new Date(key.window().start()), new Date(key.window().end()))));
    }


	// https://stackoverflow.com/questions/62467431/is-it-possible-to-use-kafkastreamsstatestore-annotation-on-spring-cloud-stream
	// https://cloud.spring.io/spring-cloud-static/spring-cloud-stream-binder-kafka/3.0.6.RELEASE/reference/html/spring-cloud-stream-binder-kafka.html#_state_store
	
	@Bean
    public StoreBuilder<KeyValueStore<String,String>> myStore() {
        return Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("ButtonsCounts-1"),Serdes.String(),Serdes.String());
    }


    static class ButtonCount {

		private String button;

		private long count;

		private Date start;

		private Date end;

		@Override
		public String toString() {
			final StringBuffer sb = new StringBuffer("buttonCount{");
			sb.append("button='").append(button).append('\'');
			sb.append(", count=").append(count);
			sb.append(", start=").append(start);
			sb.append(", end=").append(end);
			sb.append('}');
			return sb.toString();
		}

		ButtonCount() {

		}

		ButtonCount(String button, long count, Date start, Date end) {
			this.button = button;
			this.count = count;
			this.start = start;
			this.end = end;
		}

		public String getbutton() {
			return button;
		}

		public void setbutton(String button) {
			this.button = button;
		}

		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}

		public Date getStart() {
			return start;
		}

		public void setStart(Date start) {
			this.start = start;
		}

		public Date getEnd() {
			return end;
		}

		public void setEnd(Date end) {
			this.end = end;
		}
	}

}