package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class StreamingTwitter {
	
	private static String lang  = null;
	private static String cluster = "twitter";
	private static String user = "rubenstefanus";
	private static String password = "ryxryx";
	private static String bucketName = "streaming-twitter";

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		//ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		
		FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>("twitter", new SimpleStringSchema(), properties);
		myConsumer.setStartFromEarliest();
		
			env.addSource(myConsumer)
				.name("Twitter Source")
				.flatMap(new TweetToJsonDocument())
				.addSink(new CouchbaseSink(cluster, user, password, bucketName))
				.name("Couchbase Sink");
				
		// execute program
		System.out.println("Running");
		env.execute("Flink Streaming Twitter from Kafka and produce to FlinkTwitter");
	}
	
	public static class TweetToJsonDocument implements FlatMapFunction<String, JsonDocument> {

		@Override
		public void flatMap(String tweet, Collector<JsonDocument> out) throws Exception {
			
			JsonObject jsonTweet = JsonObject.fromJson(tweet);
				JsonObject tweetUser = jsonTweet.getObject("user");
				JsonObject tweetShrinkUser = JsonObject.create()
						.put("id", tweetUser.get("id"))
						.put("name", tweetUser.get("name"))
						.put("url", tweetUser.get("url"));
				
				//Map<String, List<String>> mhr = getHashTagsAndReferences(jsonTweet.getString("text"));
				
				JsonObject shrinkTweet = JsonObject.create()
						.put("created_at", jsonTweet.get("created_at"))
						.put("source", jsonTweet.get("source"))
						.put("retweet_count", jsonTweet.get("retweet_count"))
						.put("in_reply_to_user_id", jsonTweet.get("in_reply_to_user_id"))
						.put("id", jsonTweet.get("id"))
						.put("timestamp_ms", jsonTweet.get("timestamp_ms"))
						.put("text", jsonTweet.get("text"))
						//.put("lang", jsonTweet.get("lang"))
						//.put("hashtags", mhr.get("hashtags"))
						//.put("references", mhr.get("references"))
						.put("user", tweetShrinkUser);
				
				String key = "tw::" + jsonTweet.get("id");
				out.collect(JsonDocument.create(key, shrinkTweet));
		}

		/*private Map<String, List<String>> getHashTagsAndReferences(String text) {
			Map<String, List<String>> result = new HashMap<String, List<String>>();
			List<String> hashtags = new ArrayList<String>();
			List<String> references = new ArrayList<String>();
			StringTokenizer parts = new StringTokenizer(text, " .:,;?¿");
			while(parts.hasMoreTokens()) {
				String part = parts.nextToken();
				if(part.startsWith("#"))
					hashtags.add(part.substring(1));
				else if(part.startsWith("@"))
					references.add(part.substring(1));
			}
			result.put("hashtags", hashtags);
			result.put("references", references);
			return result;
		}*/

	}
	
}