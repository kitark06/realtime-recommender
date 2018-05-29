package com.kartikiyer.occulus.flink;


import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.kartikiyer.occulus.kafka.RandomInputGenerator;


public class RecommenderCore
{
	public static String DELIM = ".";

	public void generateRecommendations() throws Exception
	{
		Logger.getRootLogger().setLevel(Level.ERROR);

		RandomInputGenerator producer = new RandomInputGenerator();
		producer.produceInfiniteStream(100, producer.getSocketOutPutStream());
		
		Thread.sleep(3000);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

		DataStream<String> inputStream = env.socketTextStream("10.30.7.153", 12181);


		Time windowingTime = Time.seconds(5);

		inputStream	.map(line -> new Tuple2<String, String>(line.split("\t")[0], line.split("\t")[1]))
					.keyBy(0)
					.timeWindow(windowingTime)
					.reduce((value1, value2) -> new Tuple2(value1.f0, value1.f1 + DELIM + value2.f1))
					.filter(t2 -> t2.f1.length() > 2)
					.flatMap((Tuple2<String, String> value, Collector<Tuple2<String, String>> out) ->
					{
						String[] likeArray = value.f1.split("\\" + DELIM);

						for (int i = 0; i < likeArray.length; i++)
							for (int j = 0; j < likeArray.length; j++)
								if (i != j)
									out.collect(new Tuple2<String, String>(likeArray[i], likeArray[j]));
					})
					.map(x -> new Tuple2<>(x, 1))
					.keyBy(0)
					.sum(1)
					.keyBy("f0.f0")
					.timeWindow(windowingTime)
					// out --> Tuple2<String, Set<Tuple2<String, Integer>>>
					.apply((key, window, input, out) ->
					{
						Comparator<Tuple2<String, Integer>> cmp = (o1, o2) -> -(o1.f1 - o2.f1);
						
						Set<Tuple2<String, Integer>> recommendationList = new TreeSet(cmp);
						for (Tuple2<Tuple2<String, String>, Integer> x : input)
							recommendationList.add(new Tuple2<String, Integer>(x.f0.f1, x.f1));

						Tuple2<String, Set<Tuple2<String, Integer>>> output = new Tuple2(key.toString(), recommendationList);
						out.collect(output);
					})
					.print();


		env.execute("Genesis");
	}
}
