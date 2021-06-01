/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package psd;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application Integero a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */

import java.util.*;
import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.windowing.*;
import org.apache.flink.streaming.api.datastream.KeyedStream.*;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.api.common.functions.ReduceFunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
import org.apache.flink.streaming.api.windowing.windows.*;
import org.apache.flink.api.common.functions.*;

public class StreamingJob {    
   
    public static class Splitter implements FlatMapFunction<String, Tuple6<Double, Double, Double, Double, Double, Double>> {
        
    	@Override
        public void flatMap(String line, Collector<Tuple6<Double, Double, Double, Double, Double, Double>> out) throws Exception {
            String[] cells = line.split(",");
            Double values[] = new Double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
            try {
                values[0] = Double.parseDouble(cells[0]);
                values[1] = Double.parseDouble(cells[1]);
                values[2] = Double.parseDouble(cells[2]);
                values[3] = Double.parseDouble(cells[3]);
                values[4] = Double.parseDouble(cells[4]);
                values[5] = Double.parseDouble(cells[5]);
            }
            catch (NumberFormatException e) {
            	return;
            }
            out.collect(new Tuple6(values[0], values[1], values[2],
            					   values[3], values[4], values[5]));
        }
    }
    
    public static class State {
    	public int count = 0;
    	public double[] mean;
    	private double[] shares = { 0.2, 0.2, 0.2, 0.15, 0.15, 0.1 };
    	public Vector<Tuple6<Double, Double, Double, Double, Double, Double>> samples;
    	
    	public State() {
    		count = 0;
    		mean = new double[6];
    		samples = new Vector<Tuple6<Double, Double, Double, Double, Double, Double>>();
    	}
    	
    	public void addSample(Tuple6<Double, Double, Double, Double, Double, Double> sample) {
    		if (samples.size() >= 30) {
    			samples.remove(0);
    		}
    		samples.add(sample);
    	}
    	
    	public double getOverallMean() {
    		double res = 0.0;
    		for (int i = 0; i < 6; ++i)
    		{
    			res += shares[i] * mean[i];
    		}
    		return res;
    	}
    	
    	public String getMeanString() {
    		String message = "Count: " + count + ". Means:";
    		for (int i = 0; i < 6; ++i) {
    			message += " (" + i + ") " + this.mean[i];
    		}
    		message += ". Overall: " + this.getOverallMean();
    		return message;
    	}
    	
    	public double[][] getSortedSamples() {
    		int n = samples.size();
    		double[][] newSamples = new double[7][n];
    		for (int i = 0; i < 6; ++i) {
    			for (int j = 0; j < n; ++j) {
    				newSamples[i][j] = samples.get(j).getField(i);
    				newSamples[6][j] += newSamples[i][j] * shares[i];
    			}
    			Arrays.sort(newSamples[i]);
    		}
    		Arrays.sort(newSamples[6]);
    		return newSamples;
    	}
    	
    	public double[] getMedians() {
    		int n = samples.size();
    		double[][] sortedSamples = getSortedSamples();
    		double[] medians = new double[7]; // 6 fields + 1 overall
    		for (int i = 0; i < 7; ++i) {
	    		if (n % 2 == 0) {
    				medians[i] = (sortedSamples[i][n/2 - 1] + sortedSamples[i][n/2]) / 2;
	    		}
	    		else {
	    			medians[i] = sortedSamples[i][n/2];
	    		}
			}
    		return medians;
    	}
    	
    	public String getMedianString() {
    		double[] medians = getMedians();
    		String message = "Count: " + count + ". Medians:";
    		for (int i = 0; i < 6; ++i) {
    			message += " (" + i + ") " + medians[i];
    		}
    		message += ". Overall: " + medians[6];
    		return message;
    	}
    	
    	public double[] get10thQuantiles() {
    		int n = samples.size();
    		double[][] sortedSamples = getSortedSamples();
    		double[] quantiles = new double[7];
    		for (int i = 0; i < 7; ++i) {
    			quantiles[i] = sortedSamples[i][n/10];
    		}
    		return quantiles;
    	}
    	
    	public double[] getMeansOfSmallest() {
    		int n = samples.size();
    		double[][] sortedSamples = getSortedSamples();
    		double[] smallestMeans = new double[7];
    		for (int i = 0; i < 7; ++i) {
    			int max = Math.max(1, n/10);
    			for (int j = 0; j < max; ++j) {
    				smallestMeans[i] +=  sortedSamples[i][j];
    			}
    			smallestMeans[i] /= max;
    		}
    		return smallestMeans;
    	}
    }
    
    public static class AverageAggregate
		implements AggregateFunction<Tuple6<Double, Double, Double, Double, Double, Double>, State, String> {
	  
		@Override
		public State createAccumulator() {
			return new State();
		}
	
		@Override
		public State add(Tuple6<Double, Double, Double, Double, Double, Double> value, State accumulator) {
			int count = accumulator.count;
			for (int i = 0; i < 6; ++i)
			{
				double newMean = 0.0;
				if (count < 10) {
					newMean = (accumulator.mean[i] * (double)count + (double)value.getField(i)) / (double)(count + 1);
				}
				else {
					newMean = accumulator.mean[i] - accumulator.mean[i] / 10.0 + (double)value.getField(i) / 10.0;
				}
				accumulator.mean[i] = newMean;
			}
			accumulator.count = count + 1;
			accumulator.addSample(value);
			return accumulator;
		}
	
		@Override
		public String getResult(State accumulator) {
			return accumulator.getMeanString() + "\n" + accumulator.getMedianString();
		}
	
		@Override
		public State merge(State a, State b) {
			for (int i = 0; i < 6; ++i) {
				a.mean[i] = (a.mean[i] * a.count + b.mean[i] * b.count) / (a.count + b.count);
			}
			a.count = a.count + b.count;
			return a;
		}
	}
    
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        int windowSize = 10;
        int n = 0;
        DataStream<String> dataStream = env
                .readTextFile("test_samples.csv")
                .flatMap(new Splitter())
                .countWindowAll(windowSize, 1)
                .aggregate(new AverageAggregate());

        dataStream.print();

        env.execute("PSD");
    }
}


