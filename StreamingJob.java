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
import java.lang.*;
import java.io.*;

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
    	private double[] shares = { 0.2, 0.2, 0.2, 0.15, 0.15, 0.1 };
    	public double[][] stats;
    	public Vector<Tuple7<Double, Double, Double, Double, Double, Double, Double>> samples;
    	public boolean fullWindowLoaded = false;
    	
    	public double[] means;
    	public double[] medians;
    	public double[] quantiles;
    	public double[] meansOfSmallest;
    	public double[] securityMeasures1;
    	public double[] securityMeasures2;
    	
    	public State() {
    		count = 0;
    		samples = new Vector<Tuple7<Double, Double, Double, Double, Double, Double, Double>>();
    		stats = new double[6][7];
    		loadStats();
    		
    		means = new double[7];
    		medians = new double[7];
    		quantiles = new double[7];
    		meansOfSmallest = new double[7];
    		securityMeasures1 = new double[7];
    		securityMeasures2 = new double[7];
    	}
    	
    	public void loadStats() {
    		try (BufferedReader br = new BufferedReader(new FileReader("stats.csv"))) {
    		    String line;
    		    int lineNum = 0;
    		    while ((line = br.readLine()) != null) {
    		        String[] values = line.split(",");
    		        for (int i = 0; i < values.length; ++i) {
    		        	stats[lineNum][i] = Double.parseDouble(values[i]);
    		        }
    		        lineNum++;
    		    }
    		}
    		catch (Exception e) {
            	return;
            }
    	}
    	
    	public void addSample(Tuple6<Double, Double, Double, Double, Double, Double> sample) {
    		
    		double overall = getOverallValue(sample);
    		samples.add(new Tuple7<>(sample.f0, sample.f1, sample.f2, sample.f3, sample.f4, sample.f5, overall));
    		count++;
    		
    		if (samples.size() == 30) {
    			fullWindowLoaded = true;
    			calculateFirstMeasures();
    		}
    		
    		if (fullWindowLoaded) {
        		udpateAllMeasures();
    			samples.remove(0);
    		}
    	}
    	
    	public double[][] getSortedSamples() {
    		int n = samples.size();
    		double[][] newSamples = new double[7][n];
    		for (int i = 0; i < 6; ++i) {
    			for (int j = 0; j < n; ++j) {
    				newSamples[i][j] = (double)samples.get(j).getField(i);
    				newSamples[6][j] += newSamples[i][j] * shares[i];
    			}
    			Arrays.sort(newSamples[i]);
    		}
    		Arrays.sort(newSamples[6]);
    		return newSamples;
    	}
    	
    	public double getOverallValue(Tuple6<Double, Double, Double, Double, Double, Double> value) {
    		int n = samples.size();
    		double overall = 0.0;
    		for (int i = 0; i < 6; ++i) {
    			overall += shares[i] * (double)value.getField(i);
    		}
    		return overall;
    	}
    	
    	
    	// calculates all measures for the first time after a full window has been loaded
    	public void calculateFirstMeasures() {
    		calculateFirstMean();
    		int n = samples.size();
    		double[][] sortedSamples = getSortedSamples();
    		for (int i = 0; i < 7; ++i) {
    			medians[i] = (sortedSamples[i][n/2 - 1] + sortedSamples[i][n/2]) / 2;
    			quantiles[i] = sortedSamples[i][n/10];
    			for (int j = 0; j < 3; ++j) {
    				meansOfSmallest[i] += sortedSamples[i][j];
    			}
    			meansOfSmallest[i] /= 3;
    		}
    		calculateFirstSecurityMeasures1();
    		calculateFirstSecurityMeasures2();
    	}
    	
    	public void calculateFirstMean() {
    		int n = samples.size();
    		for (int i = 0; i < 7; ++i) {
    			for (int j = 0; j < n; ++j) {
    				means[i] += (double)samples.get(j).getField(i);
    			}
    			means[i] /= n;
    		}
    	}
    	
    	public void calculateFirstSecurityMeasures1() { 		
    		int n = samples.size();
    		for (int i = 0; i < 7; ++i) {
    			double sum = 0.0;
    			for (int j = 0; j < n; ++j) {
    				sum += Math.abs(means[i] - (double)samples.get(j).getField(i));
        		}
    			securityMeasures1[i] = means[i] - (sum / (2 * n));
    		}
    	}
    	
    	public void calculateFirstSecurityMeasures2() {
    		int n = samples.size();
    		for (int i = 0; i < 7; ++i) {
    			double sum = 0.0;
    			for (int j = 0; j < n; ++j) {
    				for (int k = 0; k < n; ++k) {
        				sum += Math.abs((double)samples.get(j).getField(i) - (double)samples.get(k).getField(i));
    				}
        		}
    			securityMeasures2[i] = means[i] - (sum / (2 * n * n));
    		}
    	}
    	
    	
    	// updates all measures after a full window has been loaded
    	public void udpateAllMeasures() { // new value on last position, old value (to be removed) on first position
    		updateMeans();
    		updateMedians();
    		updateQuantiles();
    		updateMeansOfSmallest();
    		updateSecurityMeasures1();
    		updateSecurityMeasures2();
    	}
    	
    	public void updateMeans() {
    		int n = samples.size();
    		Tuple7<Double, Double, Double, Double, Double, Double, Double> oldValue = samples.get(0);
    		Tuple7<Double, Double, Double, Double, Double, Double, Double> newValue = samples.get(n - 1);
    		for (int i = 0; i < 7; ++i) {
				means[i] -= (double)oldValue.getField(i) / 30.0;
				means[i] += (double)newValue.getField(i) / 30.0;
			}
    	}
    	
    	// TODO
    	public void updateMedians() { }
    	public void updateQuantiles() { }
    	public void updateMeansOfSmallest() { }
    	
    	public void updateSecurityMeasures1() {
    		int n = samples.size();
    		for (int i = 0; i < 7; ++i) {
    			securityMeasures1[i] += Math.abs(means[i] - (double)samples.get(0).getField(i)) / (2 * n);
    			securityMeasures1[i] -= Math.abs(means[i] - (double)samples.get(n-1).getField(i)) / (2 * n);
    		}
    	}
    	
		public void updateSecurityMeasures2() {
			int n = samples.size();
			for (int i = 0; i < 7; ++i) {
				for (int j = 0; j < n; ++j) {
					securityMeasures2[i] += Math.abs((double)samples.get(j).getField(i) - (double)samples.get(0).getField(i)) / (2 * n * n);
	    			securityMeasures2[i] -= Math.abs((double)samples.get(j).getField(i) - (double)samples.get(n-1).getField(i)) / (2 * n * n);
				}
    		}
    	}
    	
    	// Returns a list of alerts with info (count, measure type (0-5), sample type (0-6), value of sample)
    	public Vector<Tuple4<Integer, Integer, Integer, Double>> getAlerts() {
    		Vector<Tuple4<Integer, Integer, Integer, Double>> alerts = new Vector<Tuple4<Integer, Integer, Integer, Double>>(); 
    		for (int i = 0; i < 7; ++i) {
				if (means[i] <= 0.9 * stats[0][i]) {
					alerts.add(new Tuple4<>(count, 0, i, means[i]));	
				}
				if (medians[i] <= 0.9 * stats[1][i]){
					alerts.add(new Tuple4<>(count, 1, i, medians[i]));	
				}
				if (quantiles[i] <= 0.9 * stats[2][i]) {
					alerts.add(new Tuple4<>(count, 2, i, quantiles[i]));	
				}
				if (meansOfSmallest[i] <= 0.9 * stats[3][i]) {
					alerts.add(new Tuple4<>(count, 3, i, meansOfSmallest[i]));	
				}
				if (securityMeasures1[i] <= 0.9 * stats[4][i]) {
					alerts.add(new Tuple4<>(count, 4, i, securityMeasures1[i]));
				}
				if (securityMeasures2[i] <= 0.9 * stats[5][i]) {
					alerts.add(new Tuple4<>(count, 5, i, securityMeasures2[i]));
				}
			}
    		return alerts;
    	}
    }
    
    public static class SamplesAggregate
		implements AggregateFunction<Tuple6<Double, Double, Double, Double, Double, Double>, State, Vector<Tuple4<Integer, Integer, Integer, Double>>> {
	  
		@Override
		public State createAccumulator() {
			return new State();
		}
	
		@Override
		public State add(Tuple6<Double, Double, Double, Double, Double, Double> value, State accumulator) {
			accumulator.addSample(value);
			return accumulator;
		}
	
		@Override
		public Vector<Tuple4<Integer, Integer, Integer, Double>> getResult(State accumulator) {
			return accumulator.getAlerts();
		}
	
		@Override
		public State merge(State a, State b) {
			a.count = a.count + b.count;
			return a;
		}
	}
    
    public static class AlertReducer implements FlatMapFunction<Vector<Tuple4<Integer, Integer, Integer, Double>>,
    												Tuple4<Integer, Integer, Integer, Double>> {
		@Override
		public void flatMap(Vector<Tuple4<Integer, Integer, Integer, Double>> value,
				Collector<Tuple4<Integer, Integer, Integer, Double>> out) {
			value.forEach((alert) -> out.collect(alert));
		}
	}
    
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        int windowSize = 10;
        int n = 0;
        DataStream<Tuple4<Integer, Integer, Integer, Double>> dataStream = env
                .readTextFile("test_samples.csv")
                .flatMap(new Splitter())
                .countWindowAll(windowSize, 1)
                .aggregate(new SamplesAggregate())
                .flatMap(new AlertReducer());

        dataStream.print();

        env.execute("PSD");
    }
}


