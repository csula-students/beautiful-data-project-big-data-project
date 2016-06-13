package edu.csula.datascience.examples;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Collection;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.google.common.collect.Lists;

import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

public class JestExampleAppWeather {

	 public static void main(String[] args) throws URISyntaxException, IOException {
	        String indexName = "data-weather";
	        String typeName = "weather";
	        String awsAddress = "http://search-big-data-project-vefvmhcmjzl3z3b7lzrzof64su.us-west-2.es.amazonaws.com/";
	        JestClientFactory factory = new JestClientFactory();
	        factory.setHttpClientConfig(new HttpClientConfig
	            .Builder(awsAddress)
	            .multiThreaded(true)
	            .build());
	        JestClient client = factory.getObject();

	        // as usual process to connect to data source, we will need to set up
	        // node and client// to read CSV file from the resource folder
	        File csv = new File(
	            ClassLoader.getSystemResource("weather.csv")
	                .toURI()
	        );

	        try {
	            // after reading the csv file, we will use CSVParser to parse through
	            // the csv files
	            CSVParser parser = CSVParser.parse(
	                csv,
	                Charset.defaultCharset(),
	                CSVFormat.EXCEL.withHeader()
	            );
	            Collection<Weather> weathers = Lists.newArrayList();

	            int count = 0;

	            // for each record, we will insert data into Elastic Search
//	            parser.forEach(record -> {
	            for (CSVRecord record: parser) {
	                // cleaning up dirty data which doesn't have time or temperature
	                 
	            	Weather weather = new Weather(
							record.get("id"),
						//	record.get("coord"),
							//record.get("weather"),
						//	record.get("base"),
						//	record.get("clouds"),
							//record.get("dt"),
							record.get("name"),
							record.get("country"),
						//	Double.valueOf(record.get("wind_degree")),
						//	Double.valueOf(record.get("wind_speed")),
							Double.valueOf(record.get("temp_max")),
							Double.valueOf(record.get("temp_min")),
							Double.valueOf(record.get("humidity")),
							Double.valueOf(record.get("pressure")),
							Double.valueOf(record.get("temp")));

	                    if (count < 500) {
	                        weathers.add(weather);
	                        count ++;
	                    } else {
	                        try {
	                            Collection<BulkableAction> actions = Lists.newArrayList();
	                            weathers.stream()
	                                .forEach(tmp -> {
	                                    actions.add(new Index.Builder(tmp).build());
	                                });
	                            Bulk.Builder bulk = new Bulk.Builder()
	                                .defaultIndex(indexName)
	                                .defaultType(typeName)
	                                .addAction(actions);
	                            client.execute(bulk.build());
	                            count = 0;
	                            weathers = Lists.newArrayList();
	                            System.out.println("Inserted 500 documents to cloud");
	                        } catch (IOException e) {
	                            e.printStackTrace();
	                        }
	                    }
	                }
	            

	            Collection<BulkableAction> actions = Lists.newArrayList();
	            weathers.stream()
	                .forEach(tmp -> {
	                    actions.add(new Index.Builder(tmp).build());
	                });
	            Bulk.Builder bulk = new Bulk.Builder()
	                .defaultIndex(indexName)
	                .defaultType(typeName)
	                .addAction(actions);
	            client.execute(bulk.build());
	        } catch (IOException e) {
	            e.printStackTrace();
	        }

	        System.out.println("We are done! Yay!");
	   


	}

	    private static double parseSafe(String value) {
			// TODO Auto-generated method stub
	    	return Double.parseDouble(value.isEmpty() || value.equals("Not Provided") ? "0" : value);
		}
	    
	    static class Weather {
			private final String id;
			//private final String coord;
		//	private final String weather;
			//private final String base;
			//private final String clouds;
			//private final String dt;
			private final String name;
			private final String country;
		//	private final Double wind_degree;
			//private final Double wind_speed;
			private final Double temp_max;
			private final Double temp_min;
			private final Double humidity;
			private final Double pressure;
			private final Double temp;

			public Weather(String id, String name,String country,Double temp_max, Double temp_min, Double humidity, Double pressure, Double temp) {
				this.id = id;
				//this.coord = coord;
				//this.weather = weather;
				//this.base = base;
				//this.clouds = clouds;
				//this.dt =dt;
				this.name = name;
				this.country = country;
				//this.wind_degree = wind_degree;
				//this.wind_speed = wind_speed;
				this.temp_max = temp_max;
				this.temp_min = temp_min;
				this.humidity = humidity;
				this.pressure = pressure;
				this.temp = temp;
			}

			public String getId() {
				return id;
			}

			public String getCountry() {
				return country;
			}

			public String getid() {
				return id;
			}

//			public String getCoord() {
//				return coord;
//			}
//
//			public String getWeather() {
//				return weather;
//			}
//
//			public String getBase() {
//				return base;
//			}
//
//			public String getClouds() {
//				return clouds;
//			}
//			
//			public String dt() {
//				return dt;
//			}

			public String getName() {
				return name;
			}

//			public Double getWind_degree() {
//				return wind_degree;
//			}
//
//			public Double getWind_speed() {
//				return wind_speed;
//			}

			public Double getTemp_max() {
				return temp_max;
			}

			public Double getTemp_min() {
				return temp_min;
			}

			public Double getHumidity() {
				return humidity;
			}

			public Double getPressure() {
				return pressure;
			}

			public Double getTemp() {
				return temp;
			}
			
			

		}

	    
}
