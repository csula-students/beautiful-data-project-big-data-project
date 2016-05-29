package edu.csula.datascience.examples;

import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;



//PUT /weather-data
//{
//  "mappings" : {
//      "weather" : {
//          "properties" : {
//              "coord" : {
//                  "type" : "string",
//                  "index" : "analyzed"
//              },
//              "weather" : {
//                  "type" : "string",
//                  "index" : "analyzed"
//              },
//              "base" : {
//                  "type" : "string",
//                  "index" : "analyzed"
//              },
//				"clouds": {
//  				"type": "string",
//					"index" : "analyzed"
//						},
//				"dt": {
//					"type": "date",
//					"index" : "analyzed"
//						  },
//				"name": {
//					"type": "string",
//					"index" : "analyzed"
//						}
//				"wind_degree": {
//					"type": "double",
//					"index" : "analyzed"
//		  			  },
//				"wind_speed": {
//					"type": "double",
//					"index" : "analyzed"
//		  			  },
//				"temp_max": {
//					"type": "double",
//					"index" : "analyzed"
//		 			  		},
//				"temp_min": {
//					"type": "double",
//					"index" : "analyzed"
//	  							},
//				"humidity": {
//					"type": "double",
//					"index" : "analyzed"
//	  							},
//				"pressure": {
//					"type": "double",
//					"index" : "analyzed"
//	  							},
//				"temp": {
//					"type": "double",
//					"index" : "analyzed"
//	  							},
//          }
//      }
//  }
//}



public class weatherES {

	private final static String indexName = "weather-data";
	private final static String typeName = "weather";

	public static void main(String[] args) throws URISyntaxException, IOException {
		Node node = nodeBuilder()
				.settings(Settings.builder().put("cluster.name", "bhavik").put("path.home", "elasticsearch-data"))
				.node();
		Client client = node.client();

		/**
		 *
		 *
		 * INSERT data to elastic search
		 */

		// as usual process to connect to data source, we will need to set up
		// node and client// to read CSV file from the resource folder
		File csv = new File(ClassLoader.getSystemResource("weather.csv").toURI());

		// create bulk processor
		BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				System.out.println("Facing error while importing data to elastic search");
				failure.printStackTrace();
			}
		}).setBulkActions(10000).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
				.setFlushInterval(TimeValue.timeValueSeconds(5)).setConcurrentRequests(1)
				.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();

		// Gson library for sending json to elastic search
		Gson gson = new Gson();

		try {
			// after reading the csv file, we will use CSVParser to parse
			// through
			// the csv files
			CSVParser parser = CSVParser.parse(csv, Charset.defaultCharset(), CSVFormat.EXCEL.withHeader());

			// for each record, we will insert data into Elastic Search
			parser.forEach(record -> {
				Weather weather = new Weather(
								record.get("id"),
								record.get("coord"),
								record.get("weather"),
								record.get("base"),
								record.get("clouds"),
								record.get("dt"),
								record.get("name"),
								Double.valueOf(record.get("wind_degree")),
								Double.valueOf(record.get("wind_speed")),
								Double.valueOf(record.get("temp_max")),
								Double.valueOf(record.get("temp_min")),
								Double.valueOf(record.get("humidity")),
								Double.valueOf(record.get("pressure")),
								Double.valueOf(record.get("temp")));
				bulkProcessor.add(new IndexRequest(indexName, typeName).source(gson.toJson(weather)));
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static Double parseSafe(String value) {
		return Double.parseDouble(value.isEmpty() || value.equals("Not Provided") ? "0" : value);
	}

	/**
	 * @author Bhavik
	 *
	 */
	static class Weather {
		private final String id;
		private final String coord;
		private final String weather;
		private final String base;
		private final String clouds;
		private final String dt;
		private final String name;
		private final Double wind_degree;
		private final Double wind_speed;
		private final Double temp_max;
		private final Double temp_min;
		private final Double humidity;
		private final Double pressure;
		private final Double temp;

		public Weather(String id, String coord, String weather, String base, String clouds, String dt, String name, Double wind_degree, Double wind_speed,
				Double temp_max, Double temp_min, Double humidity, Double pressure, Double temp) {
			this.id = id;
			this.coord = coord;
			this.weather = weather;
			this.base = base;
			this.clouds = clouds;
			this.dt =dt;
			this.name = name;
			this.wind_degree = wind_degree;
			this.wind_speed = wind_speed;
			this.temp_max = temp_max;
			this.temp_min = temp_min;
			this.humidity = humidity;
			this.pressure = pressure;
			this.temp = temp;
		}

		public String getid() {
			return id;
		}

		public String getCoord() {
			return coord;
		}

		public String getWeather() {
			return weather;
		}

		public String getBase() {
			return base;
		}

		public String getClouds() {
			return clouds;
		}
		
		public String dt() {
			return dt;
		}

		public String getName() {
			return name;
		}

		public Double getWind_degree() {
			return wind_degree;
		}

		public Double getWind_speed() {
			return wind_speed;
		}

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
