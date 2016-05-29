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
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;



//PUT /imdb-data
//{
//	"mappings" : {
//		"imdb" : {
//			"properties" : {
//				"Title" : {
//					"type" : "string",
//					"index" : "not_analyzed"
//						},
//				"Year" : {
//					"type" : "date",
//					"index" : "not_analyzed"
//						},
//				"Released" : {
//					"type" : "string",
//					"index" : "not_analyzed"
//							 },
//				"Genre" : {
//					"type" : "string",
//					"index" : "not_analyzed"	
//						  },
//				"Director" : {
//					"type" : "string",
//					"index" : "not_analyzed"	
//		  				  },
//				"Writer" : {
//					"type" : "string",
//					"index" : "not_analyzed"	
//		  				  },
//				"Actors" : {
//					"type" : "string",
//					"index" : "not_analyzed"	
//		  				  },
//				"Plot" : {
//					"type" : "string",
//					"index" : "not_analyzed"	
//		  				  },
//				"Country" : {
//					"type" : "string",
//					"index" : "not_analyzed"	
//		  				  },
//				"Awards" : {
//					"type" : "string",
//					"index" : "not_analyzed"	
//		  				  },
//				"imdbRating" : {
//					"type" : "double",
//					"index" : "not_analyzed"	
//		  				  },
//				"imdbVotes" : {
//					"type" : "integer",
//					"index" : "not_analyzed"	
//			  				},
//				"imdbID" : {
//					"type" : "string",
//					"index" : "not_analyzed"	
//			  				},
//				"Type" : {
//					"type" : "string",
//					"index" : "not_analyzed"	
//			  				}
//						}
//					}
//			}
//}
//
//DELETE /imdb-data
//
//GET /imdb-data/_search


public class imdbES {
	private final static String indexName = "imdb-data";
	private final static String typeName = "imdb";

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
		File csv = new File(ClassLoader.getSystemResource("imdb.csv").toURI());

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
			DateFormat df = new SimpleDateFormat("dd mmm yyyy");
			// for each record, we will insert data into Elastic Search

			// parseSafe(record.get("OvertimePay")),
			// parseSafe(record.get("OtherPay")),
			// parseSafe(record.get("Benefits")),
			// parseSafe(record.get("TotalPay")),
			// parseSafe(record.get("TotalPayBenefits")),
			// Integer.parseInt(record.get("Year").isEmpty() ? "1979" :
			// record.get("Year")),
			// record.get("Notes"),

			parser.forEach(record -> {
				IMDB imdb = new IMDB(record.get("id"), record.get("Title"),
						//Integer.parseInt(record.get("Year").contains("") ? "1700" : record.get("Year")),
						record.get("Year"),
						record.get("Released"), record.get("Genre"), record.get("Director"), record.get("Writer"),
						record.get("Actors"), record.get("Plot"), record.get("Country"),

						record.get("Awards"),
						parseSafe((record.get("imdbRating").contains("N/A") || record.get("imdbRating").isEmpty())
								? "0.0" : record.get("imdbRating")),
						Integer.parseInt(
								(record.get("imdbVotes").contains("N/A") || record.get("imdbVotes").contains("")) ? "0"
										: record.get("imdbVotes")),
						record.get("imdbID"), record.get("Type")

				);

				bulkProcessor.add(new IndexRequest(indexName, typeName).source(gson.toJson(imdb)));

			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static Double parseSafe(String value) {
		return Double.parseDouble(value.isEmpty() || value.equals("Not Provided") ? "0" : value);
	}

	static class IMDB {
		private final String id;
		private final String Title;
		private final String Year;
		// private final java.util.Date Released;
		private final String Released;
		private final String Genre;
		private final String Director;
		private final String Writer;
		private final String Actors;
		private final String Plot;
		private final String Country;
		private final String Awards;
		private final double imdbRating;
		private final int imdbVotes;
		private final String imdbID;
		private final String Type;

		public IMDB(String id, String title, String year, String date, String genre, String director, String writer,
				String actors, String plot, String country, String awards, double imdbRating, int imdbVotes,
				String imdbID, String type) {
			super();
			this.id = id;
			Title = title;
			Year = year;
			Released = date;
			Genre = genre;
			Director = director;
			Writer = writer;
			Actors = actors;
			Plot = plot;
			Country = country;
			Awards = awards;
			this.imdbRating = imdbRating;
			this.imdbVotes = imdbVotes;
			this.imdbID = imdbID;
			Type = type;
		}

		public String get_id() {
			return id;
		}

		public String getTitle() {
			return Title;
		}

		public String getYear() {
			return Year;
		}

		public String getReleased() {
			return Released;
		}

		public String getGenre() {
			return Genre;
		}

		public String getDirector() {
			return Director;
		}

		public String getWriter() {
			return Writer;
		}

		public String getActors() {
			return Actors;
		}

		public String getPlot() {
			return Plot;
		}

		public String getCountry() {
			return Country;
		}

		public String getAwards() {
			return Awards;
		}

		public double getImdbRating() {
			return imdbRating;
		}

		public int getImdbVotes() {
			return imdbVotes;
		}

		public String getImdbID() {
			return imdbID;
		}

		public String getType() {
			return Type;
		}

	}
}