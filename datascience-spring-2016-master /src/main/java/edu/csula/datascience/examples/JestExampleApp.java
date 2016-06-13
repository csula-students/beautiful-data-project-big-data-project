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

import edu.csula.datascience.examples.imdbES.IMDB;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

public class JestExampleApp {

    public static void main(String[] args) throws URISyntaxException, IOException {
        String indexName = "imdb-data";
        String typeName = "imdb";
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
            ClassLoader.getSystemResource("imdb.csv")
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
            Collection<IMDB> imdbs = Lists.newArrayList();

            int count = 0;

            // for each record, we will insert data into Elastic Search
//            parser.forEach(record -> {
            for (CSVRecord record: parser) {
                // cleaning up dirty data which doesn't have time or temperature
                 
            	IMDB imdb = new IMDB(record.get("id"), record.get("Title"),
						//Integer.parseInt(record.get("Year").contains("") ? "1700" : record.get("Year")),
						record.get("Year"),
						record.get("Released"), 
						(record.get("Genre").contains("N/A") || record.get("Genre").contains("N/A")) ? "action":record.get("Genre"),
						record.get("Director"), record.get("Writer"),
						record.get("Actors"), record.get("Plot"), 
						
						(record.get("Country").contains("N/A") || record.get("Country").contains("N/A")) ? "china":record.get("Country"),

						record.get("Awards"),
						parseSafe((record.get("imdbRating").contains("N/A") || record.get("imdbRating").isEmpty())
								? "0.0" : record.get("imdbRating")),
						Integer.parseInt(
								(record.get("imdbVotes").contains("N/A") || record.get("imdbVotes").contains("")) ? "0"
										: record.get("imdbVotes")),
						record.get("imdbID"), record.get("Type")

				);

                    if (count < 500) {
                        imdbs.add(imdb);
                        count ++;
                    } else {
                        try {
                            Collection<BulkableAction> actions = Lists.newArrayList();
                            imdbs.stream()
                                .forEach(tmp -> {
                                    actions.add(new Index.Builder(tmp).build());
                                });
                            Bulk.Builder bulk = new Bulk.Builder()
                                .defaultIndex(indexName)
                                .defaultType(typeName)
                                .addAction(actions);
                            client.execute(bulk.build());
                            count = 0;
                            imdbs = Lists.newArrayList();
                            System.out.println("Inserted 500 documents to cloud");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            

            Collection<BulkableAction> actions = Lists.newArrayList();
            imdbs.stream()
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
