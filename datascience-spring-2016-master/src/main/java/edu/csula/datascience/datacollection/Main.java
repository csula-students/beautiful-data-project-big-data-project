package edu.csula.datascience.datacollection;






import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;

import org.bson.Document;
import org.json.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;





public class Main {
	 
	

	

	
	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException  {
		
		
		
		  // establish database connection to MongoDB
        MongoClient mongoClient = new MongoClient();
        // select `bd-example` as testing database
        MongoDatabase database = mongoClient.getDatabase("test");

        // select collection by name `test`
        MongoCollection<Document> collection = database.getCollection("t1");
		
		

        JSONParser parser = new JSONParser();
        String jsonString = callURL("/Users/rohanpatel/Desktop/test.json");
		 JSONArray br = new JSONArray( jsonString);

	        for(int i=0;i<br.length();i++){
	        	org.json.JSONObject t = br.getJSONObject(i);
	        	String name = (String) t.get("name");
	        
	        	
	        	
	            String city = (String) t.get("city");
	           

	            String job = (String) t.get("job");
	          
	            JSONArray cars = (JSONArray) t.get("cars");

	            Document doc = new Document("name", name)
	                    .append("city", city)
	                    .append("job", job)
	                    .append("cars", cars);
	            
	            collection.insertOne(doc);
	        }
	      
		
	}

	
	public static String callURL(String myURL) {
		System.out.println("Requested URL:" + myURL);
		StringBuilder sb = new StringBuilder();
		URLConnection urlConn = null;
		//InputStreamReader in = null;
		try {
			File url = new File(myURL);
			
//			urlConn = url.openConnection();
//			if (urlConn != null)
//				urlConn.setReadTimeout(60 * 1000);
//			if (urlConn != null && urlConn.getInputStream() != null) {
//				in = new InputStreamReader(urlConn.getInputStream(),
//						Charset.defaultCharset());
				BufferedReader bufferedReader = new BufferedReader(new FileReader(url));
				if (bufferedReader != null) {
					int cp;
					while ((cp = bufferedReader.read()) != -1) {
						sb.append((char) cp);
					}
					bufferedReader.close();
				}
			
		//in.close();
		} catch (Exception e) {
			throw new RuntimeException("Exception while calling URL:"+ myURL, e);
		} 
 
		return sb.toString();
	}
}
