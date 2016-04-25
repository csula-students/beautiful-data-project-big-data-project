package edu.csula.datascience.datacollection;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;
import org.json.simple.JSONObject;

import com.google.gson.Gson;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

import edu.csula.datascience.acquisition.Collector;
import twitter4j.Status;

public class CollectData implements Collector<JSONObject, JSONObject>{

	Mongo mongo;
	DB db;
	DBCollection collection;
    
	 public CollectData() throws IOException {
		  mongo = new Mongo();
			 db = mongo.getDB("test");
			 collection = db.getCollection("t1");
			 
	    }
	
	
	
	@Override
	public Collection<JSONObject> mungee(Collection<JSONObject> src) {
		
		return src;
	}


	@Override
	public void save(Collection<JSONObject> data) {
		int size = data.size();
		// Iterator<JSONObject> documents = data.iterator();
		ArrayList<JSONObject> json_data = new ArrayList<JSONObject>(data);
		for(JSONObject j:json_data){
			Gson gson = new Gson();
			String jo = gson.toJson(j);
			DBObject dbObject = (DBObject)JSON.parse(jo);	
			collection.insert(dbObject);
			 
		}
		
	}



}
