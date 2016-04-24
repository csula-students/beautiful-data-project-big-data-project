package edu.csula.datascience.datacollection;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.Gson;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

import sun.net.www.protocol.http.HttpURLConnection;

public class crawler {
	
	@SuppressWarnings("deprecation" )
	public static void main(String []args) throws IOException, ParseException{
		//JSONArray jsonArray =new JSONArray();
		Mongo mongo = new Mongo();
		DB db = mongo.getDB("test");
		DBCollection collection = db.getCollection("t1");
		FileWriter f=new FileWriter("/Users/rohanpatel/Desktop/f1.json");
		
		for(int i=1;i<1000;i++)
		{
			String jsonString = getTVmaze("http://api.tvmaze.com/shows/"+i);
			if(jsonString !=null){
			Object obj = JSON.parse(jsonString);
			Gson gson = new Gson();
			String jo = gson.toJson(obj);
		
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(jo);
		 //jsonArray.put(obj);
		//System.out.println("\n\njsonArray: " + obj);
		
			
			f.write(json.toJSONString());
		 
		 DBObject dbObject = (DBObject)JSON.parse(jo);	
		 
		 collection.insert(dbObject);
			}
			
		 
		}
		 f.flush();
		 f.close();
			
		
			//json.get("")
		 
	}
	
	public static String getTVmaze(String myURL){
		
	
	
	System.out.println("Requested URL:" + myURL);
	StringBuilder sb = new StringBuilder();
	//sb.append("[");
	URLConnection urlConn = null;
	InputStreamReader in = null;
	int code =0;
	try {
		URL url = new URL(myURL);
		urlConn = url.openConnection();
		HttpURLConnection huc = (HttpURLConnection)url.openConnection(); 
		huc.setRequestMethod("GET");
		huc.connect() ;
		
		   code = huc.getResponseCode(); 
		if(code != 404){   
		if (urlConn != null)
			urlConn.setReadTimeout(60 * 1000);
		if (urlConn != null && urlConn.getInputStream() != null) {
			in = new InputStreamReader(urlConn.getInputStream(),
					Charset.defaultCharset());
			BufferedReader bufferedReader = new BufferedReader(in);
			if (bufferedReader != null) {
				int cp;
				while ((cp = bufferedReader.read()) != -1) {
					sb.append((char) cp);
				}
				//sb.append("]");
				bufferedReader.close();
			}
		}
	in.close();
	}
	}catch (Exception e) {
		throw new RuntimeException("Exception while calling URL:"+ myURL, e);
	} 
	if(code==404)
		return null;
	else
		return sb.toString();
	}
}
