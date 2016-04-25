package edu.csula.datascience.weatherApi;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;

import org.json.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.mongodb.util.JSON;

import edu.csula.datascience.acquisition.Source;
import twitter4j.Status;

public class SourceData implements Source<JSONObject>{
	
	private long minId;
    
	
	 public SourceData(long minId) {
	        this.minId = minId;  
	    }
	 
	
	@Override
	public boolean hasNext() {
		
		return minId > 0;
	}
	
	@Override
	public Collection<JSONObject> next() {
		 List<JSONObject> list = Lists.newArrayList();
		 
		 String id="";
		 String st = String.valueOf(minId);
		 int len = st.length();
		 	if(len == 1) id = "0000"+st;
			if(len == 2) id = "000"+st;
			if(len == 3) id = "00"+st;
			if(len == 4) id = "0"+st;
			if(len == 5) id = st;
		 
		 String jsonString = getTVmaze("http://api.openweathermap.org/data/2.5/weather?zip="+id+",us&appid=5a68c72cd25934edf067e81ba0664d2b");
		 
		 Object obj = JSON.parse(jsonString);
			Gson gson = new Gson();
			String jo = gson.toJson(obj);
		
			JSONParser parser = new JSONParser();
			JSONObject json = null;
			try {
				json = (JSONObject) parser.parse(jo);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 list.add(json);
		 
		 minId--;
		return list;
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
