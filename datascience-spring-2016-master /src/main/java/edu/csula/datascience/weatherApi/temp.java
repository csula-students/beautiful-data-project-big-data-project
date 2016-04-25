package edu.csula.datascience.weatherApi;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

import org.json.JSONArray;
import org.json.simple.JSONObject;

import twitter4j.Status;

public class temp {

	public static void main(String[] args) throws IOException {
		
		FileWriter f=new FileWriter("file/data/file.json");
	SourceData source = new SourceData(99999);
	CollectData collector = new CollectData();
	System.out.println(source);
	
	while (source.hasNext()) {
        Collection<JSONObject> ts = source.next();
        Collection<JSONObject> collect_data = collector.mungee(ts);
        collector.save(collect_data);
        
        JSONArray array = new JSONArray();
        array.put(collect_data);
        String str = array.toString();
        f.write(str);
    }
	
	}
}
