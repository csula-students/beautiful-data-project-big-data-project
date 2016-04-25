package edu.csula.datascience.datacollection;

import org.json.JSONObject;

/**
 * A simple model for testing
 */
public class SimpleModel {
	private final String id;
    private final JSONObject json;
   

    public SimpleModel(String id, JSONObject json) {
    	this.id = id;
        this.json = json;
    }

   

    public static SimpleModel build(MockData data) {
        return new SimpleModel(data.getId(),data.getJson());
    }



	public String getId() {
		return id;
	}



	public JSONObject getJson() {
		return json;
	}
}
