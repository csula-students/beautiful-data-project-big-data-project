package edu.csula.datascience.datacollection;

import org.json.JSONObject;

/**
 * Mock raw data
 */
public class MockData {
   String id;
	public String getId() {
	return id;
}

public void setId(String id) {
	this.id = id;
}

	JSONObject json;

    public MockData(String id,JSONObject json) {
      this.id = id;
//        this.content = content;
    	this.json=json;
    	
    }

	public JSONObject getJson() {
		return json;
	}

	public void setJson(JSONObject json) {
		this.json = json;
	}

    
}
