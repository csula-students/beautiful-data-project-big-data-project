package edu.csula.datascience.datacollection;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Sorts.descending;

import java.util.List;
import java.util.Random;

import org.bson.Document;

import com.google.common.collect.Lists;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class Main {

	public static void main(String[] args) {
		MongoClient mongoClient = new MongoClient("localhost");
		MongoDatabase database = mongoClient.getDatabase("test");
		// database.createCollection("test");
		 MongoCollection<Document> collection =database.getCollection("t1");
//		for (String s : mongoClient.getDatabaseNames()) {
//			   System.out.println(s);
//			}
		 Document doc = new Document("name", "MongoDB")
		            .append("type", "database")
		            .append("count", 1)
		            .append("info", new Document("x", 203).append("y", 102));
		 
		// to insert document
	        collection.insertOne(doc);
	        System.out.println(
	            String.format(
	                "Inserted new document %s",
	                doc.toJson()
	            )
	        );
	        
	     // to find a document out of collection
	        Document foundDoc = collection.find().first();
	        System.out.println(
	            String.format(
	                "Found document %s",
	                foundDoc.toJson()
	            )
	        );
//	        // insert 1000 documents
//	        List<Document> docs = Lists.newArrayList();
//	        for (int i = 0; i < 100; i ++) {
//	            Document newDoc = new Document("name", i)
//	                .append("count", new Random().nextInt(100));
//	            docs.add(newDoc);
//	        }
//	        collection.insertMany(docs);
//	        System.out.println("Inserted many random count documents");
//
//	        // get many documents at once with iterator
//	        try (MongoCursor<Document> cursor = collection.find().iterator()) {
//	            while (cursor.hasNext()) {
//	                System.out.println(
//	                    String.format(
//	                        "Got document %s",
//	                        cursor.next().toJson()
//	                    )
//	                );
//	            }
//	        }
//
//	        // query by field to find single document
//	        Document mongoDoc = collection.find(eq("name", "MongoDB")).first();
//	        System.out.println(
//	            String.format(
//	                "Found document by name `MongoDB` %s",
//	                mongoDoc.toJson()
//	            )
//	        );
//
//	        // query by field to find a list of documents
//	        collection.find(and(gt("count", 50), lt("count", 70)))
//	            .sort(descending("count")) // sort by count in desc order
//	            .projection(excludeId())
//	            .forEach((Block<? super Document>) d -> {
//	                System.out.println(d.toJson());
//	            });
	}

}
