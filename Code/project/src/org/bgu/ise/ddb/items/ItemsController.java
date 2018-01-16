/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.opencsv.CSVReader;

/**
 * @author Alex
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {
	/**
	 * The function copy all the items(title and production year) from the Oracle
	 * table MediaItems to the System storage. The Oracle table and data should be
	 * used from the previous assignment
	 */
	@SuppressWarnings("deprecation")
	@RequestMapping(value = "fill_media_items", method = { RequestMethod.GET })
	public void fillMediaItems(HttpServletResponse response) {
		// Oracle connection details
		Connection conn = null;
		final String username = "eitansht";
		final String password = "abcd";
		final String connectionUrl = "jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/oracle";
		final String driver = "oracle.jdbc.driver.OracleDriver";
		Mongo mongo = new Mongo("localhost", 27017);
		DB db = mongo.getDB("db");
		DBCollection collection = db.getCollection("items");

		// Cleaning of the DB
		collection.remove(new BasicDBObject());

		// Connection to Oracle
		try {
			Class.forName(driver); // Driver registration
			conn = DriverManager.getConnection(connectionUrl, username, password);
			conn.setAutoCommit(false);
		} catch (Exception e) {
			System.out.println("An error during connecting to the server has occured .");
			HttpStatus status = HttpStatus.NOT_FOUND;
			response.setStatus(status.value());
			return;
		}

		// Retrieves the data
		PreparedStatement ps = null;
		String query = "SELECT * FROM MEDIAITEMS"; // Query definition
		try {
			ps = conn.prepareStatement(query); // Compilation of the query
			ResultSet rs = ps.executeQuery();
			while (rs.next()) { // Iteration over the results
				String movie = rs.getString("TITLE");
				int year = rs.getInt("PROD_YEAR");
				try {
					BasicDBObject document = new BasicDBObject();
					document.put("movie", movie);
					document.put("year", year);
					collection.insert(document);
				} catch (Exception e) {
					System.out.println("A problem occured during the insertion of the data to MongoDB.");
					HttpStatus status = HttpStatus.NOT_FOUND;
					response.setStatus(status.value());
				}
			}
			rs.close();
		} catch (SQLException e) {
			System.out.println("An error has occured.");
			HttpStatus status = HttpStatus.NOT_FOUND;
			response.setStatus(status.value());
			return;
		} finally {
			try {
				if (ps != null) {
					ps.close();
				}
			} catch (SQLException e) {
				System.out.println("An error has occured.");
				HttpStatus status = HttpStatus.NOT_FOUND;
				response.setStatus(status.value());
				return;
			}
		}

		// Disconnect from Oracle
		try {
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("An error has occured.");
			HttpStatus status = HttpStatus.NOT_FOUND;
			response.setStatus(status.value());
			return;
		}
		
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}

	/**
	 * The function copy all the items from the remote file, the remote file have
	 * the same structure as the films file from the previous assignment. You can
	 * assume that the address protocol is HTTP
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	@RequestMapping(value = "fill_media_items_from_url", method = { RequestMethod.GET })
	public void fillMediaItemsFromUrl(@RequestParam("url") String urladdress, HttpServletResponse response)
			throws IOException {
		Mongo mongo;
		DB db;
		DBCollection collection;
		/*
		 * https://drive.google.com/uc?export=download&id=1kKKkAJlqf0xzfOmLpX9sZC6Zfq8JZfxr
		 */
		try {
			// Connection Settings
			mongo = new Mongo("localhost", 27017);
			db = mongo.getDB("db");
			collection = db.getCollection("items");
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
			return;
		}

		// Cleaning of the DB
		collection.remove(new BasicDBObject());

		try {
			URL stockURL = new URL(urladdress);
			BufferedReader in = new BufferedReader(new InputStreamReader(stockURL.openStream()));
			CSVReader reader = new CSVReader(in);
			String[] record = null;

			while ((record = reader.readNext()) != null) {
				String rec = Arrays.toString(record);
				rec = rec.substring(1, rec.length() - 1);
				String[] movie_details = rec.split(Pattern.quote(","));
				String movie = movie_details[0];
				String year_str = movie_details[1];
				year_str = year_str.replaceAll("\\s+", "");
				int year = Integer.parseInt(year_str);

				try {
					BasicDBObject document = new BasicDBObject();
					document.put("movie", movie);
					document.put("year", year);
					collection.insert(document);
				} catch (MongoException me) {
					System.out.println("A problem occured during the insertion of the data to MongoDB.");
					HttpStatus status = HttpStatus.NOT_FOUND;
					response.setStatus(status.value());
				}
			}
			reader.close();
		} catch (Exception e) {
			System.out.println("An exception occured. Check the link.");
			HttpStatus status = HttpStatus.NOT_FOUND;
			response.setStatus(status.value());
			return;
		}
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}

	/**
	 * The function retrieves from the system storage N items, order is not
	 * important( any N items)
	 * 
	 * @param topN
	 *            - how many items to retrieve
	 * @return
	 */
	@SuppressWarnings("deprecation")
	@RequestMapping(value = "get_topn_items", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public MediaItems[] getTopNItems(@RequestParam("topn") int topN) {
		Mongo mongo;
		DB db;
		DBCollection collection;
		ArrayList<MediaItems> list = new ArrayList<MediaItems>();

		if (topN < 0) {
			System.out.println("Please select a legal number.");
			return new MediaItems[] {};
		}
		try {
			// Connection Settings
			mongo = new Mongo("localhost", 27017);
			db = mongo.getDB("db");
			collection = db.getCollection("items");
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
			return new MediaItems[] {};
		}

		int i = 0;
		try {
			DBCursor cursor = collection.find();
			while (cursor.hasNext() && i < topN) {
				DBObject o = cursor.next();
				String movie = (String) o.get("movie");
				int year = (Integer) o.get("year");
				MediaItems m = new MediaItems(movie, year);
				list.add(m); // Adding the movie to the list
				i++;
			}
		} catch (MongoException me) {
			System.out.println("An error has occured.");
			me.getMessage();
			return new MediaItems[] {};
		}
		if (list.size() == 0) {
			return new MediaItems[] {};
		}
		if (topN > list.size()) {
			System.out.println("Please select an different number of records.");
			return new MediaItems[] {};
		}
		return list.toArray(new MediaItems[list.size()]);
	}
}
