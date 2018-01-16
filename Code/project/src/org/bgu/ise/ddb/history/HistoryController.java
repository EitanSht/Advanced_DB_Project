/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * @author Alex
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController {
	/**
	 * The function inserts to the system storage triple(s)(username, title,
	 * timestamp). The timestamp - in ms since 1970 Advice: better to insert the
	 * history into two structures( tables) in order to extract it fast one with the
	 * key - username, another with the key - title
	 * 
	 * @param username
	 * @param title
	 * @param response
	 */
	@SuppressWarnings("deprecation")
	@RequestMapping(value = "insert_to_history", method = { RequestMethod.GET })
	public void insertToHistory(@RequestParam("username") String username, @RequestParam("title") String title,
			HttpServletResponse response) {
		Mongo mongo;
		DB db;
		DBCollection collection_users;
		DBCollection collection_movies;

		try {
			// Connection Settings
			mongo = new Mongo("localhost", 27017);
			db = mongo.getDB("db");
			collection_users = db.getCollection("history_users");
			collection_movies = db.getCollection("history_movies");
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
			return;
		}

		long timestamp_long = Instant.now().toEpochMilli(); // Timestamp from Epoch (1-1-1970)
		String timestamp = Long.toString(timestamp_long);

		try {
			if (isUserExist(username) && isMovieExist(title)) {
				BasicDBObject whereUser = new BasicDBObject();
				BasicDBObject whereMovie = new BasicDBObject();
				whereUser.put("user", username);
				whereMovie.put("movie", title);

				DBCursor cursor_user = collection_users.find(whereUser);
				if (cursor_user.hasNext()) { // User is in the history
					System.out.println("\t\t >>> Adding to the user history");
					DBObject object = cursor_user.next();
					BasicDBList moviesDBList = (BasicDBList) object.get("movie_list");

					// Preparing the new movie & timestamp data
					BasicDBObject movie_pair = new BasicDBObject();
					movie_pair.put("movie", title);
					movie_pair.put("timestamp", timestamp);
					moviesDBList.add(movie_pair);

					// Updating the document
					BasicDBObject newDocument = new BasicDBObject();
					newDocument.put("user", username);
					newDocument.put("movie_list", moviesDBList);
					BasicDBObject searchQuery = new BasicDBObject().append("user", username);
					collection_users.update(searchQuery, newDocument);
				} else { // User is not in the history
					System.out.println("\t\t >>> Creating the user history");

					// Insertion to the movie history collection
					List<Object> moviesDBList = new BasicDBList();
					BasicDBObject doc_list = new BasicDBObject();
					doc_list.put("movie", title);
					doc_list.put("timestamp", timestamp);
					moviesDBList.add(doc_list);

					// Inserting the document to the users history
					BasicDBObject document = new BasicDBObject();
					document.put("user", username);
					document.put("movie_list", moviesDBList);
					collection_users.insert(document);
				}

				DBCursor cursor_movie = collection_movies.find(whereMovie);
				if (cursor_movie.hasNext()) { // Movie is in the history
					System.out.println("\t\t >>> Adding to the movie history");
					DBObject object = cursor_movie.next();
					BasicDBList usersDBList = (BasicDBList) object.get("user_list");

					// Preparing the new user & timestamp data
					BasicDBObject user_pair = new BasicDBObject();
					user_pair = new BasicDBObject();
					user_pair.put("user", username);
					user_pair.put("timestamp", timestamp);
					usersDBList.add(user_pair);

					// Updating the document
					BasicDBObject newDocument = new BasicDBObject();
					newDocument.put("movie", title);
					newDocument.put("user_list", usersDBList);
					BasicDBObject searchQuery = new BasicDBObject().append("movie", title);
					collection_movies.update(searchQuery, newDocument);
				} else { // Movie is not in the history
					System.out.println("\t\t >>> Creating the movie history");

					// Insertion to the movie history collection
					List<Object> usersDBList = new BasicDBList();
					BasicDBObject doc_list = new BasicDBObject();
					doc_list.put("user", username);
					doc_list.put("timestamp", timestamp);
					usersDBList.add(doc_list);

					// Inserting the document to the movie history
					BasicDBObject document = new BasicDBObject();
					document.put("movie", title);
					document.put("user_list", usersDBList);
					collection_movies.insert(document);
				}
			} else {
				System.out.println("Check the username or the movie title.");
			}
		} catch (MongoException me) {
			System.out.println("A problem occured.");
			me.getMessage();
		}

		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}

	/**
	 * The function retrieves users' history The function return array of pairs
	 * <title,viewtime> sorted by VIEWTIME in descending order
	 * 
	 * @param username
	 * @return
	 */
	@SuppressWarnings("deprecation")
	@RequestMapping(value = "get_history_by_users", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public HistoryPair[] getHistoryByUser(@RequestParam("entity") String username) {
		Mongo mongo;
		DB db;
		DBCollection collection_users;
		ArrayList<HistoryPair> listHp = new ArrayList<HistoryPair>();

		try {
			// Connection Settings
			mongo = new Mongo("localhost", 27017);
			db = mongo.getDB("db");
			collection_users = db.getCollection("history_users");
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
			return new HistoryPair[] {};
		}

		BasicDBObject whereUser = new BasicDBObject();
		whereUser.put("user", username);
		try {
			DBCursor cursor_user = collection_users.find(whereUser);
			if (cursor_user.hasNext()) { // User in the history
				DBObject object = cursor_user.next();

				DBObject dbObject = (DBObject) object.get("movie_list");
				for (String key : dbObject.keySet()) {
					DBObject o = (DBObject) dbObject.get(key);
					String mov = (String) o.get("movie");
					String ts = (String) o.get("timestamp");

					Date date = new Date(Long.parseLong(ts));
					HistoryPair hp = new HistoryPair(mov, date);
					listHp.add(hp);
				}
			}
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
			return new HistoryPair[] {};
		}

		try {
			// Custom sorting of the list by date (timestamp) - descending
			Collections.sort(listHp, (new Comparator<HistoryPair>() {
				@Override
				public int compare(HistoryPair lhp, HistoryPair rhp) {
					Date leftTime = lhp.getViewtime();
					Date rightTime = rhp.getViewtime();
					return leftTime.compareTo(rightTime);
				}
			}).reversed());
		} catch (Exception e) {
			System.out.println("A problem has occured.");
			return new HistoryPair[] {};
		}

		HistoryPair[] hp = listHp.toArray(new HistoryPair[listHp.size()]);
		return hp;
	}

	/**
	 * The function retrieves items' history The function return array of pairs
	 * <username,viewtime> sorted by VIEWTIME in descending order
	 * 
	 * @param title
	 * @return
	 */
	@SuppressWarnings("deprecation")
	@RequestMapping(value = "get_history_by_items", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public HistoryPair[] getHistoryByItems(@RequestParam("entity") String title) {
		Mongo mongo;
		DB db;
		DBCollection movies;
		ArrayList<HistoryPair> listHp = new ArrayList<HistoryPair>();

		try {
			// Connection Settings
			mongo = new Mongo("localhost", 27017);
			db = mongo.getDB("db");
			movies = db.getCollection("history_movies");
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
			return new HistoryPair[] {};
		}

		BasicDBObject whereMovie = new BasicDBObject();
		whereMovie.put("movie", title);
		try {
			DBCursor cursor_movie = movies.find(whereMovie);
			if (cursor_movie.hasNext()) { // User in the history
				DBObject object = cursor_movie.next();

				DBObject dbObject = (DBObject) object.get("user_list");
				for (String key : dbObject.keySet()) {
					DBObject o = (DBObject) dbObject.get(key);
					String user = (String) o.get("user");
					String ts = (String) o.get("timestamp");

					Date date = new Date(Long.parseLong(ts));
					HistoryPair hp = new HistoryPair(user, date);
					listHp.add(hp);
				}
			}
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
			return new HistoryPair[] {};
		}

		// Custom sorting of the list by date (timestamp) - descending
		try {
			Collections.sort(listHp, (new Comparator<HistoryPair>() {
				@Override
				public int compare(HistoryPair lhp, HistoryPair rhp) {
					Date leftTime = lhp.getViewtime();
					Date rightTime = rhp.getViewtime();
					return leftTime.compareTo(rightTime);
				}
			}).reversed());
		} catch (Exception e) {
			System.out.println("A problem has occured.");
			return new HistoryPair[] {};
		}

		HistoryPair[] hp = listHp.toArray(new HistoryPair[listHp.size()]);
		return hp;
	}

	/**
	 * The function retrieves all the users that have viewed the given item
	 * 
	 * @param title
	 * @return
	 */
	@SuppressWarnings("deprecation")
	@RequestMapping(value = "get_users_by_item", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public User[] getUsersByItem(@RequestParam("title") String title) {
		Mongo mongo;
		DB db;
		DBCollection movies;
		DBCollection users;
		ArrayList<User> list_users = new ArrayList<User>();

		try {
			mongo = new Mongo("localhost", 27017);
			db = mongo.getDB("db");
			movies = db.getCollection("history_movies");
			users = db.getCollection("users");
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
			return new User[] {};
		}

		BasicDBObject whereMovie = new BasicDBObject();
		whereMovie.put("movie", title);

		try {
			DBCursor cursor_movie = movies.find(whereMovie);
			if (cursor_movie.hasNext()) { // User in the history
				DBObject object = cursor_movie.next();

				DBObject dbObject = (DBObject) object.get("user_list");
				for (String key : dbObject.keySet()) {
					DBObject o = (DBObject) dbObject.get(key);
					String user = (String) o.get("user");

					BasicDBObject whereUser = new BasicDBObject();
					whereUser.put("username", user);

					DBCursor cursor_u = users.find(whereUser);
					if (cursor_u.hasNext()) { // User in the history
						DBObject obj = cursor_u.next();
						String username = (String) obj.get("username");
						String firstName = (String) obj.get("firstName");
						String lastName = (String) obj.get("lastName");
						User u = new User(username, firstName, lastName);
						list_users.add(u);
					} else {
						System.out.println("User not found.");
					}
				}
			} else {
				System.out.println("The title was not found in the DB.");
				return new User[] {};
			}
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
			return new User[] {};
		}

		// Removal of duplicate values
		HashSet<Object> dup = new HashSet<>();
		try {
			list_users.removeIf(e -> !dup.add(e.getUsername()));
		} catch (Exception e) {
			System.out.println("A problem has occured.");
			return new User[] {};
		}

		User[] user_res = list_users.toArray(new User[list_users.size()]);
		return user_res;
	}

	/**
	 * The function calculates the similarity score using Jaccard similarity
	 * function: sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|, where U(i)
	 * is the set of usernames which exist in the history of the item i.
	 * 
	 * @param title1
	 * @param title2
	 * @return
	 */
	@RequestMapping(value = "get_items_similarity", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	public double getItemsSimilarity(@RequestParam("title1") String title1, @RequestParam("title2") String title2) {
		User[] usersA = getUsersByItem(title1);
		User[] usersB = getUsersByItem(title2);
		Set<String> s1 = new HashSet<String>();
		Set<String> s2 = new HashSet<String>();

		try {
			for (User a : usersA) {
				s1.add(a.getUsername());
			}
			for (User b : usersB) {
				s2.add(b.getUsername());
			}
		} catch (Exception e) {
			System.out.println("A problem has occured.");
			return -1;
		}

		// Intersection score
		s1.retainAll(s2);
		double intersectionScore = s1.size();

		s1 = new HashSet<String>();
		try {
			for (User a : usersA) {
				s1.add(a.getUsername());
			}
		} catch (Exception e) {
			System.out.println("A problem has occured.");
			return -1;
		}

		// Union score
		s1.addAll(s2);
		double unionScore = s1.size();

		// Similarity score
		double sim_score = 0;
		if (unionScore != 0) { // Check the division by zero
			sim_score = intersectionScore / unionScore;
		}
		return sim_score;
	}

	// Added Methods //
	
	/**
	 * The function returns True if the movie exists in the DB
	 * 
	 * @param movie - The title of the movie
	 * @return boolean - True/False
	 */
	@SuppressWarnings("deprecation")
	public boolean isMovieExist(String movie) {
		Mongo mongo;
		DB db;
		DBCollection collection;
		boolean result = false;

		try {
			// Connection Settings
			mongo = new Mongo("localhost", 27017);
			db = mongo.getDB("db");
			collection = db.getCollection("items");
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
			return result;
		}

		try {
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("movie", movie);
			DBCursor cursor = collection.find(whereQuery);
			if (cursor.hasNext()) {
				result = true;
			}
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
		}
		return result;
	}

	/**
	 * The function returns True if the user exists in the DB
	 * 
	 * @param username - The username of the user
	 * @return boolean - True/False
	 */
	@SuppressWarnings("deprecation")
	public boolean isUserExist(String username) {
		Mongo mongo;
		DB db;
		DBCollection collection;
		boolean result = false;

		try {
			// Connection Settings
			mongo = new Mongo("localhost", 27017);
			db = mongo.getDB("db");
			collection = db.getCollection("users");
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
			return result;
		}
		try {
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("username", username);
			DBCursor cursor = collection.find(whereQuery);
			if (cursor.hasNext()) {
				result = true;
			}
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
		}
		return result;
	}
}
