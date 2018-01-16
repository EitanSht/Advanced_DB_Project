/**
 * 
 */
package org.bgu.ise.ddb.registration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.time.DateUtils;
import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bson.types.ObjectId;
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

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController {
	/**
	 * The function checks if the username exist, in case of positive answer
	 * HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT, else
	 * insert the user to the system and set to HttpStatus in HttpServletResponse
	 * HttpStatus.OK
	 * 
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@SuppressWarnings("deprecation")
	@RequestMapping(value = "register_new_customer", method = { RequestMethod.POST })
	public void registerNewUser(@RequestParam("username") String username, @RequestParam("password") String password,
			@RequestParam("firstName") String firstName, @RequestParam("lastName") String lastName,
			HttpServletResponse response) {

		Mongo mongo;
		DB db;
		DBCollection collection;
		try {
			// Connection Settings
			mongo = new Mongo("localhost", 27017);
			db = mongo.getDB("db");
			collection = db.getCollection("users");
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
			HttpStatus status = HttpStatus.BAD_REQUEST;
			response.setStatus(status.value());
			return;
		}
		try {
			if (isExistUser(username)) {
				System.out.println("Username already exists");
				HttpStatus status = HttpStatus.REQUEST_TIMEOUT;
				response.setStatus(status.value());
				return;
			}
		} catch (IOException e) {
			System.out.println("A problem has occured.");
			e.printStackTrace();
		}
		try {
			BasicDBObject document = new BasicDBObject();
			document.put("username", username);
			document.put("password", password);
			document.put("firstName", firstName);
			document.put("lastName", lastName);
			collection.insert(document);
			HttpStatus status = HttpStatus.OK;
			response.setStatus(status.value());
			System.out.println("The user has been registered.");
		} catch (Exception e) {
			System.out.println("A problem has occured.");
			HttpStatus status = HttpStatus.BAD_REQUEST;
			response.setStatus(status.value());
		}
	}

	/**
	 * The function returns true if the received username exist in the system
	 * otherwise false
	 * 
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	@RequestMapping(value = "is_exist_user", method = { RequestMethod.GET })
	public boolean isExistUser(@RequestParam("username") String username) throws IOException {
		boolean result = false;

		try {
			// Connection Settings
			Mongo mongo = new Mongo("localhost", 27017);
			DB db = mongo.getDB("db");
			DBCollection collection = db.getCollection("users");

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

	/**
	 * The function returns true if the received username and password match a
	 * system storage entry, otherwise false
	 * 
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	@RequestMapping(value = "validate_user", method = { RequestMethod.POST })
	public boolean validateUser(@RequestParam("username") String username, @RequestParam("password") String password)
			throws IOException {
		boolean result = false;
		Mongo mongo;
		DB db;
		DBCollection collection;
		
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
			if (isExistUser(username)) {
				BasicDBObject whereQuery = new BasicDBObject();
				whereQuery.put("username", username);
				whereQuery.put("password", password);
				DBCursor cursor = collection.find(whereQuery);
				if (cursor.hasNext()) {
					result = true;
				} else {
					System.out.println("The password is incorrect.");
				}
			} else {
				System.out.println("Username does not exist.");
			}
		} catch (IOException e) {
			System.out.println("A problem has occured.");
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * The function retrieves number of the registered users in the past n days
	 * 
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	@RequestMapping(value = "get_number_of_registred_users", method = { RequestMethod.GET })
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException {
		int result = 0;
		Mongo mongo;
		DB db;
		DBCollection collection;
		
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

		Date startDate = DateUtils.addDays(new Date(), -days);
		Date endDate = new Date();

		DBCursor cursor = collection.find();
		while (cursor.hasNext()) {
			BasicDBObject object = (BasicDBObject) cursor.next();
			String oid = object.getString("_id");
			ObjectId o = new ObjectId(oid);
			Date d = o.getDate();
			if (!d.before(startDate) && !d.after(endDate)) {
				result++;
			}
		}
		return result;
	}

	/**
	 * The function retrieves all the users
	 * 
	 * @return
	 */
	@SuppressWarnings("deprecation")
	@RequestMapping(value = "get_all_users", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public User[] getAllUsers() {
		Mongo mongo;
		DB db;
		DBCollection collection;
		ArrayList<User> list = new ArrayList<User>();

		try {
			// Connection Settings
			mongo = new Mongo("localhost", 27017);
			db = mongo.getDB("db");
			collection = db.getCollection("users");
		} catch (MongoException me) {
			System.out.println("A problem has occured.");
			me.getMessage();
			return new User[] {};
		}

		DBCursor cursor = collection.find();
		while (cursor.hasNext()) {
			DBObject o = cursor.next();
			String username = (String) o.get("username");
			String firstName = (String) o.get("firstName");
			String lastName = (String) o.get("lastName");
			User u = new User(username, firstName, lastName);
			list.add(u);
		}
		User[] users = list.toArray(new User[list.size()]);
		return users;
	}
}
