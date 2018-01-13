/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.tuple.Triple;
import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javafx.util.Pair;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController {
	// History tables
	Map<String, List<Pair<String, String>>> user_history = new HashMap<String, List<Pair<String, String>>>();
	Map<String, List<Pair<String, String>>> movie_history = new HashMap<String, List<Pair<String, String>>>();

//	map.put("dog", "type of animal");
	//	System.out.println(map.get("dog"));
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
	@RequestMapping(value = "insert_to_history", method = { RequestMethod.GET })
	public void insertToHistory(@RequestParam("username") String username, @RequestParam("title") String title,
			HttpServletResponse response) {
		System.out.println(username + " " + title);
		long timestamp_long = Instant.now().toEpochMilli(); // Timestamp from Epoch (1-1-1970)
		String timestamp = Long.toString(timestamp_long);
		System.out.println(timestamp);
		
		List<Pair<String, String>> pair_list = null;
		Pair<String, String> user_p = new Pair<String, String> (title, timestamp);
		Pair<String, String> movie_p = new Pair<String, String> (username, timestamp);
		
		if (user_history.containsKey(username)) {
			System.out.println("added");
			pair_list = user_history.get(username);
		}
		else {
			System.out.println("new");
			pair_list = new ArrayList<Pair<String, String>>();
		}
		pair_list.add(user_p);
		user_history.put(username, pair_list); // Adding the event to users history
		
		pair_list = null;
		if (movie_history.containsKey(title)) {
			System.out.println("added");
			pair_list = movie_history.get(title);
		}
		else {
			System.out.println("new");
			pair_list = new ArrayList<Pair<String, String>>();
		}
		pair_list.add(movie_p);
		movie_history.put(title, pair_list); // Adding the event to movies history
		
		System.out.println("Users : " + Arrays.asList(user_history));
		System.out.println("Movies: " + Arrays.asList(movie_history));

		//
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
	@RequestMapping(value = "get_history_by_users", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public HistoryPair[] getHistoryByUser(@RequestParam("entity") String username) {
		// :TODO your implementation
		HistoryPair hp = new HistoryPair("aa", new Date());
		System.out.println("ByUser " + hp);
		return new HistoryPair[] { hp };
	}

	/**
	 * The function retrieves items' history The function return array of pairs
	 * <username,viewtime> sorted by VIEWTIME in descending order
	 * 
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_history_by_items", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public HistoryPair[] getHistoryByItems(@RequestParam("entity") String title) {
		// :TODO your implementation
		HistoryPair hp = new HistoryPair("aa", new Date());
		System.out.println("ByItem " + hp);
		return new HistoryPair[] { hp };
	}

	/**
	 * The function retrieves all the users that have viewed the given item
	 * 
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_users_by_item", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public User[] getUsersByItem(@RequestParam("title") String title) {
		// :TODO your implementation
		User hp = new User("aa", "aa", "aa");
		System.out.println(hp);
		return new User[] { hp };
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
		// :TODO your implementation
		double ret = 0.0;
		return ret;
	}

}
