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

import com.opencsv.CSVReader;

/**
 * @author Alex
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {

	private MediaItems[] media_items = null;

	/**
	 * The function copy all the items(title and production year) from the Oracle
	 * table MediaItems to the System storage. The Oracle table and data should be
	 * used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method = { RequestMethod.GET })
	public void fillMediaItems(HttpServletResponse response) {
		System.out.println("was here");
		
		// Connection details
		Connection conn = null;
		final String username = "eitansht";
		final String password = "abcd";
		final String connectionUrl = "jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/oracle";
		final String driver = "oracle.jdbc.driver.OracleDriver";

		// Connect to Oracle
		try {
			Class.forName(driver); // Driver registration
			conn = DriverManager.getConnection(connectionUrl, username, password); // Connection
			conn.setAutoCommit(false);
		} catch (Exception e) {
			e.printStackTrace();
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
			media_items = null;
			ArrayList<MediaItems> list = new ArrayList<MediaItems>();
			while (rs.next()) { // Iteration over the results
				String movie = rs.getString("TITLE");
				int year = rs.getInt("PROD_YEAR");
				MediaItems m = new MediaItems(movie, year);
				list.add(m); // Adding the movie to the list
			}
			media_items = list.toArray(new MediaItems[list.size()]); // Returns an array from the arraylist
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			HttpStatus status = HttpStatus.NOT_FOUND;
			response.setStatus(status.value());
			return;
		} finally {
			try {
				if (ps != null) {
					ps.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
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
	@RequestMapping(value = "fill_media_items_from_url", method = { RequestMethod.GET })
	public void fillMediaItemsFromUrl(@RequestParam("url") String urladdress, HttpServletResponse response)
			throws IOException {
		System.out.println(urladdress);
		/*
		 * https://drive.google.com/uc?export=download&id=1kKKkAJlqf0xzfOmLpX9sZC6Zfq8JZfxr
		 */
		media_items = null;
		ArrayList<MediaItems> list = new ArrayList<MediaItems>();
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

				MediaItems m = new MediaItems(movie, year);
				list.add(m);
			}
			reader.close();
			media_items = list.toArray(new MediaItems[list.size()]);
		} catch (Exception e) {
			media_items = null;
			System.out.println("An exception occured. Check the link.");
			HttpStatus status = HttpStatus.NOT_FOUND;
			response.setStatus(status.value());
			return;
		}
		//
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}

	/**
	 * The function retrieves from the system storage N items, order is not
	 * important( any N items)
	 * 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public MediaItems[] getTopNItems(@RequestParam("topn") int topN) {
		if (null == media_items) {
			return new MediaItems[] {};
		}
		if ((topN > media_items.length) || (topN < 0)) {
			System.out.println("Please select an different number of records.");
			return new MediaItems[] {};
		}
		ArrayList<MediaItems> list = new ArrayList<MediaItems>();
		for (int i = 0; i < topN; i++) {
			list.add(media_items[i]);
		}
		return list.toArray(new MediaItems[list.size()]);
	}
}
