/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController{
	/**
	 * The function inserts to the system storage triple(s)(username, title, timestamp). 
	 * The timestamp - in ms since 1970
	 * Advice: better to insert the history into two structures( tables) in order to extract it fast one with the key - username, another with the key - title
	 * @param username
	 * @param title
	 * @param response
	 */
	@RequestMapping(value = "insert_to_history", method={RequestMethod.GET})
	public void insertToHistory (@RequestParam("username")    String username,
			@RequestParam("title")   String title,
			HttpServletResponse response){
		String timestamp=Long.toString(System.currentTimeMillis());
		try{
			Jedis jedis=getJedisConnection();
			Pipeline pp=jedis.pipelined();

			//first hash - userName_h-> (title,timestamp)
			pp.hset(username+"_h", title, timestamp);

			//second hash - title_h-> (userName,timestamp)
			pp.hset(title+"_h",username , timestamp);

			pp.sync();
			jedis.close();

			System.out.println("History inserted:"+username+" "+title+" "+timestamp);

			HttpStatus status = HttpStatus.OK;
			response.setStatus(status.value());
		}
		catch (Exception e){
			e.printStackTrace();
			HttpStatus status = HttpStatus.CONFLICT;
			response.setStatus(status.value());
		}
	}



	/**
	 * The function retrieves  users' history
	 * The function return array of pairs <title,viewtime> sorted by VIEWTIME in descending order
	 * @param username
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@RequestMapping(value = "get_history_by_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByUser(@RequestParam("entity")    String username){
		HistoryPair[] result=null;
		try{
			Jedis jedis=getJedisConnection();
			// get the user's history
			Map<String,String> userPairs =jedis.hgetAll(username+"_h");

			// sort by key (timestamp)
			List sortedKeys=new ArrayList(userPairs.keySet());
			Collections.sort(sortedKeys ,Collections.reverseOrder());

			result=new HistoryPair[sortedKeys.size()];
			for (int i = 0; i < sortedKeys.size(); i++) {
				String title=userPairs.get(sortedKeys.get(i));
				Long milliSec= Long.valueOf((String) sortedKeys.get(i));
				Date date=new Date(milliSec);
				result[i]=new HistoryPair(title, date);
			}

			System.out.println("Retrive History of The User "+username);
			jedis.close();
			return result;
		}
		catch (Exception e){
			e.printStackTrace();
			return null;
		}
	}


	/**
	 * The function retrieves  items' history
	 * The function return array of pairs <username,viewtime> sorted by VIEWTIME in descending order
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_history_by_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByItems(@RequestParam("entity")    String title){
		HistoryPair[] result=null;
		try{
			Jedis jedis=getJedisConnection();
			// get the movie's history
			Map<String,String> titlePairs =jedis.hgetAll(title+"_h");

			// sort by key (timestamp)
			List sortedKeys=new ArrayList(titlePairs.keySet());
			Collections.sort(sortedKeys ,Collections.reverseOrder());

			result=new HistoryPair[sortedKeys.size()];
			for (int i = 0; i < sortedKeys.size(); i++) {
				String user=titlePairs.get(sortedKeys.get(i));
				Long milliSec= Long.valueOf((String) sortedKeys.get(i));
				Date date=new Date(milliSec);
				result[i]=new HistoryPair(user, date);
			}

			System.out.println("Retrive History of The Movie "+title);
			jedis.close();
			return result;
		}
		catch (Exception e){
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * The function retrieves all the  users that have viewed the given item
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_users_by_item",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  User[] getUsersByItem(@RequestParam("title") String title){
		User[] result=null;
		try{
			Jedis jedis= getJedisConnection();

			// get the movie's history
			Map<String,String> titlePairs =jedis.hgetAll(title+"_h");
			HashSet<String> all_users = new HashSet<String>();
			all_users.addAll(titlePairs.values());
			result=new User[all_users.size()];
			int i=0;
			for (String username : all_users){
				User u = new User(username, jedis.hget(username, "firstName"), jedis.hget(username, "lastName"));
				System.out.println(u);
				result[i]=u;
				i++;
			}
			jedis.close();

		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
		System.out.println("Retrive all the users that saw the movie "+title);
		return result;
	}

	/**
	 * The function calculates the similarity score using Jaccard similarity function:
	 *  sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|,
	 *  where U(i) is the set of usernames which exist in the history of the item i.
	 * @param title1
	 * @param title2
	 * @return
	 */
	@RequestMapping(value = "get_items_similarity",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	public double  getItemsSimilarity(@RequestParam("title1") String title1,
			@RequestParam("title2") String title2){
		double sim = 0.0;
		try{
			Jedis jedis= getJedisConnection();

			// get the movie 1 history
			Map<String,String> title1Pairs =jedis.hgetAll(title1+"_h");
			HashSet<String> users1 = new HashSet<String>();
			users1.addAll(title1Pairs.values());
			// get the movie 2 history
			Map<String,String> title2Pairs =jedis.hgetAll(title2+"_h");
			HashSet<String> users2= new HashSet<String>();
			users2.addAll(title2Pairs.values());
			
			// create intersection
			HashSet<String> intersection = new HashSet<String>(users1); // use the copy constructor
			intersection.retainAll(users2);
			
			// calculate similarity
			sim=(intersection.size())/(users1.size()+users2.size()-intersection.size());
			
			jedis.close();

		}catch(Exception e){
			e.printStackTrace();
			return -1;
		}
		System.out.println("Retrive Similarity of the movies "+title1+" "+title2);
		return sim;
	}

	static JedisPool jedisPool = null;
	public synchronized Jedis getJedisConnection() {	
		try {
			if (jedisPool == null) {
				jedisPool = new JedisPool("132.72.65.45");

			}
			return jedisPool.getResource();
		} catch (JedisConnectionException e) {
			System.out.println(e.getStackTrace());
			return null;
		}
	}

}
