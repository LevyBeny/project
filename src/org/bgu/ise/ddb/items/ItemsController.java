/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.IOException;



import java.sql.Connection;
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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;



/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {
	
	
	
	/**
	 * The function copy all the items(title and production year) from the Oracle table MediaItems to the System storage.
	 * The Oracle table and data should be used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method={RequestMethod.GET})
	public void fillMediaItems(HttpServletResponse response){
		System.out.println("was here");
		try{
			Jedis jedis= getJedisConnection();
			//registration of the driver
			Class.forName("oracle.jdbc.driver.OracleDriver");
			String connectionURL= "jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/ORACLE ";
            Connection conn= DriverManager.getConnection(connectionURL,"kulikr","abcd");
        	String query = "SELECT * from MEDIAITEMS";
    		PreparedStatement ps = conn.prepareStatement(query);
    		ResultSet rs = ps.executeQuery();
    	    Pipeline pp= jedis.pipelined();
    		while(rs.next())
    		{
    			// For every item in mediaItems save hash {title; year}
    			String title = rs.getString("TITLE");
    			String year =String.valueOf(rs.getInt("PROD_YEAR"));
    			String mid = String.valueOf(rs.getInt("MID"));
    			
    			pp.sadd("set_items", mid);
    			pp.hset(mid, "title", title);
    			pp.hset(mid, "year", year);
    			pp.sync();
    				
    		}
    		
    		rs.close();
    		ps.close();
    		jedis.close();
    		HttpStatus status = HttpStatus.OK;
    		response.setStatus(status.value());
    		
     		
    		
		}
		catch (ClassNotFoundException | SQLException e){
			HttpStatus status = HttpStatus.CONFLICT;
			response.setStatus(status.value());
			e.printStackTrace();
		}
	}
	
	

	/**
	 * The function copy all the items from the remote file,
	 * the remote file have the same structure as the films file from the previous assignment.
	 * You can assume that the address protocol is http
	 * @throws IOException 
	 */
	@RequestMapping(value = "fill_media_items_from_url", method={RequestMethod.GET})
	public void fillMediaItemsFromUrl(@RequestParam("url")    String urladdress,
			HttpServletResponse response) throws IOException{
		System.out.println(urladdress);
		
		//:TODO your implementation
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	
	
	/**
	 * The function retrieves from the system storage N items,
	 * order is not important( any N items) 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public  MediaItems[] getTopNItems(@RequestParam("topn")    int topN){
		//:TODO your implementation
		MediaItems m = new MediaItems("Game of Thrones", 2011);
		System.out.println(m);
		return new MediaItems[]{m};
	}
	
	//A helping function that creates a synchronized pool connection to Jedis,and returns it. 
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
