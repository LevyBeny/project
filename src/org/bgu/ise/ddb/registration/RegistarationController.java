/**
 * 
 */
package org.bgu.ise.ddb.registration;

import java.io.IOException;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;

import javax.servlet.http.HttpServletResponse;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController{


	/**
	 * The function checks if the username exist,
	 * in case of positive answer HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT,
	 * else insert the user to the system  and set to HttpStatus in HttpServletResponse HttpStatus.OK
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method={RequestMethod.POST})
	public void registerNewUser(@RequestParam("username") String username,
			@RequestParam("password")    String password,
			@RequestParam("firstName")   String firstName,
			@RequestParam("lastName")  String lastName,
			HttpServletResponse response){
		System.out.println(username+" "+password+" "+lastName+" "+firstName);

		try {
			if (isExistUser(username)){
				HttpStatus status = HttpStatus.CONFLICT;
				response.setStatus(status.value());
			}
			Jedis jedis=getJedisConnection();
			Pipeline pp = jedis.pipelined();
			pp.sadd("SUsers", username);
			pp.hset(username, "password", password);
			pp.hset(username, "firstName", firstName);
			pp.hset(username, "lastName", lastName);
			pp.sync();
			
			HttpStatus status = HttpStatus.OK;
			response.setStatus(status.value());
			jedis.close();			
		}
		catch (Exception e) {
			HttpStatus status = HttpStatus.CONFLICT;
			response.setStatus(status.value());
			e.printStackTrace();
		}
	}

	/**
	 * The function returns true if the received username exist in the system otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method={RequestMethod.GET})
	public boolean isExistUser(@RequestParam("username") String username) throws IOException{
		System.out.println(username);
		boolean result = false;
		try{
			Jedis jedis= getJedisConnection();
			result= jedis.sismember("SUsers",username);
			jedis.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}		
		return result;	
	}

	/**
	 * The function returns true if the received username and password match a system storage entry, otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method={RequestMethod.POST})
	public boolean validateUser(@RequestParam("username") String username,
			@RequestParam("password")    String password) throws IOException{
		System.out.println(username+" "+password);
		boolean result = false;
		//:TODO your implementation


		return result;

	}

	/**
	 * The function retrieves number of the registered users in the past n days
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method={RequestMethod.GET})
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException{
		System.out.println(days+"");
		int result = 0;
		//:TODO your implementation

		return result;

	}

	/**
	 * The function retrieves all the users
	 * @return
	 */
	@RequestMapping(value = "get_all_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public  User[] getAllUsers(){
		//:TODO your implementation
		User u = new User("alex", "alex", "alex");
		System.out.println(u);
		return new User[]{u};
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
