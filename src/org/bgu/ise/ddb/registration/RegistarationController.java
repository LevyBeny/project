/**
 * 
 */
package org.bgu.ise.ddb.registration;

import java.io.IOException;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Date;
import java.util.Set;

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
import redis.clients.jedis.Pipeline;
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

		DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss"); // Use to cast a date to string

		try {
			if (isExistUser(username)){
				HttpStatus status = HttpStatus.CONFLICT;
				response.setStatus(status.value());
			}

			Date now = Calendar.getInstance().getTime();
			String date= df.format(now);

			Jedis jedis= new Jedis("132.72.65.45");//getJedisConnection();
			Pipeline pp = jedis.pipelined();
			pp.sadd("SUsers", username);
			pp.hset(username, "password", password);
			pp.hset(username, "firstName", firstName);
			pp.hset(username, "lastName", lastName);
			pp.hset(username, "regDate", date);
			pp.sync();
			
			System.out.println("Registered User: "+username+" "+firstName+" "+lastName+" "+date+" ");

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
		boolean result = false;
		try{
			Jedis jedis= new Jedis("132.72.65.45");//getJedisConnection();
			result= jedis.sismember("SUsers",username);
			if (result)
				System.out.println("User Name exist: "+username);
			else
				System.out.println("User Name do not exist: "+username);
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
		try{
			if (isExistUser(username)){
				Jedis jedis= new Jedis("132.72.65.45");//getJedisConnection();
				if (password.equals(jedis.hget(username, "password")))
					result=true;
				jedis.close();
			}
			else
				result = false;
		}
		catch (Exception e){
			e.printStackTrace();
		}
		if (result)
			System.out.println("User validated: "+username);
		else
			System.out.println("User not validated: "+username);
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
		DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss"); // Use to cast a date to string

		try{
			Jedis jedis= new Jedis("132.72.65.45");//getJedisConnection();

			Long time_milsec = System.currentTimeMillis() - 1000*60*60*24*days;
			Date before_days= new Date(time_milsec);

			Set<String> all_users = jedis.smembers("SUsers");
			for (String username : all_users){
				String s_date=jedis.hget(username, "regDate");
				Date reg_date = df.parse(s_date);
				long diff = reg_date.getTime() - before_days.getTime();
				if(diff >= 0 )
					result++;
			}
			jedis.close();
		}
		catch (ParseException e) {
			e.printStackTrace();
		}
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
		User[] result=null;
		try{
			Jedis jedis= new Jedis("132.72.65.45");//getJedisConnection();
			Set<String> all_users = jedis.smembers("SUsers");
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

		return result;
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
