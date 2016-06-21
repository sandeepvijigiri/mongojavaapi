package main.serviceimpl;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import main.service.MongoUtilService;

@Service
public class MongoUtilImpl implements MongoUtilService {

	 public DB getMongoDB(){
	    	
    	 MongoClient mongoClient;
    	 DB db=null;
 		try {
 			 mongoClient = new MongoClient("candidate.19.mongolayer.com" , 10929 );
 			
 			//mongodb://eagreedev1:eagreedev1@candidate.19.mongolayer.com:10929/dev1
 	         // Now connect to your databases
 	          db = mongoClient.getDB("dev1");
 	         
 	       //  System.out.println("Connect to database successfully");
 	         boolean auth = db.authenticate("eagreedev1", "eagreedev1".toCharArray());
 	      //   System.out.println("Authentication: "+auth);
 	        
 			
 		} catch (UnknownHostException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}
 		
 		
 		return db;
 			
    	
    }
    
	 @Override 
    public BasicDBObject whereCond(String where_fld,String where_optr,String where_val){
    	BasicDBObject match = new BasicDBObject();
    	BasicDBList and = new BasicDBList();
    	String[] arrWhereFlds=where_fld.split("_E_");
    	String[] arrWhereOptr=where_optr.split("_E_");
    	String[] arrWhereVals=where_val.split("_E_");
    	for(int i=0;i<arrWhereFlds.length;i++){
    		//  Expression Contains OR Condition
    		if(arrWhereVals[i].contains("_OR_")){
    			BasicDBList or = new BasicDBList();
    			String[] arrOr = arrWhereVals[i].split("_OR_");
    			for(int k=0;k<arrOr.length;k++){
    				  if(arrWhereOptr[i].equalsIgnoreCase("like")){
    					     or.add(new BasicDBObject(arrWhereFlds[i].toString(),new BasicDBObject("$regex",".*"+ arrOr[k] +".*")));
    			    	}else if(arrWhereOptr[i].equalsIgnoreCase("unlike")){
    			    		or.add(new BasicDBObject(arrWhereFlds[i].toString(),new BasicDBObject("$not",new BasicDBObject("$regex",".*"+ arrOr[k] +".*"))));
    			    	}
    				    else{
    			    		 or.add(new BasicDBObject(arrWhereFlds[i].toString(),new BasicDBObject(arrWhereOptr[i],arrOr[k])));
      			    	}
    			}
    			DBObject orCond = new BasicDBObject("$or", or);
    			//and.add(new BasicDBObject("$match",orCond));
    			and.add(orCond);
    			   			
    		}else{
	    			 // and.add( new BasicDBObject("$match",new BasicDBObject(arrWhereFlds[i].toString(),new BasicDBObject(arrWhereOptr[i],arrWhereVals[i]))));
	    			if(arrWhereOptr[i].equalsIgnoreCase("like")){
	    				and.add(new BasicDBObject(arrWhereFlds[i].toString(),new BasicDBObject("$regex",".*"+arrWhereVals[i]+".*")));
	    			}else if(arrWhereOptr[i].equalsIgnoreCase("unlike")){
	    				and.add(new BasicDBObject(arrWhereFlds[i].toString(),new BasicDBObject("$not",new BasicDBObject("$regex",".*"+arrWhereVals[i]+".*"))));
	    			}else{
	    				and.add(new BasicDBObject(arrWhereFlds[i].toString(),new BasicDBObject(arrWhereOptr[i],arrWhereVals[i])));
	    			}
    			
    			}
         	}
    	match.append("$and", and);
    	return match; 
      }
	 
    @Override
    public AggregationOutput aggData(String accountId,String aggreements,String secondCategory,String thirdCategory,BasicDBObject whereCond,String[] arrGroupFlds){
    	
    	 DB db = getMongoDB();
       
         DBCollection agmt_con = db.getCollection(aggreements);
               
         Map<String, Object> dbObjIdMap = new HashMap<String, Object>();
         for(int g=0;g<arrGroupFlds.length;g++){
        	 dbObjIdMap.put(arrGroupFlds[g], "$"+arrGroupFlds[g]);
         }
         
         DBObject groupFields = new BasicDBObject( "_id", new BasicDBObject(dbObjIdMap));
         groupFields.put("count", new BasicDBObject("$sum", 1));
                 
         DBObject unwind = null;
         DBObject unwind_c = null;
         
         BasicDBObject whereMatch  = new BasicDBObject("$match",whereCond);
         
         BasicDBObject match = new BasicDBObject(
     	        "$match",new BasicDBObject(
     	            "account_id",new ObjectId(accountId)
     	        ));
          
         if(!secondCategory.equals(""))
         	unwind= new BasicDBObject("$unwind", "$" + secondCategory );
                
         if(!thirdCategory.equals(""))
         	unwind_c = new BasicDBObject("$unwind", "$"+thirdCategory);
        
         DBObject group = new BasicDBObject("$group", groupFields);
         AggregationOutput output=null;
         
             
         if(!secondCategory.equals("") && !thirdCategory.equals(""))
           output =  agmt_con.aggregate(match,whereMatch,unwind,unwind_c,group);	
         else if(!secondCategory.equals(""))
           output = agmt_con.aggregate(match,unwind,group);
         else
           output = agmt_con.aggregate(match,group);
      
        return output;
    }
    
    @Override
    public AggregationOutput aggPriview(String accountId,String aggreements,String[] reportFlds,BasicDBObject whereCond){
    	 
    	AggregationOutput output=null;
    	
    	 DB db = getMongoDB();
    	 
    	 
         
         DBCollection agmt_con = db.getCollection(aggreements);
    	
    	  BasicDBObject match = new BasicDBObject(
        	        "$match",new BasicDBObject(
        	        		 "account_id",new ObjectId(accountId)));
    	  
    	  BasicDBObject whereMatch  = new BasicDBObject("$match",whereCond);
    	  
    	  BasicDBObject fields = new BasicDBObject();
    	  for(int k=0;k<reportFlds.length;k++){
    		  fields.put(reportFlds[k], 1); 
    	  }
    	 
    	  DBObject project = new BasicDBObject("$project", fields );
    	  
    	  output = agmt_con.aggregate(match,whereMatch,project);
    	  
    	      	
    	return output;
    }
    
    
    
    
    
    @Override
    public AggregationOutput aggCount(String accountId,String aggreements,String secondCategory,String thirdCategory,BasicDBObject whereCond){
    	
    	DB db = getMongoDB();
    	DBCollection agmt_con = db.getCollection(aggreements);
        DBObject groupFields = new BasicDBObject("_id", null);
        groupFields.put("count", new BasicDBObject("$sum", 1));
       
        DBObject unwind = null;
        DBObject unwind_c = null;
        
        BasicDBObject match = new BasicDBObject(
      	        "$match",new BasicDBObject(
      	        		 "account_id",new ObjectId(accountId)));
      
        
         BasicDBObject whereMatch  = null;
         if(whereCond !=null)
             whereMatch = new BasicDBObject("$match",whereCond);
        
        
        if(!secondCategory.equals(""))
        	unwind= new BasicDBObject("$unwind", "$" + secondCategory );
               
        if(!thirdCategory.equals(""))
        	unwind_c = new BasicDBObject("$unwind", "$"+thirdCategory);
       
        DBObject group = new BasicDBObject("$group", groupFields);
        AggregationOutput output=null;
           
        if(!secondCategory.equals("") && !thirdCategory.equals(""))
          output =  agmt_con.aggregate(match,whereMatch,unwind,unwind_c,group);	
        else if(!secondCategory.equals(""))
          output = agmt_con.aggregate(match,unwind,group);
        else
          output = agmt_con.aggregate(match,whereMatch,group);
        
       return output;
    }
    
	
	

	
}
