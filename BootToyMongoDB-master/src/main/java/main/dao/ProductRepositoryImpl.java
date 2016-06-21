package main.dao;




import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.UnwindOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;


import static org.springframework.data.mongodb.core.aggregation.Aggregation.group;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class ProductRepositoryImpl {
   

    public List<String> findBySkuOnlyAvailablesCustom(String name) {
       /* Criteria criteria = Criteria.where("containerUniqueId").is(name);
        return mongoTemplate.find(Query.query(criteria), Product.class);*/
    	
    	/* Aggregation aggregation = newAggregation(
    		       group("draftingStatus").count().as("total").last("draftingStatus").as("accountId")    
    		     );
    	 		     AggregationResults groupResults = mongoTemplate.aggregate(
    		       aggregation, AgreementVersions.class, TopLevelCountReport.class);
    		     
    		     List<TopLevelCountReport> salesReport = groupResults.getMappedResults();
    		     System.out.println("Sales Report==="+salesReport);*/
    			 List<String> salesReport = null;
    			 
    			 BasicDBObject whereCond=whereCond("agreement_versions.clause.top_level_category_E_agreement_versions.clause.top_level_category_E_agreement_versions.clause.top_level_category", "Like_E_Like_E_Like", "Covenants_OR_Term and Termination_E_C_E_Non-disclosure Agreement_OR_A_OR_B");
    			// System.out.println("where cond===="+whereCond);
    		     mongoChk("","","");
    		//	 System.out.println("Agreements Count  =: " + aggCount("5756c2c346fce8741e000041","agreement_containers","","",whereCond));
    		//	 System.out.println("Agreement Content =: " + aggCount("5756c2c346fce8741e000041","agreement_containers","agreement_versions","",whereCond));
    		//	 System.out.println("Agreement Top Level  =: " + aggCount("5756c2c346fce8741e000041","agreement_containers","agreement_versions", "agreement_versions.clause",whereCond));
    		//	 System.out.println("Agreement Second Level  =: " + aggData("5756c2c346fce8741e000041","agreement_containers","agreement_versions", "agreement_versions.clause",whereCond,"agreement_versions.clause.top_level_category_G_agreement_versions.clause.second_level_category".split("_G_")));
    		//	 System.out.println("Companies Count =: " + aggCount("5756c2c346fce8741e000041","companies","", "",whereCond) );  
    		//	 System.out.println("Agreement Priview=: "+ aggPriview("5756c2c346fce8741e000041", "agreement_containers","container_unique_id_reference_to_master_unique_id,agreement_versions.clause.top_level_category".split(",") , whereCond));
    			 return salesReport;		     
    	
    	
    }
    
    
    
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
    
    
    
    
    public String mongoChk(String where_content,String where_optr,String where_cond){
    	
    	
		try {
			
			
			//mongodb://eagreedev1:eagreedev1@candidate.19.mongolayer.com:10929/dev1
	         // Now connect to your databases
	         DB db = getMongoDB();
	         DBCollection agmt_con = db.getCollection("agreement_containers");
	         System.out.println("agmt_con==="+agmt_con);
	        		 
	         DBObject groupFields = new BasicDBObject( "_id", null);
	         
	         groupFields.put("count", new BasicDBObject( "$sum", 1));
	         System.out.println("group fields===="+groupFields);
	         
	         
	        /* Object match = new BasicDBObject("$match", 
	        		   new BasicDBObject("score",
	        		   new BasicDBObject("$gt", 70).append("$lte", 90) ) )
	         */
	         
	         DBObject clause1 = new BasicDBObject("account_id",new ObjectId("5756c2c346fce8741e000041"));
	         DBObject clause2 = new BasicDBObject("account_id",new ObjectId("5756c2c146fce8741e000017"));
	         BasicDBList or = new BasicDBList();
	         or.add(clause1);
	         or.add(clause2);
	         DBObject query = new BasicDBObject("$or", or);
	         
	         
	         DBObject unwind = new BasicDBObject("$unwind", "$agreement_versions");
	         DBObject unwind_c = new BasicDBObject("$unwind", "$agreement_versions.clause");
	        
	         DBObject group = new BasicDBObject("$group", groupFields);
	         
	     //    DBObject match = new BasicDBObject("$match", new BasicDBObject("container_unique_id_reference_to_master_unique_id", "//^1465//i") );
	         
	        /* BasicDBObject matchKeyWord = new BasicDBObject(
	        	        "$match",new BasicDBObject(
	        	            "container_unique_id_reference_to_master_unique_id",new BasicDBObject("$regex",".*1465.*")
	        	        )
	        	    );
	         */
	             BasicDBObject matchObj = new BasicDBObject();
	                
	         
	          matchObj.put("$match",new BasicDBObject(
	        	            "account_id",new ObjectId("5756c2c346fce8741e000041")
	        	        ));
	         
	         
	      /*    matchObj.put("$match",new BasicDBObject(
	        	            "container_unique_id_reference_to_master_unique_id",new BasicDBObject("$lt","9005")
	        	        ));*/
	          System.out.println("280  match obj==="+matchObj);
	          
	          
	         /* BasicDBObject unwindObj = new BasicDBObject();
	          
	          unwindObj.put("$unwind", "$agreement_versions");
	          	          
	          unwindObj.put("$unwind", "$agreement_versions.clause");*/
	          
	         
	        // DBObject match = new BasicDBObject("$regex", new BasicDBObject("container_unique_id_reference_to_master_unique_id", "/^ABC/i") );
	         
	         
	      //   AggregationOutput output = agmt_con.aggregate(match,matchKeyWord,unwind,unwind_c,group);
	         
	       //   AggregationOutput output = agmt_con.aggregate(matchObj,unwind,unwind_c,group);
	         
	      //    DBObject query = new BasicDBObject("$or", matchObj);
	          
	      /*    DBObject objectToFind = BasicDBObjectBuilder.start()
	                  .add("account_id", "5756c2c146fce8741e000017")
	                  .get();
	          */
	    //     AggregationOutput output = agmt_con.aggregate(query,unwind,unwind_c,group); 
	          
	          
	          List<BasicDBObject> coll = new ArrayList<BasicDBObject>();
				coll.add(new BasicDBObject(
        	            "container_unique_id_reference_to_master_unique_id","9005"
        	 	    ));
				coll.add(new BasicDBObject("container_unique_id_reference_to_master_unique_id","9004"
        	        	    ));
				BasicDBObject query1 = new BasicDBObject("$or", coll);
				System.out.println("query1===="+query1);
				
				BasicDBObject query_tmp = new BasicDBObject("$match", query1);
				
				
				//5756c2c146fce8741e000017
			//	System.out.println("output qry==="+matchObj);
			//	 System.out.println("count===="+agmt_con.find(query1,unwind));
				 
				 BasicDBObject matchKeyWord = new BasicDBObject(
		        	        "$match",new BasicDBObject(
		        	            "container_unique_id_reference_to_master_unique_id",new BasicDBObject("$ne",new BasicDBObject("$regex",".*1465.*"))
		        	        ));
				 
				 BasicDBObject matchKeyWord1 = new BasicDBObject(
		        	        "$match",new BasicDBObject(
		        	            "container_unique_id_reference_to_master_unique_id",new BasicDBObject("$regex",".*900.*")
		        	        ));
				 
				/* coll.clear();
				 coll.add(matchKeyWord);
				 coll.add(matchKeyWord1);
				// System.out.println("count111====="+agmt_con.find(query1,unwind).count());
				 
				 BasicDBObject query2 = new BasicDBObject("$or", coll);*/
				 
			  System.out.println("query_tmp===="+matchKeyWord);	 
	        // AggregationOutput output = agmt_con.aggregate(query_tmp,unwind,unwind_c,group);
	       
		//	  AggregationOutput output = agmt_con.aggregate(matchKeyWord);
			//	System.out.println("output qry==="+query1);  
	        // AggregationOutput output = agmt_con.aggregate(query1,unwind,unwind_c,group); 
	              
	         
	      //  System.out.println("output===="+output);
	         
	         
	      // create our pipeline operations, first with the $match
	       //  DBObject match = new BasicDBObject("$match", new BasicDBObject("type", "airfare") );

	         // build the $projection operation
	       /*  DBObject fields = new BasicDBObject("agreement_versions.clause.top_level_category", 1);
	          fields.put("_id", 0);
	         DBObject project = new BasicDBObject("$project", fields);*/

	         // Now the $group operation
	     /*    DBObject groupFields_2 = new BasicDBObject( "_id", "$agreement_versions.clause.top_level_category");
	         groupFields_2.put("count", new BasicDBObject( "$sum", 1));
	         DBObject group_2 = new BasicDBObject("$group", groupFields_2);*/

	         // run aggregation
	     //    AggregationOutput     output =  agmt_con.aggregate( project, group_2 );
	         
	     //    System.out.println("output2===="+output);
	         
	         
	         
	         
	       //  System.out.println("db collections "+db.getCollection("agreement_containers").aggregate(group("account_id").count() ,)
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
    	
    	return "";
    }
    
    
    


}
