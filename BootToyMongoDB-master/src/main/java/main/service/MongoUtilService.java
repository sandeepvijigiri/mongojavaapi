package main.service;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;

public interface MongoUtilService {
		
    public AggregationOutput aggCount(String accountId,String aggreements,String secondCategory,String thirdCategory,BasicDBObject whereCond);
	
    public AggregationOutput aggData(String accountId,String aggreements,String secondCategory,String thirdCategory,BasicDBObject whereCond,String[] arrGroupFlds);
    
    public AggregationOutput aggPriview(String accountId,String aggreements,String[] reportFlds,BasicDBObject whereCond);
    
    public BasicDBObject whereCond(String where_fld,String where_optr,String where_val); 
    

}
