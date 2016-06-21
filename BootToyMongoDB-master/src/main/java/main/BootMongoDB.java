package main;


import main.service.MongoUtilService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;



@Configuration
@EnableAutoConfiguration
@ComponentScan
public class BootMongoDB implements CommandLineRunner {

        
    @Autowired
    private MongoUtilService mongoUtilService;
   
    private static final Logger logger = LoggerFactory.getLogger(BootMongoDB.class);

    public void run(String... args) throws Exception {
    	BasicDBObject whereCond = new BasicDBObject();
    	AggregationOutput output1=mongoUtilService.aggCount("5756c2c346fce8741e000041","agreement_containers","","",whereCond);
    	
    	//whereCond=mongoUtilService.whereCond("agreement_versions.clause.top_level_category_E_agreement_versions.clause.top_level_category_E_agreement_versions.clause.top_level_category", "Like_E_Like_E_Like", "Covenants_OR_Term and Termination_E_C_E_Non-disclosure Agreement_OR_A_OR_B");
    	whereCond=mongoUtilService.whereCond("agreement_versions.clause.top_level_category", "Like", "Covenants");
    	AggregationOutput output=mongoUtilService.aggData("5756c2c346fce8741e000041","agreement_containers","agreement_versions", "agreement_versions.clause",whereCond,"agreement_versions.clause.top_level_category".split("_G_"));
    	System.out.println("output==="+output);
    	output=mongoUtilService.aggPriview("5756c2c346fce8741e000041", "agreement_containers", "container_unique_id_reference_to_master_unique_id,account_id,agreement_versions.drafting_status".split(","), whereCond);
    	System.out.println("Agg Priview===="+output);
    	logger.info("result of getName is {}",output);
        }

    public static void main(String[] args) throws Exception {
    	System.out.println("Enter in main");
        SpringApplication.run(BootMongoDB.class, args);
    	
    }
}
