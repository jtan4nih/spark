package com.matthewrathbone.sparktest.cassandra;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraConnection {
	private static CassandraConnection cassandraConnection;
	private static Session session;
	
	private CassandraConnection() {}
	
	public static CassandraConnection getInstance() {
		if(cassandraConnection == null) {
			cassandraConnection = new CassandraConnection();
		} 
		return cassandraConnection;
	}
	
	private Session getCassandraSession() {
		if(session == null) {
			System.out.println(">>>>>>>>>> connecting to cassandra");
			String serverIP = "127.0.0.1";
			String keyspace = "amazon_data";
			Cluster cluster = Cluster.builder().addContactPoints(serverIP).build();
			session = cluster.connect(keyspace);
		}
		return session;
	}
	
	public String getProductDetails() {
		String productDetails = "/home/cloudera/Desktop/SPARK_POC/Cassandra_Data/product_details.txt";
		Session session = getCassandraSession();
		String cqlStatement = "SELECT * FROM product_details";
	    StringBuffer produtDetailsSB = new StringBuffer();
	    int count = 0;
	    for (Row row : session.execute(cqlStatement)) {
	        //System.out.println(row.toString());
	    	if(count > 0) {
	    		produtDetailsSB.append("\n");
	    	}
	    	produtDetailsSB.append(row.getInt("product_id"));
	    	produtDetailsSB.append("\t");
	    	produtDetailsSB.append(row.getString("category_name"));
	    	produtDetailsSB.append("\t");
	    	produtDetailsSB.append(row.getInt("price"));
	    	produtDetailsSB.append("\t");
	    	produtDetailsSB.append(row.getString("product_name"));
	    	count++;
	    }
	    createFile(productDetails, produtDetailsSB.toString());
	    return productDetails;
	}
	
	public String getReviewTransactions() {
		String reviewTransactions = "/home/cloudera/Desktop/SPARK_POC/Cassandra_Data/review_transactions.txt";
		Session session = getCassandraSession();
		String cqlStatement = "SELECT * FROM review_transactions";
	    StringBuffer reviewTransactionsSB = new StringBuffer();
	    int count = 0;
	    for (Row row : session.execute(cqlStatement)) {
	    	if(count > 0) {
	    		reviewTransactionsSB.append("\n");
	    	}
	    	reviewTransactionsSB.append(row.getInt("transaction_id"));
	    	reviewTransactionsSB.append("\t");
	    	reviewTransactionsSB.append(row.getInt("product_id"));
	    	reviewTransactionsSB.append("\t");
	    	reviewTransactionsSB.append(row.getInt("rating"));
	    	reviewTransactionsSB.append("\t");
	    	reviewTransactionsSB.append(row.getString("reviewer_id"));
	    	reviewTransactionsSB.append("\t");
	    	reviewTransactionsSB.append(row.getString("trans_time"));
	    	count++;
	    }
	    createFile(reviewTransactions, reviewTransactionsSB.toString());
	    return reviewTransactions;
	}
	
	private void createFile(String filePath, String content) {
		try {

			File file = new File(filePath);

			if (!file.exists() && file.createNewFile()) {
				System.out.println("File is created!");
			} 
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(content);
			bw.close();
			System.out.println("Done");
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
	
	public static void main(String[] args) {
	    CassandraConnection cc = CassandraConnection.getInstance();
	    System.out.println("Product Details : "+cc.getProductDetails());
	    System.out.println("Review Transactions : "+cc.getReviewTransactions());
	}

}
