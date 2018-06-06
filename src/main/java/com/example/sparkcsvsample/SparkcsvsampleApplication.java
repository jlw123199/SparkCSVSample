package com.example.sparkcsvsample;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@SpringBootApplication
public class SparkcsvsampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(SparkcsvsampleApplication.class, args);
	}


	@RequestMapping(path = "/spark")
	public ResponseEntity<String> GetS()
	{
		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
				.config("spark.master", "local").config("spark.sql.warehouse.dir", "file:///C:\\")
				.getOrCreate();



		Dataset<Row> df = spark.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "true").load("d:/ab.csv");

		String result = "=========" + df.count();
		df.show();
		return new ResponseEntity<>( result, HttpStatus.OK);
	}

}
