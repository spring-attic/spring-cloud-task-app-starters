package org.springframework.cloud.task.app.spark.cluster;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkClusterTaskApplication {

	public static void main(String[] args) {
		SpringApplication.run(SparkClusterTaskApplication.class, args);
	}
}
