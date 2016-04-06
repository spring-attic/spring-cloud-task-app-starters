package org.springframework.cloud.task.app.spark.yarn;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkYarnTaskApplication {

	public static void main(String[] args) {
		SpringApplication.run(SparkYarnTaskApplication.class, args);
	}
}
