# ClickHouse & Flat File Data Ingestion Tool

This project is a simplified data ingestion tool developed as part of the **Zeotap Software Engineer Intern assignment**. It enables bidirectional data transfer between **ClickHouse databases** and **flat files (CSV/TSV)** using a basic Java backend and HTML frontend.

---

## Features

-  **Bidirectional data flow**:
  - ClickHouse → Flat File
  - Flat File → ClickHouse
-  **JWT-based connection authentication** (placeholder for extension)
-  **Schema discovery** from flat files
-  **Data preview** before ingestion
-  **Batch processing** for efficient large-file transfers
-  **Progress tracking** (via logs or future UI enhancement)

---

Setup Instructions
Prerequisites

Java 11 or higher
Maven 3.6 or higher
ClickHouse server (for actual usage)
Web browser with JavaScript enabled

Backend Setup

Clone the repository to your local machine
Navigate to the project directory:
cd clickhouse-integration

Create the missing test class to satisfy Maven build:
java// src/test/java/com/example/clickhouseintegration/ClickHouseIntegrationApplicationTests.java
package com.example.clickhouseintegration;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ClickHouseIntegrationApplicationTests {

    @Test
    void contextLoads() {
    }
}

Build the project:
mvn clean package

Run the application:
java -jar target/clickhouse-integration-0.0.1-SNAPSHOT.jar


Frontend Setup

Create a directory for the static files:
mkdir -p src/main/resources/static

Save the HTML content to src/main/resources/static/index.html
Once the Spring Boot application is running, the frontend will be accessible at http://localhost:8080

Configuration
application.properties
The application is configured with the following properties by default:
propertiesserver.port=8080

# Temporary file storage location
spring.servlet.multipart.location=${java.io.tmpdir}

# Logging
logging.level.root=info
logging.level.com.example.clickhouseintegration=debug

# Max file upload size
spring.servlet.multipart.max-file-size=100MB
spring.servlet.multipart.max-request-size=100MB
You can modify these settings to match your environment requirements.
API Endpoints
