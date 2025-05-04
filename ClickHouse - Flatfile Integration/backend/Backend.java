// Let's start with the project structure and essential files

// pom.xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>2.7.10</version>
        <relativePath/> <!-- lookup parent from repository -->
    </parent>
    <groupId>com.example</groupId>
    <artifactId>clickhouse-integration</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>clickhouse-integration</name>
    <description>ClickHouse and Flat File Data Ingestion Tool</description>
    <properties>
        <java.version>11</java.version>
        <clickhouse.jdbc.version>0.4.6</clickhouse.jdbc.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>
        <dependency>
            <groupId>ru.yandex.clickhouse</groupId>
            <artifactId>clickhouse-jdbc</artifactId>
            <version>${clickhouse.jdbc.version}</version>
        </dependency>
        <dependency>
            <groupId>com.opencsv</groupId>
            <artifactId>opencsv</artifactId>
            <version>5.7.1</version>
        </dependency>
        <dependency>
            <groupId>io.jsonwebtoken</groupId>
            <artifactId>jjwt-api</artifactId>
            <version>0.11.5</version>
        </dependency>
        <dependency>
            <groupId>io.jsonwebtoken</groupId>
            <artifactId>jjwt-impl</artifactId>
            <version>0.11.5</version>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>io.jsonwebtoken</groupId>
            <artifactId>jjwt-jackson</artifactId>
            <version>0.11.5</version>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                        </exclude>
                    </excludes>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>

// Main Application Class
package com.example.clickhouseintegration;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.util.unit.DataSize;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.servlet.MultipartConfigElement;

@SpringBootApplication
public class ClickHouseIntegrationApplication {

    public static void main(String[] args) {
        SpringApplication.run(ClickHouseIntegrationApplication.class, args);
    }

    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/**")
                        .allowedOrigins("*")
                        .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                        .allowedHeaders("*");
            }
        };
    }

    @Bean
    public MultipartConfigElement multipartConfigElement() {
        MultipartConfigFactory factory = new MultipartConfigFactory();
        factory.setMaxFileSize(DataSize.ofMegabytes(100));
        factory.setMaxRequestSize(DataSize.ofMegabytes(100));
        return factory.createMultipartConfig();
    }
}

// Configuration
package com.example.clickhouseintegration.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class AsyncConfig {

    @Bean(name = "taskExecutor")
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(4);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("ClickHouse-");
        executor.initialize();
        return executor;
    }
}

// Models/DTOs
package com.example.clickhouseintegration.model;

import lombok.Data;

@Data
public class ClickHouseConnectionDTO {
    private String host;
    private Integer port;
    private String database;
    private String user;
    private String jwtToken;
}

package com.example.clickhouseintegration.model;

import lombok.Data;

import java.util.List;

@Data
public class TableInfo {
    private String tableName;
    private List<ColumnInfo> columns;
}

package com.example.clickhouseintegration.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ColumnInfo {
    private String columnName;
    private String dataType;
}

package com.example.clickhouseintegration.model;

import lombok.Data;

import java.util.List;

@Data
public class IngestionRequest {
    private ClickHouseConnectionDTO clickHouseConnection;
    private String direction; // "ClickHouseToFlatFile" or "FlatFileToClickHouse"
    private String tableName;
    private List<String> selectedColumns;
    private String outputFilename;
    private String delimiter;
    private JoinInfo joinInfo;
}

package com.example.clickhouseintegration.model;

import lombok.Data;

import java.util.List;

@Data
public class JoinInfo {
    private boolean enabled;
    private List<String> additionalTables;
    private String joinCondition;
    private String joinType; // INNER, LEFT, RIGHT, FULL
}

package com.example.clickhouseintegration.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class IngestionResult {
    private boolean success;
    private String message;
    private long recordCount;
    private String errorDetails;
}

package com.example.clickhouseintegration.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class DataPreview {
    private List<String> columns;
    private List<Map<String, Object>> data;
}

// Services
package com.example.clickhouseintegration.service;

import com.example.clickhouseintegration.model.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ClickHouseService {

    private static final int BATCH_SIZE = 10000;
    private static final int PREVIEW_LIMIT = 100;

    public Connection getConnection(ClickHouseConnectionDTO connectionDTO) throws SQLException {
        String url;
        Properties properties = new Properties();
        
        // Handle JWT token authentication
        if (connectionDTO.getPort() == 8443 || connectionDTO.getPort() == 9440) {
            url = String.format("jdbc:clickhouse://%s:%d/%s?ssl=true", 
                connectionDTO.getHost(), connectionDTO.getPort(), connectionDTO.getDatabase());
        } else {
            url = String.format("jdbc:clickhouse://%s:%d/%s", 
                connectionDTO.getHost(), connectionDTO.getPort(), connectionDTO.getDatabase());
        }
        
        properties.setProperty("user", connectionDTO.getUser());
        
        // Use JWT token if provided
        if (connectionDTO.getJwtToken() != null && !connectionDTO.getJwtToken().isEmpty()) {
            properties.setProperty("password", connectionDTO.getJwtToken());
            properties.setProperty("custom_http_headers", "Authorization: Bearer " + connectionDTO.getJwtToken());
        }
        
        return DriverManager.getConnection(url, properties);
    }

    public List<String> getTables(ClickHouseConnectionDTO connectionDTO) throws SQLException {
        List<String> tables = new ArrayList<>();
        
        try (Connection connection = getConnection(connectionDTO);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW TABLES")) {
            
            while (resultSet.next()) {
                tables.add(resultSet.getString(1));
            }
        }
        
        return tables;
    }

    public TableInfo getTableSchema(ClickHouseConnectionDTO connectionDTO, String tableName) throws SQLException {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableName(tableName);
        List<ColumnInfo> columns = new ArrayList<>();
        
        try (Connection connection = getConnection(connectionDTO);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("DESCRIBE TABLE " + tableName)) {
            
            while (resultSet.next()) {
                String columnName = resultSet.getString("name");
                String dataType = resultSet.getString("type");
                columns.add(new ColumnInfo(columnName, dataType));
            }
        }
        
        tableInfo.setColumns(columns);
        return tableInfo;
    }

    public DataPreview previewClickHouseData(ClickHouseConnectionDTO connectionDTO, String tableName, 
                                            List<String> selectedColumns, JoinInfo joinInfo) throws SQLException {
        
        List<Map<String, Object>> dataRows = new ArrayList<>();
        
        try (Connection connection = getConnection(connectionDTO)) {
            String columnsClause = String.join(", ", selectedColumns);
            
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append("SELECT ").append(columnsClause).append(" FROM ").append(tableName);
            
            // Handle JOIN if enabled
            if (joinInfo != null && joinInfo.isEnabled() && joinInfo.getAdditionalTables() != null 
                    && !joinInfo.getAdditionalTables().isEmpty()) {
                for (String additionalTable : joinInfo.getAdditionalTables()) {
                    queryBuilder.append(" ").append(joinInfo.getJoinType()).append(" JOIN ")
                              .append(additionalTable);
                }
                
                if (joinInfo.getJoinCondition() != null && !joinInfo.getJoinCondition().isEmpty()) {
                    queryBuilder.append(" ON ").append(joinInfo.getJoinCondition());
                }
            }
            
            queryBuilder.append(" LIMIT ").append(PREVIEW_LIMIT);
            
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(queryBuilder.toString())) {
                
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                while (resultSet.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object value = resultSet.getObject(i);
                        row.put(columnName, value);
                    }
                    dataRows.add(row);
                }
            }
        }
        
        return new DataPreview(selectedColumns, dataRows);
    }

    @Async("taskExecutor")
    public CompletableFuture<IngestionResult> clickHouseToFlatFile(IngestionRequest request) {
        long recordCount = 0;
        
        try (Connection connection = getConnection(request.getClickHouseConnection());
             FileWriter fileWriter = new FileWriter(request.getOutputFilename());
             CSVWriter csvWriter = new CSVWriter(fileWriter, 
                    request.getDelimiter().charAt(0), 
                    CSVWriter.DEFAULT_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END)) {
            
            // Write header
            String[] header = request.getSelectedColumns().toArray(new String[0]);
            csvWriter.writeNext(header);
            
            // Build query
            StringBuilder queryBuilder = new StringBuilder();
            String columns = String.join(", ", request.getSelectedColumns());
            queryBuilder.append("SELECT ").append(columns).append(" FROM ").append(request.getTableName());
            
            // Handle JOIN if enabled
            JoinInfo joinInfo = request.getJoinInfo();
            if (joinInfo != null && joinInfo.isEnabled() && joinInfo.getAdditionalTables() != null 
                    && !joinInfo.getAdditionalTables().isEmpty()) {
                for (String additionalTable : joinInfo.getAdditionalTables()) {
                    queryBuilder.append(" ").append(joinInfo.getJoinType()).append(" JOIN ")
                              .append(additionalTable);
                }
                
                if (joinInfo.getJoinCondition() != null && !joinInfo.getJoinCondition().isEmpty()) {
                    queryBuilder.append(" ON ").append(joinInfo.getJoinCondition());
                }
            }
            
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(queryBuilder.toString())) {
                
                while (resultSet.next()) {
                    String[] row = new String[request.getSelectedColumns().size()];
                    for (int i = 0; i < request.getSelectedColumns().size(); i++) {
                        Object value = resultSet.getObject(i + 1);
                        row[i] = value != null ? value.toString() : "";
                    }
                    csvWriter.writeNext(row);
                    recordCount++;
                    
                    if (recordCount % BATCH_SIZE == 0) {
                        csvWriter.flush();
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error during ClickHouse to Flat File ingestion", e);
            return CompletableFuture.completedFuture(new IngestionResult(false, 
                "Error during ingestion", 0, e.getMessage()));
        }
        
        return CompletableFuture.completedFuture(new IngestionResult(true, 
            "Data successfully exported to " + request.getOutputFilename(), recordCount, null));
    }

    @Async("taskExecutor")
    public CompletableFuture<IngestionResult> flatFileToClickHouse(MultipartFile file, IngestionRequest request) {
        long recordCount = 0;
        
        try (Connection connection = getConnection(request.getClickHouseConnection());
             InputStreamReader reader = new InputStreamReader(file.getInputStream());
             CSVReader csvReader = new CSVReader(reader)) {
            
            // Read header and validate selected columns
            String[] header = csvReader.readNext();
            if (header == null) {
                return CompletableFuture.completedFuture(new IngestionResult(false, 
                    "Empty file", 0, "The uploaded file has no content"));
            }
            
            List<String> fileColumns = Arrays.asList(header);
            List<Integer> columnIndices = new ArrayList<>();
            
            for (String selectedColumn : request.getSelectedColumns()) {
                int index = fileColumns.indexOf(selectedColumn);
                if (index == -1) {
                    return CompletableFuture.completedFuture(new IngestionResult(false, 
                        "Column not found", 0, "Column " + selectedColumn + " not found in file"));
                }
                columnIndices.add(index);
            }
            
            // Create table if not exists
            StringBuilder createTableQuery = new StringBuilder();
            createTableQuery.append("CREATE TABLE IF NOT EXISTS ")
                          .append(request.getTableName())
                          .append(" (");
            
            for (int i = 0; i < request.getSelectedColumns().size(); i++) {
                if (i > 0) {
                    createTableQuery.append(", ");
                }
                createTableQuery.append(request.getSelectedColumns().get(i))
                              .append(" String");
            }
            
            createTableQuery.append(") ENGINE = MergeTree() ORDER BY tuple()");
            
            try (Statement statement = connection.createStatement()) {
                statement.execute(createTableQuery.toString());
            }
            
            // Prepare insert statement
            StringBuilder insertQuery = new StringBuilder();
            insertQuery.append("INSERT INTO ")
                     .append(request.getTableName())
                     .append(" (")
                     .append(String.join(", ", request.getSelectedColumns()))
                     .append(") VALUES (");
            
            for (int i = 0; i < request.getSelectedColumns().size(); i++) {
                if (i > 0) {
                    insertQuery.append(", ");
                }
                insertQuery.append("?");
            }
            insertQuery.append(")");
            
            // Execute batch inserts
            try (PreparedStatement preparedStatement = 
                    connection.prepareStatement(insertQuery.toString())) {
                
                String[] nextLine;
                int batchCount = 0;
                
                while ((nextLine = csvReader.readNext()) != null) {
                    for (int i = 0; i < columnIndices.size(); i++) {
                        int columnIndex = columnIndices.get(i);
                        String value = columnIndex < nextLine.length ? nextLine[columnIndex] : "";
                        preparedStatement.setString(i + 1, value);
                    }
                    
                    preparedStatement.addBatch();
                    batchCount++;
                    recordCount++;
                    
                    if (batchCount >= BATCH_SIZE) {
                        preparedStatement.executeBatch();
                        batchCount = 0;
                    }
                }
                
                if (batchCount > 0) {
                    preparedStatement.executeBatch();
                }
            }
        } catch (Exception e) {
            log.error("Error during Flat File to ClickHouse ingestion", e);
            return CompletableFuture.completedFuture(new IngestionResult(false, 
                "Error during ingestion", 0, e.getMessage()));
        }
        
        return CompletableFuture.completedFuture(new IngestionResult(true, 
            "Data successfully imported to ClickHouse table " + request.getTableName(), 
            recordCount, null));
    }

    public List<Map<String, String>> parseCSVHeader(MultipartFile file, String delimiter) throws IOException {
        List<Map<String, String>> columns = new ArrayList<>();
        
        try (InputStreamReader reader = new InputStreamReader(file.getInputStream());
             CSVReader csvReader = new CSVReader(reader)) {
            
            String[] header = csvReader.readNext();
            if (header != null) {
                for (String columnName : header) {
                    Map<String, String> column = new HashMap<>();
                    column.put("columnName", columnName.trim());
                    column.put("dataType", "String");  // Default type for CSV
                    columns.add(column);
                }
            }
        }
        
        return columns;
    }

    public DataPreview previewCSVData(MultipartFile file, String delimiter, List<String> selectedColumns) 
            throws IOException {
        List<Map<String, Object>> dataRows = new ArrayList<>();
        
        try (InputStreamReader reader = new InputStreamReader(file.getInputStream());
             CSVReader csvReader = new CSVReader(reader)) {
            
            String[] header = csvReader.readNext();
            if (header == null) {
                return new DataPreview(Collections.emptyList(), Collections.emptyList());
            }
            
            List<String> fileColumns = Arrays.stream(header)
                                           .map(String::trim)
                                           .collect(Collectors.toList());
            
            List<Integer> columnIndices = new ArrayList<>();
            for (String selectedColumn : selectedColumns) {
                int index = fileColumns.indexOf(selectedColumn);
                if (index != -1) {
                    columnIndices.add(index);
                }
            }
            
            String[] nextLine;
            int rowCount = 0;
            
            while ((nextLine = csvReader.readNext()) != null && rowCount < PREVIEW_LIMIT) {
                Map<String, Object> row = new HashMap<>();
                
                for (int i = 0; i < columnIndices.size(); i++) {
                    int columnIndex = columnIndices.get(i);
                    if (columnIndex < nextLine.length) {
                        row.put(selectedColumns.get(i), nextLine[columnIndex]);
                    } else {
                        row.put(selectedColumns.get(i), "");
                    }
                }
                
                dataRows.add(row);
                rowCount++;
            }
        }
        
        return new DataPreview(selectedColumns, dataRows);
    }
}

// Controllers
package com.example.clickhouseintegration.controller;

import com.example.clickhouseintegration.model.*;
import com.example.clickhouseintegration.service.ClickHouseService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
public class ClickHouseController {

    private final ClickHouseService clickHouseService;

    @PostMapping("/clickhouse/connect")
    public ResponseEntity<?> testConnection(@RequestBody ClickHouseConnectionDTO connectionDTO) {
        try {
            clickHouseService.getConnection(connectionDTO).close();
            return ResponseEntity.ok(Map.of("success", true, "message", "Connection successful"));
        } catch (SQLException e) {
            log.error("Connection error", e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                               .body(Map.of("success", false, "message", "Connection failed", 
                                           "error", e.getMessage()));
        }
    }

    @PostMapping("/clickhouse/tables")
    public ResponseEntity<?> getTables(@RequestBody ClickHouseConnectionDTO connectionDTO) {
        try {
            List<String> tables = clickHouseService.getTables(connectionDTO);
            return ResponseEntity.ok(tables);
        } catch (SQLException e) {
            log.error("Error getting tables", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                               .body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/clickhouse/schema")
    public ResponseEntity<?> getTableSchema(@RequestBody Map<String, Object> request) {
        try {
            ClickHouseConnectionDTO connectionDTO = new ClickHouseConnectionDTO();
            connectionDTO.setHost((String) request.get("host"));
            connectionDTO.setPort((Integer) request.get("port"));
            connectionDTO.setDatabase((String) request.get("database"));
            connectionDTO.setUser((String) request.get("user"));
            connectionDTO.setJwtToken((String) request.get("jwtToken"));
            
            String tableName = (String) request.get("tableName");
            
            TableInfo tableInfo = clickHouseService.getTableSchema(connectionDTO, tableName);
            return ResponseEntity.ok(tableInfo);
        } catch (SQLException e) {
            log.error("Error getting table schema", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                               .body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/clickhouse/preview")
    public ResponseEntity<?> previewClickHouseData(@RequestBody IngestionRequest request) {
        try {
            DataPreview preview = clickHouseService.previewClickHouseData(
                request.getClickHouseConnection(),
                request.getTableName(),
                request.getSelectedColumns(),
                request.getJoinInfo()
            );
            return ResponseEntity.ok(preview);
        } catch (SQLException e) {
            log.error("Error previewing ClickHouse data", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                               .body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/flatfile/parse")
    public ResponseEntity<?> parseCSVHeader(
            @RequestParam("file") MultipartFile file,
            @RequestParam("delimiter") String delimiter) {
        try {
            List<Map<String, String>> columns = clickHouseService.parseCSVHeader(file, delimiter);
            return ResponseEntity.ok(columns);
        } catch (IOException e) {
            log.error("Error parsing CSV header", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                               .body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/flatfile/preview")
    public ResponseEntity<?> previewCSVData(
            @RequestParam("file") MultipartFile file,
            @RequestParam("delimiter") String delimiter,
            @RequestParam("columns") List<String> selectedColumns) {
        try {
            DataPreview preview = clickHouseService.previewCSVData(file, delimiter, selectedColumns);
            return ResponseEntity.ok(preview);
        } catch (IOException e) {
            log.error("Error previewing CSV data", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                               .body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/ingestion/clickhouse-to-flatfile")
    public ResponseEntity<?> clickHouseToFlatFile(@RequestBody IngestionRequest request) {
        try {
            CompletableFuture<IngestionResult> future = clickHouseService.clickHouseToFlatFile(request);
            return ResponseEntity.ok(Map.of("message", "Ingestion started", "jobId", future.hashCode()));
        } catch (Exception e) {
            log.error("Error starting ClickHouse to Flat File ingestion", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                               .body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/ingestion/flatfile-to-clickhouse")
    public ResponseEntity<?> flatFileToClickHouse(
            @RequestParam("file") MultipartFile file,
            @RequestParam("request") String requestJson) {
        try {
            // In a real app, you would parse the requestJson to IngestionRequest using ObjectMapper
            // For simplicity, we'll just return a placeholder
            return ResponseEntity.ok(Map.of("message", "This is a placeholder. You would implement JSON parsing here."));
        } catch (Exception e) {
            log.error("Error starting Flat File to ClickHouse ingestion", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                               .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/ingestion/status/{jobId}")
    public ResponseEntity<?> getIngestionStatus(@PathVariable int jobId) {
        // In a real app, you would track the jobs by ID
        // For now, we'll return a placeholder
        return ResponseEntity.ok(Map.of(
            "jobId", jobId,
            "status", "IN_PROGRESS",
            "progress", 50,
            "message", "Processing records..."
        ));
    }
}

// application.properties
server.port=8080

# Temporary file storage location
spring.servlet.multipart.location=${java.io.tmpdir}

# Logging
logging.level.root=info
logging.level.com.example.clickhouseintegration=debug

# Max file upload size
spring.servlet.multipart.max-file-size=100MB
spring.servlet.multipart.max-request-size=100MB