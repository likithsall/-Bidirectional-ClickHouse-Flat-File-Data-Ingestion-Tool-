# -Bidirectional-ClickHouse-Flat-File-Data-Ingestion-Tool-

clickhouse-integration/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/
│   │   │       └── example/
│   │   │           └── clickhouseintegration/
│   │   │               ├── ClickHouseIntegrationApplication.java
│   │   │               ├── config/
│   │   │               │   └── AsyncConfig.java
│   │   │               ├── controller/
│   │   │               │   └── ClickHouseController.java
│   │   │               ├── model/
│   │   │               │   ├── ClickHouseConnectionDTO.java
│   │   │               │   ├── ColumnInfo.java
│   │   │               │   ├── DataPreview.java
│   │   │               │   ├── IngestionRequest.java
│   │   │               │   ├── IngestionResult.java
│   │   │               │   ├── JoinInfo.java
│   │   │               │   └── TableInfo.java
│   │   │               └── service/
│   │   │                   └── ClickHouseService.java
│   │   └── resources/
│   │       ├── application.properties
│   │       └── static/
│   │           └── index.html
│   └── test/
│       └── java/
│           └── com/
│               └── example/
│                   └── clickhouseintegration/
│                       └── ClickHouseIntegrationApplicationTests.java
└── pom.xml
