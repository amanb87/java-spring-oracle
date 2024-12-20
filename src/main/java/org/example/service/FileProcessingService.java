package org.example.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.example.mapper.DataMapper;
import org.example.model.DataRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Service
public class FileProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(FileProcessingService.class);

    private static final List<String> EXPECTED_HEADERS = Arrays.asList("field1", "field2");

    @Autowired
    private DataMapper dataMapper;

    public void processAndSaveFile(MultipartFile file) throws Exception {
        // Empty file check
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("File is empty or null");
        }

        // File extension should be csv
        if (!Objects.requireNonNull(file.getOriginalFilename()).toLowerCase().endsWith(".csv")) {
            throw new IllegalArgumentException("File must be a CSV file");
        }

        logger.info("Processing file: {}", file.getOriginalFilename());

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .setIgnoreEmptyLines(false)
                     .build())) {


            validateHeaders(csvParser.getHeaderNames());

            // Check if file has any data rows
            if (!csvParser.iterator().hasNext()) {
                throw new IllegalArgumentException("CSV file is empty (no data rows)");
            }

            for (CSVRecord record : csvParser) {
                try {
                    DataRecord dataRecord = parseCSVRecord(record);
                    dataMapper.insertRecord(dataRecord);
                    logger.info("Successfully inserted record from line {}", record.getRecordNumber());
                } catch (Exception e) {
                    logger.error("Error processing line {}: {}", record.getRecordNumber(), e.getMessage());
                    throw new Exception("Error processing line " + record.getRecordNumber() + ": " + e.getMessage());
                }
            }
        }
    }

    private void validateHeaders(List<String> actualHeaders) {
        logger.info("Validating headers: {}", actualHeaders);

        // Check for exact number of headers
        if (actualHeaders.size() != EXPECTED_HEADERS.size()) {
            throw new IllegalArgumentException(
                    "Invalid number of headers. Expected " + EXPECTED_HEADERS.size() +
                            " headers but found " + actualHeaders.size() +
                            ". Required headers are: " + String.join(", ", EXPECTED_HEADERS));
        }

        // Check if all headers match exactly
        for (int i = 0; i < EXPECTED_HEADERS.size(); i++) {
            String expectedHeader = EXPECTED_HEADERS.get(i);
            String actualHeader = actualHeaders.get(i);

            if (!expectedHeader.equals(actualHeader)) {
                throw new IllegalArgumentException(
                        "Invalid header found: '" + actualHeader +
                                "'. Expected header: '" + expectedHeader +
                                "'. Required headers are: " + String.join(", ", EXPECTED_HEADERS));
            }
        }
    }

    private DataRecord parseCSVRecord(CSVRecord record) {
        // Validate record has all required fields
        if (record.size() < EXPECTED_HEADERS.size()) {
            throw new IllegalArgumentException(
                    "Record at line " + record.getRecordNumber() +
                            " has insufficient columns. Expected: " + EXPECTED_HEADERS.size() +
                            ", Found: " + record.size());
        }

        String field1Value = record.get("field1").trim();
        String field2Value = record.get("field2").trim();

        // Check for empty values before creating the object
        if (field1Value.isEmpty() || field2Value.isEmpty()) {
            String emptyField = field1Value.isEmpty() ? "field1" : "field2";
            throw new IllegalArgumentException(
                    "Empty value found for '" + emptyField + "' at line " + record.getRecordNumber());
        }

        DataRecord dataRecord = new DataRecord();
        dataRecord.setField1(field1Value);
        dataRecord.setField2(field2Value);

        return dataRecord;
    }
}