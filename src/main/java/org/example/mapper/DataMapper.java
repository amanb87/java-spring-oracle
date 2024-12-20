package org.example.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.example.model.DataRecord;

@Mapper
public interface DataMapper {
    void insertRecord(DataRecord record);
}