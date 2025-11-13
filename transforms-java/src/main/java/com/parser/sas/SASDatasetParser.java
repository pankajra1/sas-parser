package com.novartis.data42.parser.sas;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.palantir.spark.binarystream.data.PortableFile;

public interface SASDatasetParser {
    public Dataset<Row> parse(Dataset<PortableFile> files);
}