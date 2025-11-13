package com.novartis.data42.parser.sas;

import java.util.Iterator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.palantir.spark.binarystream.data.PortableFile;

class SJDBCParser implements SASDatasetParser {
    @Override
    public Dataset<Row> parse(Dataset<PortableFile> files) {
        return files.flatMap(SJDBCParser::parseFile, SJDBCRowIterator.encoder());
    }

    private static Iterator<Row> parseFile(PortableFile portableFile) {
        return new SJDBCRowIterator(portableFile);
    }
}
