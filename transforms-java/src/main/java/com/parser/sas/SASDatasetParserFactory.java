package com.novartis.data42.parser.sas;

public class SASDatasetParserFactory {
    public static SASDatasetParser create() {
        return new SJDBCParser();
    }
}