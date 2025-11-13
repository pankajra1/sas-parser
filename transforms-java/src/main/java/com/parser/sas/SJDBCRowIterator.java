package com.novartis.data42.parser.sas;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.NoSuchElementException;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dullesopen.jdbc.Driver;
import com.dullesopen.jdbc.DataSetInfo;
import com.palantir.spark.binarystream.data.PortableFile;

class SJDBCRowIterator implements Iterator<Row> {
    private static final Logger logger = LoggerFactory.getLogger(SJDBCRowIterator.class);

    private static final StructType schema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("logical_path",  DataTypes.StringType, true),
            DataTypes.createStructField("dataset_label", DataTypes.StringType, true),
            DataTypes.createStructField("row_index", DataTypes.IntegerType, true),
            DataTypes.createStructField("column_index", DataTypes.IntegerType, true),
            DataTypes.createStructField("column_name", DataTypes.StringType, true),
            DataTypes.createStructField("column_label", DataTypes.StringType, true),
            DataTypes.createStructField("column_format", DataTypes.StringType, true),
            DataTypes.createStructField("data_type", DataTypes.StringType, true),
            DataTypes.createStructField("value_string", DataTypes.StringType, true),
            DataTypes.createStructField("value_double", DataTypes.DoubleType, true),
            DataTypes.createStructField("value_date", DataTypes.DateType, true),
            DataTypes.createStructField("value_time", DataTypes.IntegerType, true),
            DataTypes.createStructField("value_timestamp", DataTypes.TimestampType, true),
            DataTypes.createStructField("error_msg", DataTypes.StringType, true)
    });

    private boolean hasNext;
    private String logicalPath;
    private Path tempDir;
    private Path tempFile;
    private int rowIndex = -1;
    private int colIndex;
    private Connection conn;
    private Statement stmt;
    private ResultSet rs;
    private	String datasetLabel;
    private int colCount;
    private String[] colNames;
    private	int[] colTypes;
    private	String[] colFormats;
    private	String[] colLabels;
    private String errorMsg;

    SJDBCRowIterator(PortableFile portableFile) {
        logicalPath = portableFile.getLogicalPath().toString();
        if(logicalPath.endsWith(".sas7bdat") || logicalPath.endsWith(".xpt")) {
            try {
                createTempFile(portableFile);
                init();
                loadMetaData();
                nextRow();
                if(!hasNext) {
                    errorMsg = "Empty SAS dataset";
                    logger.warn(errorMsg + ": " + logicalPath);
                }
            } catch (Throwable t) {
                cleanUp();
                errorMsg = "Failed to open S-JDBC connection: " + t.getMessage();
                logger.error("Failed to open S-JDBC connection for " + logicalPath, t);
            }
        } else {
            errorMsg = "Unsupported SAS dataset (only .sas7bdat and .xpt supported): " + logicalPath;
            logger.error(errorMsg);
        }
    }

    @Override
    public boolean hasNext() {
        return errorMsg != null || hasNext;
    }

    @Override
    public Row next() {
        if(!hasNext()) {
            throw new NoSuchElementException();
        }

        Object[] values = new Object[schema.length()];
        values[0] = logicalPath;
        if(errorMsg != null) {
            values[values.length - 1] = errorMsg;
            errorMsg = null;
        } else if(addValues(values)) {
            if (++colIndex == colCount) {
                nextRow();
            }
        }
        return RowFactory.create(values);
    }

    private boolean addValues(Object[] values) {
        try {
            int i = 0;
            values[++i] = datasetLabel;
            values[++i] = Integer.valueOf(rowIndex);
            values[++i] = Integer.valueOf(colIndex);
            values[++i] = colNames[colIndex];
            values[++i] = colLabels[colIndex];
            values[++i] = colFormats[colIndex];

            switch (colTypes[colIndex]) {
                case Types.CHAR:
                    values[++i] = "String";
                    values[i+1] = rs.getString(colIndex+1);
                    break;
                case Types.DOUBLE:
                    double d = rs.getDouble(colIndex+1);
                    values[++i] = "Double";
                    values[i+2] = rs.wasNull() ? null : Double.valueOf(d);
                    break;
                case Types.DATE:
                    values[++i] = "Date";
                    values[i+3] = rs.getDate(colIndex+1);
                    break;
                case Types.TIME:
                    Time time = rs.getTime(colIndex+1);
                    values[++i] = "Time";
                    if(!rs.wasNull()) {
                        values[i + 1] = time.toString();
                        values[i + 4] = Integer.valueOf((int)time.getTime() / 1000);
                    }
                    break;
                case Types.TIMESTAMP:
                    Timestamp ts = rs.getTimestamp(colIndex + 1);
                    if(colFormats[colIndex].startsWith("TOD")) {
                        values[++i] = "Time";
                        if(!rs.wasNull()) {
                            // Don't use Timestamp.toLocalDateTime() as is converts to local timezone
                            LocalDateTime dtc = LocalDateTime.ofEpochSecond(ts.getTime()/1000, 0, ZoneOffset.UTC);
                            LocalTime localTime = dtc.toLocalTime();
                            values[i + 1] = localTime.toString();
                            values[i + 4] = localTime.toSecondOfDay();
                        }
                    } else {
                        values[++i] = "Timestamp";
                        values[i + 5] = ts;
                    }
                    break;
                default:
                    values[values.length - 1] = "Unsupported JDBC data type " + colTypes[colIndex];
                    logger.error(values[values.length - 1] + " found in "
                            + logicalPath + " row " + rowIndex + " col " + colIndex);
            }
            return true;
        } catch(Throwable t) {
            cleanUp();
            values[values.length - 1] = "Failed to get value: " + t.getMessage();
            logger.error("Failed to get value for "
                    + logicalPath + " row " + rowIndex + " col " + colIndex, t);
            return false;
        }
    }

    private void init() throws Throwable {
        Driver driver = new Driver();
        String format = logicalPath.toLowerCase().endsWith(".xpt") ? ":xport" : "";
        Path path = logicalPath.toLowerCase().endsWith(".xpt") ? tempFile : tempDir;
        String url = "jdbc:carolina:bulk:libnames=(tmp" + format + "='" + path + "')";
        String table = "tmp";
        
        conn = driver.connect(url, null);
        stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
        
        if(logicalPath.toLowerCase().endsWith(".xpt")) {
            DatabaseMetaData md = conn.getMetaData();
            List<String> tables = new ArrayList<>();
            try (ResultSet rs = md.getTables(null, null, "%", null)) {
                if (rs.next()) {
                    table = rs.getString("TABLE_NAME");
                    tables.add(table);
                }
            }
            if (tables.size() != 1) {
                String tableNames = String.join(", ", tables);
                String msg = String.format("A single table in XPT-file is supported, found %d: %s", tables.size(), tableNames);
                throw new RuntimeException(msg);
            }
        }

        rs = stmt.executeQuery("select * from tmp." + table);
    }

    private void createTempFile(PortableFile portableFile) throws Throwable {
        String filename = logicalPath.toLowerCase().endsWith(".xpt") ? "tmp.xpt" : "tmp.sas7bdat";
        tempDir = Files.createTempDirectory("tmp");
        tempFile = Paths.get(tempDir.toString(), filename);
        portableFile.processWithThenClose(stream -> Files.copy(stream, tempFile));
    }

    private void loadMetaData() throws Throwable {
        ResultSetMetaData md = rs.getMetaData();
        DataSetInfo info = md.unwrap(DataSetInfo.class);
        datasetLabel = info.getLabel();
        colCount = md.getColumnCount();
        colNames = new String[colCount];
        colTypes = new int[colCount];
        colFormats = new String[colCount];
        colLabels = new String[colCount];
        for(int i = 0; i < colCount; i++) {
            colNames[i] = md.getColumnName(i + 1);
            colTypes[i] = md.getColumnType(i + 1);
            colFormats[i] = info.getVariableFormat(i + 1);
            colLabels[i] = info.getVariableLabel(i + 1);
        }
    }

    private void nextRow() {
        try {
            hasNext = rs.next();
            if (!hasNext) {
                cleanUp();
            } else {
                colIndex = 0;
                rowIndex++;
            }
        } catch(Throwable t) {
            cleanUp();
            errorMsg = "Failed to get next row: " + t.getMessage();
            logger.error("Failed to get next row for " + logicalPath, t);
        }
    }

    private void cleanUp() {
        hasNext = false;
        close(rs, stmt, conn);
        delete(tempFile, tempDir);
    }

    private void close(AutoCloseable... closables) {
        for (AutoCloseable closable : closables) {
            try {
                closable.close();
            } catch(Throwable t) {
                logger.error("Failed to close S-JDBC resource", t);
            }
        }
    }

    private void delete(Path... paths) {
        for (Path path : paths) {
            try {
                Files.delete(path);
            } catch(Throwable t) {
                logger.error("Failed to delete temp file", t);
            }
        }
    }

    static Encoder<Row> encoder() {
        return RowEncoder.apply(schema);
    }

    public String[] getColFormats(){
        return colFormats;
    }

    public String[] getColLabels(){
        return colLabels;
    }

    public int[] getColTypes(){
        return colTypes;
    }

    public String[] getColNames(){
        return colNames;
    }
}
