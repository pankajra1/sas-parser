package com.novartis.data42.parser.sas;

import java.io.InputStream;
import com.novartis.data42.parser.sas.SJDBCRowIterator;
import com.palantir.spark.binarystream.InputStreamProcessor;
import com.palantir.spark.binarystream.InputStreamToIteratorFunction;
import com.palantir.spark.binarystream.data.FileStatus;
import com.palantir.spark.binarystream.data.PortableFile;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

import com.palantir.util.syntacticpath.Path;
import com.palantir.util.syntacticpath.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.sql.Types;

public class SasParserXptTest{
    SJDBCRowIterator rowIter;
    Map<String, List<String>> expectedData;


    @Before
    public void init() throws Exception{
        PortableFile pFile = new PortableFile() {
            @Override
            public FileStatus getStatus() {
                return null;
            }

            @Override
            public Path getLogicalPath() {
                return Paths.get("formats_xpt.xpt");
            }
            
            @Override
            public <T, E extends Exception> T processWithThenClose(InputStreamProcessor<T, E> function) {
                try (InputStream is = getClass().getClassLoader().getResourceAsStream("formats_xpt.xpt")) {
                    return function.process(is);
                }catch(Exception e){
                    e.printStackTrace();
                }
                return null;
            }


            @Override
            public <T, E extends Exception> T processWith(InputStreamProcessor<T, E> function) {
                return null;
            }

            @Override
            public <T, E extends Exception> Iterator<T> convertToIterator(InputStreamToIteratorFunction<T, E> function) {
                return null;
            }


        };
        
        
        rowIter = new SJDBCRowIterator(pFile);

        expectedData = new LinkedHashMap<>();
        expectedData.put("VAR01", Arrays.asList("number", "8", "BEST9.3", "9", "3"));
        expectedData.put("VAR02", Arrays.asList("number", "8", "9.3", "9", "3"));
        expectedData.put("VAR03", Arrays.asList("number", "8", "DATE.", null, null));
        expectedData.put("VAR04", Arrays.asList("number", "8", "DATE7.", "7", null));
        expectedData.put("VAR05", Arrays.asList("number", "8", "DATE9.", "9", null));
        expectedData.put("VAR06", Arrays.asList("number", "8", "YYMMDD.", null, null));
        expectedData.put("VAR07", Arrays.asList("number", "8", "YYMMDD8.", "8", null));
        expectedData.put("VAR08", Arrays.asList("number", "8", "YYMMDD10.", "10", null));
        expectedData.put("VAR09", Arrays.asList("number", "8", "TIME.", null, null));
        expectedData.put("VAR10", Arrays.asList("number", "8", "TIME5.", "5", null));
        expectedData.put("VAR11", Arrays.asList("number", "8", "TIME8.", "8", null));
        expectedData.put("VAR12", Arrays.asList("number", "8", "HHMM.", null, null));
        expectedData.put("VAR13", Arrays.asList("number", "8", "HHMM5.", "5", null));
        expectedData.put("VAR14", Arrays.asList("number", "8", "DATETIME.", null, null));     
    }
    
    @Test
    public void testColumnCount(){
        assertEquals(rowIter.next().length(), 14);
    }

    @Test
    public void testColumnNames(){
        String[] actualCols = rowIter.getColNames();
        assertEquals(Arrays.asList(expectedData.keySet().toArray(new String[0])), Arrays.asList(actualCols));
    }

    @Test
    public void testColumnTypes(){
        List<Integer> expected = Arrays.asList(Types.DOUBLE, Types.DOUBLE, Types.DATE, Types.DATE, Types.DATE,
                                 Types.DATE, Types.DATE, Types.DATE, Types.TIME, Types.TIME, Types.TIME, Types.TIME,
                                 Types.TIME, Types.TIMESTAMP);
        assertEquals(expected, Arrays.stream(rowIter.getColTypes()).boxed().collect(Collectors.toList()));
    }

    @Test
    public void testColumnFormats(){
        Collection<List<String>> values = expectedData.values();
        List<String> expected = new ArrayList<>();
        Iterator<List<String>> iterator = values.iterator();
        while (iterator.hasNext()){
            expected.add(iterator.next().get(2));
        }
        assertEquals(expected, Arrays.asList(rowIter.getColFormats()));
    }

    @Test
    public void testRowCount(){
        int count = 0;
        while(rowIter.hasNext()){
            rowIter.next();
            count ++;
        }
        assertEquals(14, count);
    }

    @Test
    public void testSpecialCharsInColumnNames(){
        String pattern = "^[a-zA-Z0-9_]+$";
        int counter = 0;
        String[] actualCols = rowIter.getColNames();
        List<String> cols = Arrays.asList(actualCols);
        for (String col : cols){
            if(!col.matches(pattern))
                counter++;
        }
        assertEquals(0, counter);
    }


    @Test
    public void testColumnLength(){
        String[] actualCols = rowIter.getColNames();
        long failedColsCount = Arrays.asList(actualCols).stream().filter(s -> s.length() > 32).count();
        assertEquals(0, failedColsCount);
    }

}
