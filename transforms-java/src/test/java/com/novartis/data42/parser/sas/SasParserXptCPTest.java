package com.novartis.data42.parser.sas;

import java.io.InputStream;
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

public class SasParserXptCPTest{
    SJDBCRowIterator rowIter;
    Map<String, List<String>> expectedData;
    
    //Enabled tests
    @Before
    public void init() throws Exception{
        PortableFile pFile = new PortableFile() {
            @Override
            public FileStatus getStatus() {
                return null;
            }

            @Override
            public Path getLogicalPath() {
                return Paths.get("formats_xpt_cp.xpt");
            }
            
            @Override
            public <T, E extends Exception> T processWithThenClose(InputStreamProcessor<T, E> function) {
                try (InputStream is = getClass().getClassLoader().getResourceAsStream("formats_xpt_cp.xpt")) {
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
        expectedData.put("NAME", Arrays.asList("char", "8", null, null, null));
        expectedData.put("SEX", Arrays.asList("char", "1", null, null, null));
        expectedData.put("AGE", Arrays.asList("number", "8", null, null, null));
        expectedData.put("HEIGHT", Arrays.asList("number", "8", null, null, null));
        expectedData.put("WEIGHT", Arrays.asList("number", "8", null, null, null));    
    }

    @Test
    public void testColumnCount(){
        String[] actualCols = rowIter.getColNames();
        assertEquals(actualCols.length, 5);
    }

    @Test
    public void testColumnNames(){
        String[] actualCols = rowIter.getColNames();
        assertEquals(Arrays.asList(expectedData.keySet().toArray(new String[0])), Arrays.asList(actualCols));
    }

    @Test
    public void testColumnTypes(){
        List<Integer> expected = Arrays.asList(Types.CHAR, Types.CHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE);
        assertEquals(expected, Arrays.stream(rowIter.getColTypes()).boxed().collect(Collectors.toList()));
    }


}
