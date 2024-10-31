import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.ICSVWriter;
import com.opencsv.bean.*;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import com.opencsv.exceptions.CsvValidationException;
import com.opencsv.processor.RowProcessor;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class CsvUtils {

    /**
     *  parse InputStream into list of bean, skip first line
     *
     * @param input the input stream to parse
     * @param clazz the type of bean being populated
     * @return List of bean converted from file
     * @param <T> type of bean
     */
    public static <T> List<T> parseSkipHeader(InputStream input, Class<T> clazz) {
        var reader = getCsvReaderBuilder(input,1).build();

        return new CsvToBeanBuilder<T>(reader)
                .withType(clazz)
                .withThrowExceptions(true)
                .build()
                .parse();
    }

    /**
     *  parse multipart file into list of bean
     *  <br/>
     *  validate header by checking if the first line in file equals header in param
     *
     * @param file the file to parse
     * @param clazz the type of bean being populated
     * @param expectedHeader header wanted for file
     * @return List of bean converted from file
     * @param <T> type of bean
     * @throws IOException if exception happen during file reading
     * @throws CsvValidationException if header mismatch the expected one
     */
    public static <T> List<T> parse(InputStream file, Class<T> clazz, List<String> expectedHeader) throws IOException, CsvValidationException {
        var reader = getCsvReaderBuilder(file, 0).build();

        var actualHeader = reader.readNext();
        if (expectedHeader != null) {
            validateHeader(actualHeader, expectedHeader);
        }

        return new CsvToBeanBuilder<T>(reader)
                .withType(clazz)
                .withThrowExceptions(false)
                .build()
                .parse();
    }

    /**
     *  parse multipart file into list of string array (remove whitespace on each column except first)
     *  <br/>
     *  validate header by checking if the first column in file equals header in param
     *
     * @param file the file to parse
     * @param expectedHeader the expected header, the list contain all possible headers for the first column
     * @return List of String[] from file
     * @throws IOException if exception happen during file reading
     * @throws CsvValidationException if header mismatch the expected one
     */
    public static List<String[]> parse(InputStream file, List<String> expectedHeader) throws IOException, CsvException {
        try (var reader = getCsvReaderBuilder(file, 0)
                .withRowProcessor(removeWhitespaces())
                .build()) {

            if (expectedHeader != null && !expectedHeader.isEmpty()) {
                var actualHeader = reader.readNextSilently();
                validateHeader(actualHeader, expectedHeader);
            }

            return reader.readAll();
        }
    }

    /**
     * convert list of bean into csv (byte[]), the bean to convert must annotate it's field with @CsvBindByPosition AND @CsvBindByName in order to work
     *
     * @param datas data to convert into csv (byte array format)
     * @param clazz the class of Bean to convert
     * @return byte[] of Bean to Csv
     * @param <T> type of bean to convert
     * @throws IOException if error occur while writing csv to byte[]
     * @throws CsvRequiredFieldEmptyException if field is empty
     * @throws CsvDataTypeMismatchException if wrong field type
     */
    public static <T> byte[] writeWithHeaderAndOrder(List<T> datas, Class<T> clazz) throws IOException, CsvRequiredFieldEmptyException, CsvDataTypeMismatchException {
        try (var baos = new ByteArrayOutputStream();
             var osw = new OutputStreamWriter(baos, StandardCharsets.UTF_8);
             var csvWriter = getCsvWriter(osw)) {
            osw.write('\ufeff');
            var mappingStrategy = new NameAndPositionMappingStrategy<T>();
            mappingStrategy.setType(clazz);
            var beanToCsv = new StatefulBeanToCsvBuilder<T>(csvWriter)
                    .withMappingStrategy(mappingStrategy)
                    .build();

            beanToCsv.write(datas);
            osw.flush();
            return baos.toByteArray();
        }
    }

    private static ICSVWriter getCsvWriter(OutputStreamWriter osw) {
        return new CSVWriter(osw, ';', ICSVWriter.NO_QUOTE_CHARACTER, ICSVWriter.DEFAULT_ESCAPE_CHARACTER, ICSVWriter.DEFAULT_LINE_END);
    }

    private static CSVReaderBuilder getCsvReaderBuilder(InputStream input, int skipLine) {
        var parser = new CSVParserBuilder()
                .withSeparator(';')
                .withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS)
                .build();
        return new CSVReaderBuilder(new InputStreamReader(input, StandardCharsets.UTF_8))
                .withSkipLines(skipLine)
                .withCSVParser(parser);
    }

    private static void validateHeader(String[] actualHeader, List<String> expectedHeader) throws CsvValidationException {
        if (headerNotEquals(Arrays.asList(actualHeader), expectedHeader)) {
            System.out.printf("Header validation error actual=[%s], expected=[%s]%n", Arrays.toString(actualHeader), expectedHeader);
            throw new CsvValidationException("BAD_HEADER");
        }
    }

    private static boolean headerNotEquals(List<String> actualHeader, List<String> expectedHeader) {
        if (actualHeader == null || actualHeader.size() != expectedHeader.size()) {
            return true;
        }
        return !actualHeader.equals(expectedHeader);
    }

    /**
     * remove whitespace from each column except first and header
     * @return RowProcessor
     */
    private static RowProcessor removeWhitespaces() {
        return new RowProcessor() {
            @Override
            public String processColumnItem(String s) {
                return RegExUtils.removeAll(s, "[\\s\u202F]");
            }

            @Override
            public void processRow(String[] row) {
                // 1 to escape the first column from being processed
                for (int i = 1; i < row.length; i++) {
                    row[i] = processColumnItem(row[i]);
                }
            }
        };
    }

    /**
     * Mapping strategy to write header, ordered by @CsvBindByPosition
     * @param <T>
     */
    private static class NameAndPositionMappingStrategy<T> extends ColumnPositionMappingStrategy<T> {
        @Override
        public String[] generateHeader(T bean) throws CsvRequiredFieldEmptyException {
            final int numColumns = getFieldMap().values().size();
            super.generateHeader(bean);

            String[] header = new String[numColumns];

            BeanField<?, ?> beanField;
            for (int i = 0; i < numColumns; i++) {
                beanField = findField(i);
                String columnHeaderName = extractHeaderName(beanField);
                header[i] = columnHeaderName;
            }
            return header;
        }

        private String extractHeaderName(final BeanField<?, ?> beanField) {
            if (beanField == null || beanField.getField() == null || beanField.getField().getDeclaredAnnotationsByType(
                    CsvBindByName.class).length == 0) {
                return StringUtils.EMPTY;
            }

            final CsvBindByName bindByNameAnnotation = beanField.getField().getDeclaredAnnotationsByType(CsvBindByName.class)[0];
            return bindByNameAnnotation.column();
        }
    }
}
