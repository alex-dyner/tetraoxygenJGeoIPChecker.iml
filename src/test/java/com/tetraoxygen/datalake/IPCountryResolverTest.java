package com.tetraoxygen.datalake;

import junit.framework.TestCase;
import junit.framework.Assert;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class IPCountryResolverTest extends TestCase {
    private char Delimiter = ',';

    private File generateTestNetworkFile(String subnetInfo) throws IOException {
        return generateTestNetworkFile(subnetInfo, "dummy");
    }

    private File generateTestNetworkFile(String fileBody, String countryName) throws IOException {
        File result = File.createTempFile("IPInSubNetCheckerTest", ".tmp");
        FileWriter fw = new FileWriter(result);
        fw.write(fileBody + Delimiter + countryName);
        fw.close();
        return result;
    }

    private String executeHiveEvaluate(String ip, String network, String countryName) throws HiveException, IOException {
        IPCountryResolver checker = new IPCountryResolver();

        String fileBody = network + Delimiter + countryName;
        File lookupFile = generateTestNetworkFile(fileBody, countryName);
        String subnet = lookupFile.getAbsolutePath();

        return checker.evaluate(ip, subnet);
    }

    public void testLongIpEvaluate() throws HiveException, IOException {
        IPCountryResolver checker = new IPCountryResolver();
        File lookupFile = generateTestNetworkFile("12.12.12.12.12/12");
        Assert.assertNull(checker.evaluate("1", lookupFile.getAbsolutePath()));
    }

    public void testShortIpEvaluate() throws HiveException, IOException {
        IPCountryResolver checker = new IPCountryResolver();
        File lookupFile = generateTestNetworkFile("12.12.12.12/12");
        Assert.assertNull(checker.evaluate("1", lookupFile.getAbsolutePath()));
    }

    public void testNullEvaluate() throws HiveException, IOException {
        IPCountryResolver checker = new IPCountryResolver();
        File lookupFile = generateTestNetworkFile("12.12.12.12/12");
        Assert.assertNull(checker.evaluate(null, lookupFile.getAbsolutePath()));
    }

    public void testFalse000Evaluate() throws HiveException, IOException {
        IPCountryResolver checker = new IPCountryResolver();
        String subnet = "1.0.0.0/1" + Delimiter + "AAA";
        File lookupFile = generateTestNetworkFile(subnet);

        String result = checker.evaluate("128.14.230.186", lookupFile.getAbsolutePath());
        Assert.assertNull(result);
    }

    public void testNonFullByteMatchEvaluate() throws HiveException, IOException {
        String result = executeHiveEvaluate("242.7.7.7", "240.0.0.1/4", "XXX");
        Assert.assertEquals("XXX", result);
    }

    public void testTrue000Evaluate() throws HiveException, IOException {
        String result = executeHiveEvaluate("126.14.230.186", "1.0.0.0/1", "ASD");
        Assert.assertEquals("ASD", result);
    }

    public void testFasleEvaluate() throws HiveException, IOException {
        String result = executeHiveEvaluate("247.14.230.186", "210.14.0.0/19", "Z");
        Assert.assertNull(result);
    }

    public void testTrueEvaluate() throws HiveException, IOException {
        String result = executeHiveEvaluate("247.14.230.186", "247.14.230.0/24", "X");
        Assert.assertEquals("X", result);
    }

    public void testMultilineEvaluate() throws HiveException, IOException {
        String body = "4.4.4.4/31\tR\n247.14.230.186/24\tA\n247.14.10.187/30\tB\n10.0.0.3/30\tE";
        String result = executeHiveEvaluate("247.14.230.200", body, "");
        Assert.assertEquals("A", result);
    }
}
