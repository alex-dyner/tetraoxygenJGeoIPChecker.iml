package com.tetraoxygen.datalake;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class IPCountryResolver extends UDF {
    private class NetworkInfo {
        public byte[] NetworkAddress;
        public byte PrefixSize;
        public Integer LocalCountryId;

        public NetworkInfo(byte[] networkAddress, byte prefixSize, Integer countryId) {
            this.NetworkAddress = networkAddress;
            this.PrefixSize = prefixSize;
            this.LocalCountryId = countryId;
        }

        public boolean contains(byte[] ipAddressSegm) {
            byte finalByte = (byte) (0xFF00 >> (PrefixSize & 0x07));
            int nMaskFullBytes = PrefixSize / 8;
            for (int i = 0; i < nMaskFullBytes; i++) {
                if (ipAddressSegm[i] != NetworkAddress[i]) {
                    return false;
                }
            }

            if (finalByte != 0) {
                return (ipAddressSegm[nMaskFullBytes] & finalByte) == (NetworkAddress[nMaskFullBytes] & finalByte);
            }
            return true;
        }
    }

    private Map<Integer, String> numberToCountryMap = new HashMap<Integer, String>();

    private ArrayList<NetworkInfo> cache;

    private void initHdfsLookup(String inputFilePath) throws HiveException {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream inputDataFile = fs.open(new Path(inputFilePath));
            BufferedReader lineReader = new BufferedReader(new InputStreamReader(inputDataFile));
            init(lineReader);

        } catch (Exception e) {
            throw new HiveException("Exception when attempting to access: " + inputFilePath, e);
        }
    }

    private void init(BufferedReader lineReader) throws HiveException {
        cache = new ArrayList<NetworkInfo>();
        HashMap<String, Integer> countyToNumberMap = new HashMap<String, Integer>();
        try {
            String line = null;
            int currCountryNum = 0;
            while ((line = lineReader.readLine()) != null) {
                String[] geo = line.split(",");
                String[] addressAndMask = geo[0].split("/");
                byte[] networkAddress = parseAddress(addressAndMask[0]);
                byte prefixSize = Byte.parseByte(addressAndMask[1]);

                String countryName = geo[1];
                Integer countryId = countyToNumberMap.get(countryName);
                if (countryId == null) {
                    countryId = currCountryNum++;
                    countyToNumberMap.put(countryName, countryId);
                    numberToCountryMap.put(countryId, countryName);
                }
                cache.add(new NetworkInfo(networkAddress, prefixSize, countryId));
            }
        } catch (FileNotFoundException e) {
            throw new HiveException("Input file doesn't exist", e);
        } catch (IOException e) {
            throw new HiveException("Process input file failed, please check format", e);
        }
    }

    private String getCountryNameById(Integer countryId) {
        return numberToCountryMap.get(countryId);
    }

    public String evaluate(String ipAddress, String networkFilePath) throws HiveException {
        if ((ipAddress == null) || (networkFilePath == null)) {
            return null;
        }

        if (cache == null) {
            initHdfsLookup(networkFilePath);
        }

        Integer contryId = lookup(ipAddress);
        if (contryId != null) {
            return getCountryNameById(contryId);
        }
        else {
            return null;
        }
    }

    private Integer lookup(String ipAddress) {
        byte[] ipAddressSegm = parseAddress(ipAddress);
        if (ipAddressSegm.length != 4) {
            return null;
        }

        NetworkInfo result = null;
        for (NetworkInfo n: cache) {
            if (n.contains(ipAddressSegm)) {
                result = n;
                break;
            }
        }

        return result != null ? result.LocalCountryId : null;
    }


    private byte[] parseAddress (String ipAddress){
        String[] ipAddressSegments = ipAddress.split("\\.");
        byte[] result = new byte[ipAddressSegments.length];
        for(int i = 0; i < ipAddressSegments.length; i++){
            String ipStr = ipAddressSegments[i];
            int ipInt = Integer.parseInt(ipStr);
            result[i] = (byte) (ipInt & 0xFF);
        }
        return result;
    }
}
