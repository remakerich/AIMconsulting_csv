package com.powerofmya;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        String[] inputfiles = {"input1.csv", "input2.csv"};

        File tmpfolder = new File("tmp");
        if (tmpfolder.mkdirs())
            System.out.println("tmp folder created");

        CountDownLatch countdown = new CountDownLatch(inputfiles.length);
        List<CSVThread> threads = new ArrayList<>();
        for (String file : inputfiles)
            threads.add(new CSVThread(file, countdown));

        for (CSVThread thread : threads) {
            thread.start();
        }
        countdown.await();

        for (File tmpfile : Objects.requireNonNull(tmpfolder.listFiles())) {
            if (tmpfile.delete())
                System.out.printf("%s removed\n", tmpfile.getName());
        }

        if (tmpfolder.delete())
            System.out.println("tmp folder removed");

        System.out.println("main DONE");
    }
}

class CSVThread extends Thread {

    private final String path;
    private final CountDownLatch countdown;

    public CSVThread(String path, CountDownLatch countdown) {
        this.countdown = countdown;
        this.path = path;
    }

    public void write(String filename, String item, boolean append) throws IOException {

        BufferedWriter bw = new BufferedWriter(new FileWriter(filename, append));
        bw.write(String.format("%s;", item));
        bw.close();
    }

    @Override
    public void run() {

        try {
            BufferedReader inputread = new BufferedReader(new FileReader(path));

            String[] headers = inputread.readLine().split(";");

            String line;
            while ((line = inputread.readLine()) != null) {
                String[] items = line.split(";");

                for (int i = 0; i < headers.length; i++) {
                    String tmpfile = String.format("tmp\\%s_tmp.csv", headers[i]);
                    write(tmpfile, items[i], true);
                }
            }
            inputread.close();

            for (String header : headers) {

                String tmpfile = String.format("tmp\\%s_tmp.csv", header);
                String finalfile = String.format("%s.csv", header);

                BufferedReader tmpread = new BufferedReader(new FileReader(tmpfile));
                String[] tmpdata = tmpread.readLine().split(";");
                tmpread.close();

                List<String> uniques = new ArrayList<>();
                for (String item : tmpdata) {
                    if (!uniques.contains(item)) {
                        uniques.add(item);
                    }
                }
                write(finalfile, String.join(";", uniques), false);
            }
            System.out.printf("Updated: %s .csv.... ", Arrays.toString(headers));
        } catch (Exception ex) {
            System.out.printf("%s\n", ex);
        }
        System.out.printf("Thread %s DONE.\n", path);
        countdown.countDown();
    }
}