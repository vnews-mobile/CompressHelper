import net.jpountz.lz4.*;

import java.io.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Create by xuantang
 * @date on 12/7/17
 */
public class FileUtil {

    public static void main(String[] args) {
        //TestCompress();
        //TestBlockCompress();
        TestBlockSliceFileCompress();
    }
    /**
     *
     * @param filename
     * @return
     */
    private static int getTotalSize(InputStream inputStream) {
        DataInputStream dataInputStream = null;
        int size = 0;
        try {
            dataInputStream = new DataInputStream(inputStream);
            int number = dataInputStream.available() / 4;
            for (int i = 0; i < number; i++) {
                System.out.println(dataInputStream.readInt());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return size;
    }

    /**
     * Write result to file
     * @param filename
     * @param res
     */
    public static void writeIntToFile(String filename, int res, boolean append) {
        File file = new File(filename);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        DataOutputStream dataOutputStream = null;
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(filename, true);
            dataOutputStream = new DataOutputStream(fileOutputStream);
            dataOutputStream.writeInt(res);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        FileWriter fileWriter = null;
//        try {
//            fileWriter = new FileWriter(file, append);
//            fileWriter.append(res + "\n");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }finally {
//            try {
//                fileWriter.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
    }

    /**
     *
     */
    private static void TestCompress() {
        File inFile = new File("/d1/input/slide.db");
        File outFile = new File("/d1/input/slide.lz4");
        try {
            FileInputStream fileInputStream = new FileInputStream(inFile);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);

            int len = fileInputStream.available();
            byte[] content = new byte[len];
            byte[] buf = new byte[1024];
            int count;
            int index = 0;
            while ((count = bufferedInputStream.read(buf)) != -1) {
                System.arraycopy(buf, 0, content, index*1024, count);
                index++;
            }

            long start = System.currentTimeMillis();

            byte[] compress = lz4Compress(content);
            long middle = System.currentTimeMillis();
            System.out.println(Thread.currentThread().toString() + " lz4Factory com Time: " +  (middle - start) + " ms");
            byte[] bytes = lz4Decompress(compress);
            long end = System.currentTimeMillis();
            System.out.println(Thread.currentThread().toString() + " lz4Factory decom Time: " +  (end - middle) + " ms");

            System.out.println(compress.length / 1024.0 / 1024.0 + "MB");

            System.out.println(content.length == bytes.length);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(outFile));

            assert compress != null;
            bufferedOutputStream.write(compress);

            bufferedOutputStream.close();
            bufferedInputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param inputStream
     * @param type
     * @return
     */
    private static BlockingQueue<Integer> getQueueFromFile(InputStream inputStream, String type) {
        BlockingQueue<Integer> queue = new ArrayBlockingQueue<Integer>(8);
        DataInputStream dataInputStream = null;
        try {
            dataInputStream = new DataInputStream(inputStream);
            String line;
            if (type.equals("write")) {
                dataInputStream.readInt();
            } else if (type.equals("read")) { }
            else {
                throw new IllegalArgumentException();
            }
            int number = dataInputStream.available() / 4;
            for (int i = 0; i < number; i++) {
                queue.add(dataInputStream.readInt());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return queue;
    }
    /**
     *
     * @param filename
     * @param type
     * @return
     */
    private static BlockingQueue<Integer> getQueueFromFile(String filename, String type) {
        BlockingQueue<Integer> queue = new ArrayBlockingQueue<Integer>(9);
        BufferedReader writeReader = null;
        try {
            writeReader = new BufferedReader(
                    new FileReader(filename));
            String line;
            if (type.equals("write")) {
                writeReader.readLine();
            } else if (type.equals("read")) { }
            else {
                throw new IllegalArgumentException();
            }
            while ((line = writeReader.readLine()) != null) {
                if (line.length() > 0) {
                    Integer size = Integer.parseInt(line);
                    queue.add(size);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return queue;
    }

    /**
     *
     * @param filename
     * @return
     */
    private static int getTotalSize(String filename) {
        BufferedReader writeReader = null;
        int size = 0;
        try {
            writeReader = new BufferedReader(
                    new FileReader(filename));
            String line = writeReader.readLine();
            size = Integer.parseInt(line);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return size;
    }

    public void TestDecompress() {
        int totalSize = getTotalSize("/d1/initSize.txt");
        BlockingQueue<Integer> writeQueue = getQueueFromFile("/d1/initSize.txt", "write");
        BlockingQueue<Integer> readQueue = getQueueFromFile("/d1/initSize.txt", "read");
        writeToFileBySlice("/d1/word", "/d1/word-de-slice.db",
                totalSize, readQueue, writeQueue);
    }

    /**
     *
     * @param source
     * @param des
     * @param totalSize
     * @param readQueue
     * @param writeQueue
     */
    public void writeToFile(String source, String des, int totalSize, BlockingQueue<Integer> readQueue,
                            BlockingQueue<Integer> writeQueue) {
        final int threadNumber = 1;
        int readOff = 0;
        int writeOff = 0;
        int number = readQueue.size();

        File file = new File(des);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        ExecutorService executorService = Executors.newFixedThreadPool(threadNumber);
        // calculate time
        for(int index = 0; index < number; index++) {
            BufferedRandomAccessFile readFile = null;
            BufferedRandomAccessFile writeFile = null;
            try {
                readFile = new BufferedRandomAccessFile(source, "r");
                writeFile = new BufferedRandomAccessFile(des, "rw", 10);
                writeFile.setLength(totalSize);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                Integer readSize = readQueue.take();
                Integer writeSize = writeQueue.take();
                ReadThread readThread = new ReadThread(readFile, writeFile, readOff, readSize, writeOff);
                executorService.execute(readThread);
                readOff += readSize;
                writeOff += writeSize;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        executorService.shutdown();
    }

    public void writeToFileBySlice(String source, String des, int totalSize, BlockingQueue<Integer> readQueue,
                                   BlockingQueue<Integer> writeQueue) {
        final int threadNumber = 1;
        int readOff = 0;
        int writeOff = 0;
        int number = readQueue.size();

        File file = new File(des);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        ExecutorService executorService = Executors.newFixedThreadPool(threadNumber);
        // calculate time
        for(int index = 0; index < number; index++) {
            BufferedRandomAccessFile readFile = null;
            BufferedRandomAccessFile writeFile = null;
            try {
                readFile = new BufferedRandomAccessFile(source + index + ".lz4", "r");
                writeFile = new BufferedRandomAccessFile(des, "rw", 10);
                writeFile.setLength(totalSize);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                Integer readSize = readQueue.take();
                Integer writeSize = writeQueue.take();
                ReadThread readThread = new ReadThread(readFile, writeFile, 0, readSize, writeOff);
                executorService.execute(readThread);
                //readOff += readSize;
                writeOff += writeSize;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        executorService.shutdown();
    }
    /**
     * read and write thread
     */
    class ReadThread extends Thread {
        BufferedRandomAccessFile writeFile;
        BufferedRandomAccessFile readFile;
        int readOff, readSize;
        int writeOff;

//        final int READ_SIZE = 1024 * 128;

        ReadThread(BufferedRandomAccessFile readFile,  BufferedRandomAccessFile writeFile,
                   int readOff, int readSize, int writeOff) {
            this.readFile = readFile;
            this.writeFile = writeFile;
            this.readSize = readSize;
            this.readOff = readOff;
            this.writeOff = writeOff;
        }

        @Override
        public void run() {
            try {
                readFile.seek(readOff);
                // read data
                writeFile.seek(writeOff);
                // read data according to the file size
                // write to file
                byte[] bytes = new byte[readSize];
                readFile.read(bytes);
                byte[] uncompress = lz4Decompress(bytes);
                writeFile.write(uncompress, 0, uncompress.length);
                readFile.close();
                writeFile.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            super.run();
        }
    }
    /**
     * read slice thread
     */
    class ReadSliceThread extends Thread {
        BufferedRandomAccessFile writeFile;
        InputStream readFile;
        int readOff, readSize;
        int writeOff;

//        final int READ_SIZE = 1024 * 128;

        ReadSliceThread(InputStream inputStream,  BufferedRandomAccessFile writeFile, int readSize, int writeOff) {
            this.readFile = inputStream;
            this.writeFile = writeFile;
            this.readSize = readSize;
            this.writeOff = writeOff;
        }

        @Override
        public void run() {
            try {
                // read data
                writeFile.seek(writeOff);
                // read data according to the file size
                // write to file
                byte[] bytes = new byte[readSize];
                readFile.read(bytes);
                byte[] uncompress = lz4Decompress(bytes);
                writeFile.write(uncompress, 0, uncompress.length);
                readFile.close();
                writeFile.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            super.run();
        }
    }


    /**
     *
     */
    private static void TestBlockCompress() {
        int BLOCK_SIZE = 8;
        File inFile = new File("/d1/word.db");
        try {
            inFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File outFile = new File("/d1/word.lz4");
        File sliceFile = new File("/d1/sliceSize.dat");
        try {
            sliceFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            FileInputStream fileInputStream = new FileInputStream(inFile);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);

            int len = fileInputStream.available();
            writeIntToFile("/d1/initSize.dat", len, false);

            int oneWriteSize = len / BLOCK_SIZE;

            long start = System.currentTimeMillis();

            byte[] content = new byte[9547776];
            System.out.println(oneWriteSize);
            byte[] buf = new byte[1024];
            int count;
            int index = 0;
            int tmp = 0;
            int compressSize = 0;
            while ((count = bufferedInputStream.read(buf)) != -1) {
                // more than time size
                tmp += count;
                //.System.out.println(len);
                try {
                    System.arraycopy(buf, 0, content, index * 1024, count);
                } catch (Exception e) {
                    System.out.println(tmp);
                }

                index++;
                // write
                if (tmp >= oneWriteSize || count < 1024) {
                    System.out.println("_____________");
                    byte[] compress = lz4Compress(content, 0, tmp);
                    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
                            new FileOutputStream(outFile, true));
                    bufferedOutputStream.write(compress);
                    bufferedOutputStream.close();
                    compressSize += compress.length;
                    writeIntToFile("/d1/sliceSize.dat", compress.length, true);
                    writeIntToFile("/d1/initSize.dat", tmp, true);
                    index = 0;
                    tmp = 0;
                }
            }



            long middle = System.currentTimeMillis();
            System.out.println(Thread.currentThread().toString() + " lz4Factory com Time: " +  (middle - start) + " ms");

            long end = System.currentTimeMillis();
            System.out.println(Thread.currentThread().toString() + " lz4Factory decom Time: " +  (end - middle) + " ms");

            System.out.println(compressSize / 1024.0 / 1024.0 + "MB");


            bufferedInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     */
    private static void TestBlockSliceFileCompress() {
        int BLOCK_SIZE = 8;
        File inFile = new File("/d1/word.db");
        try {
            inFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        int slice = 0;

        try {
            FileInputStream fileInputStream = new FileInputStream(inFile);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);

            int len = fileInputStream.available();
            writeIntToFile("/d1/initSize.dat", len, false);
            int oneWriteSize = len / BLOCK_SIZE;

            long start = System.currentTimeMillis();
            System.out.println(oneWriteSize % 512);
            //byte[] content = new byte[9547776];
            byte[] content = new byte[oneWriteSize];
            byte[] buf = new byte[512];
            int count;
            int index = 0;
            int tmp = 0;
            int compressSize = 0;
            while ((count = bufferedInputStream.read(buf)) != -1) {
                // more than time size
                tmp += count;
                System.arraycopy(buf, 0, content, index*512, count);
                index++;
                // write
                if (tmp >= oneWriteSize || count < 512) {
                    byte[] compress = lz4Compress(content, 0, tmp);
                    File outFile = new File("/d1/word" + slice++ + ".lz4");
                    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
                            new FileOutputStream(outFile, true));
                    bufferedOutputStream.write(compress);
                    bufferedOutputStream.close();
                    compressSize += compress.length;
                    writeIntToFile("/d1/sliceSize.dat", compress.length, true);
                    writeIntToFile("/d1/initSize.dat", tmp, true);
                    index = 0;
                    tmp = 0;
                }
            }



            long middle = System.currentTimeMillis();
            System.out.println(Thread.currentThread().toString() + " lz4Factory com Time: " +  (middle - start) + " ms");

            long end = System.currentTimeMillis();
            System.out.println(Thread.currentThread().toString() + " lz4Factory decom Time: " +  (end - middle) + " ms");

            System.out.println(compressSize / 1024.0 / 1024.0 + "MB");


            bufferedInputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //lz4压缩
    public static byte[] lz4Compress(byte[] data, int off, int size) throws IOException {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        LZ4Compressor compressor = factory.fastCompressor();
        LZ4BlockOutputStream compressedOutput = new LZ4BlockOutputStream(byteOutput, 8192, compressor);
        compressedOutput.write(data, off, size);
        compressedOutput.close();
        return byteOutput.toByteArray();
    }

    //lz4压缩
    public static byte[] lz4Compress(byte[] data) throws IOException {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        LZ4Compressor compressor = factory.fastCompressor();
        LZ4BlockOutputStream compressedOutput = new LZ4BlockOutputStream(byteOutput, 8192, compressor);
        compressedOutput.write(data);
        compressedOutput.close();
        return byteOutput.toByteArray();
    }

    public static byte[] lz4Decompress(byte[] data) throws IOException {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
        LZ4FastDecompressor decompresser = factory.fastDecompressor();
        LZ4BlockInputStream lzis = new LZ4BlockInputStream(new ByteArrayInputStream(data), decompresser);
        int count;
        byte[] buffer = new byte[8192];
        while ((count = lzis.read(buffer)) != -1) {
            baos.write(buffer, 0, count);
        }
        lzis.close();
        return baos.toByteArray();
    }

}
