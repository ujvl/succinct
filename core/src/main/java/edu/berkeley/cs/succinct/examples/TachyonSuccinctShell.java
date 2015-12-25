package edu.berkeley.cs.succinct.examples;

import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.regex.SuccinctRegEx;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.ReadType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.InStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class TachyonSuccinctShell {

    private static final String READ_TYPE = "NO_CACHE";

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("Parameters: " +
                    "[tachyon-master-location] " +
                    "[file-path]");
            System.exit(-1);
        }

        setupTFS(args[0]);
        TachyonURI inFileURI = new TachyonURI(args[1]);

        ReadType rType = ReadType.valueOf(READ_TYPE);
        InStreamOptions readOptions = new InStreamOptions.Builder(ClientContext.getConf()).setReadType(rType).build();

        try {

            TachyonFileSystem tfs = TachyonFileSystem.TachyonFileSystemFactory.get();
            TachyonFile file = tfs.open(inFileURI);
            ByteBuffer byteBuffer = readBytes(tfs, file, readOptions);

            SuccinctFileBuffer succinctFileBuffer = new SuccinctFileBuffer();
            succinctFileBuffer.readFromStream(new DataInputStream(new ByteArrayInputStream(byteBuffer.array())));

            activateShell(succinctFileBuffer);

        } catch (TachyonException|IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Reads bytes in from file existing in tfs
     * @param tfs tachyon file system
     * @param file file to read from
     * @param readOps read options
     * @return byte buffer of file
     * @throws IOException
     * @throws TachyonException
     */
    public static ByteBuffer readBytes(TachyonFileSystem tfs, TachyonFile file, InStreamOptions readOps)
            throws IOException, TachyonException {
        FileInStream inStream = tfs.getInStream(file, readOps);
        ByteBuffer buf = ByteBuffer.allocate((int) inStream.remaining());
        inStream.read(buf.array());
        buf.order(ByteOrder.nativeOrder());
        return buf;
    }

    public static void setupTFS(String masterURI) {
        TachyonURI masterLoc = new TachyonURI(masterURI);
        TachyonConf tachyonConf = ClientContext.getConf();
        tachyonConf.set(Constants.MASTER_HOSTNAME, masterLoc.getHost());
        tachyonConf.set(Constants.MASTER_PORT, Integer.toString(masterLoc.getPort()));
        ClientContext.reset(tachyonConf);
    }

    /**
     * Runs the shell until termination for specified file buffer (adapted from SuccinctShell.java)
     * @param succinctFileBuffer file buffer to run queries on
     * @throws IOException
     */
    public static void activateShell(SuccinctFileBuffer succinctFileBuffer) throws IOException {
        BufferedReader shellReader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print("succinct> ");
            String command = shellReader.readLine();
            String[] cmdArray = command.split(" ");
            if (cmdArray[0].compareTo("count") == 0) {
                if (cmdArray.length != 2) {
                    System.err.println("Could not parse count query.");
                    System.err.println("Usage: count [query]");
                    continue;
                }
                System.out.println(
                        "Count[" + cmdArray[1] + "] = " + succinctFileBuffer.count(cmdArray[1].getBytes()));
            } else if (cmdArray[0].compareTo("search") == 0) {
                if (cmdArray.length != 2) {
                    System.err.println("Could not parse search query.");
                    System.err.println("Usage: search [query]");
                    continue;
                }
                Long[] results = succinctFileBuffer.search(cmdArray[1].getBytes());
                System.out.println("Result size = " + results.length);
                System.out.print("Search[" + cmdArray[1] + "] = {");
                if (results.length < 10) {
                    for (Long result : results) {
                        System.out.print(result + ", ");
                    }
                    System.out.println("}");
                } else {
                    for (int i = 0; i < 10; i++) {
                        System.out.print(results[i] + ", ");
                    }
                    System.out.println("...}");
                }
            } else if (cmdArray[0].compareTo("extract") == 0) {
                if (cmdArray.length != 3) {
                    System.err.println("Could not parse extract query.");
                    System.err.println("Usage: extract [offset] [length]");
                    continue;
                }
                Integer offset, length;
                try {
                    offset = Integer.parseInt(cmdArray[1]);
                } catch (Exception e) {
                    System.err.println("[Extract]: Failed to parse offset: must be an integer.");
                    continue;
                }
                try {
                    length = Integer.parseInt(cmdArray[2]);
                } catch (Exception e) {
                    System.err.println("[Extract]: Failed to parse length: must be an integer.");
                    continue;
                }
                System.out.println("Extract[" + offset + ", " + length + "] = " + new String(
                        succinctFileBuffer.extract(offset, length)));
            } else if (cmdArray[0].compareTo("regex") == 0) {
                if (cmdArray.length != 2) {
                    System.err.println("Could not parse regex query.");
                    System.err.println("Usage: regex [query]");
                    continue;
                }

                Map<Long, Integer> results;
                try {
                    SuccinctRegEx succinctRegEx = new SuccinctRegEx(succinctFileBuffer, cmdArray[1]);

                    System.out.println("Parsed Expression: ");
                    succinctRegEx.printRegEx();
                    System.out.println();

                    Set<RegExMatch> chunkResults = succinctRegEx.compute();
                    results = new TreeMap<Long, Integer>();
                    for (RegExMatch result : chunkResults) {
                        results.put(result.getOffset(), result.getLength());
                    }
                } catch (RegExParsingException e) {
                    System.err.println("Could not parse regular expression: [" + cmdArray[1] + "]: " + e.getMessage());
                    continue;
                }
                System.out.println("Result size = " + results.size());
                System.out.print("Regex[" + cmdArray[1] + "] = {");
                int count = 0;
                for (Map.Entry<Long, Integer> entry : results.entrySet()) {
                    if (count >= 10)
                        break;
                    System.out.print("offset = " + entry.getKey() + "; len = " + entry.getValue() + ", ");
                    count++;
                }
                System.out.println("...}");
            } else if (cmdArray[0].compareTo("quit") == 0) {
                System.out.println("Quitting...");
                break;
            } else {
                System.err.println("Unknown command. Command must be one of: count, search, regex, extract, quit.");
            }
        }
    }

}
