package edu.coursera.distributed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * A basic and very limited implementation of a file server that responds to GET
 * requests from HTTP clients.
 */
public final class FileServer {
  /**
   * Main entrypoint for the basic file server.
   *
   * @param socket Provided socket to accept connections on.
   * @param fs A proxy filesystem to serve files from. See the PCDPFilesystem
   *           class for more detailed documentation of its usage.
   * @throws IOException If an I/O error is detected on the server. This
   *                     should be a fatal error, your file server
   *                     implementation is not expected to ever throw
   *                     IOExceptions during normal operation.
   */
  public void run(final ServerSocket socket, final PCDPFilesystem fs) throws IOException {
    /*
     * Enter a spin loop for handling client requests to the provided
     * ServerSocket object.
     */
    while (true) {
      // TODO 1) Use socket.accept to get a Socket object
      Socket s = socket.accept();

      /*
       * TODO 2) Using Socket.getInputStream(), parse the received HTTP
       * packet. In particular, we are interested in confirming this
       * message is a GET and parsing out the path to the file we are
       * GETing. Recall that for GET HTTP packets, the first line of the
       * received packet will look something like:
       *
       *     GET /path/to/file HTTP/1.1
       */
      InputStream stream = s.getInputStream();
      InputStreamReader reader = new InputStreamReader(stream);
      BufferedReader bufferedReader = new BufferedReader(reader);

      String line = bufferedReader.readLine();
      assert line != null;
      assert line.startsWith("GET");
      final String path = line.split(" ")[1];

      PCDPPath pcdpPath = new PCDPPath(path.toString());

      OutputStream out = s.getOutputStream();
      PrintWriter printer = new PrintWriter(out);

      /*
       * TODO 3) Using the parsed path to the target file, construct an
       * HTTP reply and write it to Socket.getOutputStream(). If the file
       * exists, the HTTP reply should be formatted as follows:
       *
       *   HTTP/1.0 200 OK\r\n
       *   Server: FileServer\r\n
       *   \r\n
       *   FILE CONTENTS HERE\r\n
       *
       * If the specified file does not exist, you should return a reply
       * with an error code 404 Not Found. This reply should be formatted
       * as:
       *
       *   HTTP/1.0 404 Not Found\r\n
       *   Server: FileServer\r\n
       *   \r\n
       *
       * Don't forget to close the output stream.
       */
      if (fs.readFile(pcdpPath) != null) {
        printer.write("HTTP/1.0 200 OK\r\n");
        printer.write("\r\n");
        printer.write("\r\n");
        printer.write(fs.readFile(pcdpPath) + "\r\n");
      } else {
        printer.write("HTTP/1.0 404 Not Found\r\n");
        printer.write("\r\n");
        printer.write("\r\n");
      }

      printer.flush();
      out.close();
    }
  }
}
