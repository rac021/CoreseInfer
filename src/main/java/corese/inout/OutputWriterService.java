
package corese.inout;

import java.util.List ;
import java.io.FileWriter ;
import java.nio.file.Paths ;
import java.nio.file.Files ;
import java.io.IOException ;
import java.io.PrintWriter ;
import java.io.BufferedWriter ;
import java.nio.file.StandardOpenOption ;
import java.nio.charset.StandardCharsets ;

/**
 *
 * @author ryahiaoui
 */
public class OutputWriterService implements AutoCloseable {

  private final String         FILE_NAME ;

  private final FileWriter     fw        ; 
  private final BufferedWriter bw        ; 
  private final PrintWriter    out       ; 


  public OutputWriterService( String FILE_NAME ) throws IOException       {
    this.FILE_NAME = FILE_NAME                                            ;
    this.fw        = new FileWriter(FILE_NAME, true)                      ;
    this.bw        =  Files.newBufferedWriter(Paths.get(FILE_NAME)        , 
                      StandardCharsets.UTF_8, StandardOpenOption.APPEND)  ;
    this.out       = new PrintWriter( bw, false )                         ;
  }

  @Override
  public void close() throws IOException {
    this.fw.close()  ;
    this.bw.close()  ;
    this.out.close() ;
    System.out.println(" Resources closed for file : " + FILE_NAME ) ;
  }

  public void writeToFile(List<String> lines ) {
   
      for(String line : lines) {
          out.println(line)    ;
      }
      out.flush()              ;
  }

  public void writeToFile( String line ) {
      out.println(line)                  ;
      out.flush()                        ;
  }
  
}
