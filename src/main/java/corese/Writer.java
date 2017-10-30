
package corese ;

import java.io.File ;
import java.util.List ;
import java.nio.file.Path ;
import java.nio.file.Files ;
import java.nio.file.Paths ;
import java.io.IOException ;
import java.nio.file.LinkOption ;
import java.nio.file.StandardOpenOption ;
import java.nio.charset.StandardCharsets ;

/**
 *
 * @author ryahiaoui
 */
public class Writer {
    
    
    public List<String> readTextFile(String fileName) throws IOException {
      Path path = Paths.get(fileName)                          ;
      return Files.readAllLines(path, StandardCharsets.UTF_8 ) ;
    }

    public static void writeTextFile(List<String> strLines, String fileName) throws IOException {
        Path path = Paths.get(fileName)                                                 ;
        Files.write(path, strLines, StandardCharsets.UTF_8,  StandardOpenOption.APPEND) ;
    }
    
    public static void checkFile(String path ) throws IOException {
       
       String directory = path.substring(0 , path.lastIndexOf("/"))                     ;
       Path pat = Paths.get(path)                                                       ;
       boolean exists = Files.exists(pat, new LinkOption[]{ LinkOption.NOFOLLOW_LINKS}) ;
       
       if(!exists)                  {
          checkDirectory(directory) ;
       }
       else                         {
           deleteFile(path)         ;
       }
       
       createFile(path)             ;
    }

    private static void checkDirectory( String directory ) throws IOException {
      
     Path path = Paths.get(directory)                                     ;
     if(!Files.exists(path, new LinkOption[]{ LinkOption.NOFOLLOW_LINKS}) )
       Files.createDirectory(path)                                        ;
    }
    
    public static void deleteFile ( String path ) throws IOException {
      Path pat = Paths.get(path)                                     ;
      if(Files.exists(pat) )   {
          Files.delete(pat)                                          ;
      }
   }
    
    private static void createFile( String path ) throws IOException {
        File file = new File(path)                                   ;
        file.createNewFile()                                         ;
    }
    
    public static boolean existFile( String path ) throws IOException {
        File file = new File(path)                                    ;
        return file.exists()                                          ;
    }
    
    public static boolean existDirectory( String path ) throws IOException {
        File file = new File(path)                                         ;
        return file.exists() && file.isDirectory()                         ;
    }
    
    public static void checkNotEmptyDirectory(String directory) throws IOException {
      
       long count = Files.list(Paths.get(directory))
                         .filter(p -> p.toFile().isFile())                    
                         .count() ;
           
        if( count == 0 ) {
             throw new IllegalArgumentException( " No file found in the derectory ; " + directory ) ;
        }
    }
    
    public static String checkAndBuildDirectoryPath( String basePath , String directoryName ) throws IOException {
               
       String path = Files.list(Paths.get(basePath ))
                          .filter(p -> p.toFile().isDirectory())                    
                          .filter(p -> p.toAbsolutePath().toString()
                                        .equalsIgnoreCase(basePath + File.separator + directoryName ))
                          .map(p -> p.toAbsolutePath().toString())
                          .findFirst()
                          .orElse(null) ;
           
        if( path == null ) {
             throw new IllegalArgumentException( " No file found in the derectory ; " + directoryName ) ;
        }
        
        return path  ;
    }

    public static String getfileName( String outputFile ) {
       Path path = Paths.get(outputFile)    ;
       return path.getFileName().toString() ;
    }    
      
    public static String getFileExtension( String fileName ) {      
       if(fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0 )
       return fileName.substring(fileName.lastIndexOf(".") ) ;
       else return "" ;
    }
     
    public static String getFileWithoutExtension( String fileName ) {      
         return fileName.replaceFirst("[.][^.]+$", "") ;
    }
    
     public static String getFolder( String outputFile ) {
       if(outputFile.endsWith("/")) return outputFile ;
       Path path = Paths.get(outputFile)              ;
       return path.getParent().toString() + "/"       ;
    }

}

