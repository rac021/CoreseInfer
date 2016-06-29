
package corese;

/**
 *
 * @author ryahiaoui
 */

import fr.inria.acacia.corese.exceptions.EngineException ;
import fr.inria.edelweiss.kgtool.print.ResultFormat      ;
import fr.inria.edelweiss.kgraph.query.QueryProcess      ;
import fr.inria.edelweiss.kgraph.rule.RuleEngine         ;
import fr.inria.edelweiss.kgram.core.Mappings            ;
import fr.inria.edelweiss.kgram.core.Mapping             ;
import fr.inria.acacia.corese.api.IDatatype              ;
import fr.inria.edelweiss.kgraph.core.Graph              ;
import fr.inria.edelweiss.kgtool.load.Load               ;
import java.util.regex.Pattern                           ;
import java.io.IOException                               ;
import java.nio.file.Files                               ;
import java.nio.file.Paths                               ;
import java.util.ArrayList                               ;
import java.util.Arrays                                  ;
import java.util.List                                    ;
import java.util.Objects;


public class Main {
      
        private static volatile Main _instance = null ;
                
        private static Graph        g   ;
        private static Load         ld  ;
        private static RuleEngine   re  ;
      
        int flushCount = 10000          ;
 
        enum FORMAT { TTL, XML , CSV }  ;
 
        private static final String URI_VALIDATOR = "^((https?|ftp|file)://|(www\\.)|(<_:))[-a-zA-Z0-9+&@#/%?=~_|!:,.;µs%°]*[-a-zA-Z0-9+&@#/%=~_|]" ;
     
        private Main(){}

        public static Main getInstance( List<String> filesToLoad, boolean entailment ) {
                if (_instance == null) {
                        synchronized (Main.class) {
                                if (_instance == null) {
                                        _instance = new Main() ;
                                        initialize(filesToLoad , entailment) ;
                                }
                        }
                }
                return _instance ;
        }
      
        private static void initialize( List<String> filesToLoad, boolean entailment ) {
          
            try {
                g = Graph.create(entailment) ;             
                ld = Load.create(g)  ;
                     for(String file : filesToLoad ) {
                           System.out.println(" Loading file : "+file) ;
                           ld.load(file) ;
                }
            }
            catch (Exception ex) {
                    ex.printStackTrace() ;
            }
        }

      private String toStringDataType ( Mapping m, String value ) {
          
           IDatatype dt = (IDatatype) m.getValue(value) ;
           if(dt == null) return null                   ;
           
           /*
           dt.intValue()        ;
           dt.doubleValue()     ;
           dt.booleanValue()    ;
           dt.stringValue()     ; 
           dt.getLabel()        ;
           dt.getDatatypeURI()  ;
           dt.getLang()         ;
           */
           
           if ( dt.getLang() != null )     {
             return "\"" + dt.getLabel()
                    .replaceAll("\"", "'") 
                    + "\"@" + dt.getLang() ;
           }
           
           if (dt.isURI() || dt.isBlank() )  {
             return "<" + dt.getLabel() +">" ;
           }
           
           return "\"" + dt.getLabel()
                           .replaceAll("\"", "'")    + 
                           "\"^^" + dt.getDatatype() ;
      }
      
      private void genericRequest(  String  request        ,
                                    List<String> variables ,
                                    String  outputFile     , 
                                    int     fragment       , 
                                    int     numRequest     ,
                                    FORMAT  format 
                                  ) throws  IOException    {
              
                QueryProcess exec      =  QueryProcess.create(g) ;
                Mappings     map       =  null                   ;
                
                try {
                        map = exec.query(request ) ;
                } catch (EngineException e) {
                        System.out.println(" -------- " ) ;
                          e.printStackTrace()             ;
                        System.out.println(" -------- " ) ;
                }

                if( format == FORMAT.TTL || format == FORMAT.CSV ) {
                    
                    List<String> lines = new ArrayList<> () ;

                    String res    = ""    ;
                    int    count  = 0     ;
                    int    loop   = 0     ;

                    String currentFile    ;

                    currentFile =  getCurrentFile(outputFile, numRequest, fragment , loop ) ;
                    Writer.checkFile( currentFile )                                         ;

                    for ( Mapping m : map ) {

                        for(String variable : variables )              {
                            String dt =  toStringDataType(m, variable) ;
                            if(dt == null ) continue                   ;
                            res +=  dt  +  " "                         ;
                        }
                        
                          /* Ignore literal values */                        
                        if( format == FORMAT.CSV || isSubjectURIOrBlank(res) ) {
                            count ++                   ;
                            lines.add( res + " . " )   ;
                        }

                        if( fragment != 0 && count % fragment == 0  )   {
                            
                           if( ! lines.isEmpty() )  {
                               
                              Writer.writeTextFile(lines, currentFile ) ;
                              lines.clear();
                              currentFile =  getCurrentFile( outputFile , 
                                                             numRequest , 
                                                             fragment   ,
                                                             ++loop )   ;                              
                              Writer.checkFile( currentFile )           ;
                           }
                        }

                        if( lines.size() % flushCount == 0 )          {                     
                            Writer.writeTextFile(lines, currentFile ) ;
                            lines.clear()                             ;
                        }

                        res = "" ;
                    }

                    if(!lines.isEmpty()) {
                       Writer.writeTextFile(lines,  currentFile) ;
                       lines.clear()                             ;                    
                    }

                    /* Delete last file if empty */
                    if(Files.lines( Paths
                            .get(currentFile)).count() == 0 )   {
                       Paths.get(currentFile).toFile().delete() ;
                    } 
                }
                
                else if (format == FORMAT.XML )               {
                    
                    Writer.checkFile( outputFile )            ;
                    ResultFormat f = ResultFormat.create(map) ;
                    Writer.writeTextFile ( 
                          Arrays.asList(f.toString()) , 
                                                 outputFile ) ;
                }
                
                /*
                   print Turtle Result
                   TripleFormat f = TripleFormat.create(g, true);
                   System.out.println(f);
                */
        }
         
          private static boolean isSubjectURIOrBlank( String path )   { 
          
          Objects.requireNonNull( path , 
                " subject "                              +                 
                " parameter should not be null " )       ;
          if(path.isEmpty()) return false                ;
          
          String subject = path.split(" ")[0]            ;  
          
          if( subject.startsWith("<")  
              && subject.endsWith(">") )  {
              return subject.substring(1, 
                            subject.lastIndexOf(">"))
                                 .matches(URI_VALIDATOR) ;
          }
          return false ;
        }
          
        private static List<String> getVariables( String sparqlQuery ) {
        
            List<String> variables = new ArrayList<>() ;
            
            String[] tokens  = sparqlQuery.replaceAll("\\s+", " ")
                                          .replaceAll(Pattern.quote("("), " ")
                                          .replaceAll(Pattern.quote(")"), " ")
                                          .replaceAll(Pattern.quote("{"), " ")
                                          .replaceAll(Pattern.quote("}"), " ")
                                          .replaceAll(Pattern.quote(","), " ")
                                          .replaceAll(Pattern.quote(";"), " ")
                                          .replaceAll(Pattern.quote("."), " ")
                                          .split(" ") ;

            for(String token : tokens ) {
                  
               if(token.equalsIgnoreCase("AS"))            {
                   variables.remove(variables.size() - 1 ) ;
                   continue                                ;
               }
               
               if( token.startsWith("?") && 
                   !variables.contains(token )) {
                     variables.add(token)       ;
               }
               
               else if( token.equalsIgnoreCase("where") ) break ;
            }
            
          return variables ;
        }
        
        private static boolean isSelectQuery ( String query ) {
         
            return query.trim()
                        .replaceAll("\\s+", " ")
                        .toLowerCase()
                        .contains("select ")   ;
        }
        
        private static String getCurrentFile(  String outFile , 
                                               int numRequest , 
                                               int fragment   ,
                                               int loop )     {
            if ( fragment <= 0 ) {
              return outFile ; 
            }
            if(Files.isDirectory(Paths.get(outFile)) ) {
                  if(outFile.endsWith("/")) {
                    return outFile+numRequest + "." + loop    ; 
                  }
                  else {
                   return outFile+"/"+numRequest + "." + loop ; 
                  }
            }
            else {
                if(fragment > 0 ) {
                  return outFile + "." + loop ; }
                else {
                  return outFile ;              }
            }
       } 
        
        public static void main( String[] args) throws IOException    {
            
            if( args.length < 6 ) {
                System.out.println(" Nombre paramètres incomplet ! ") ;
                return ;
            }
            
            List<String> owls         = new ArrayList<>() ;
            List<String> ttl          = new ArrayList<>() ;
            List<String> queries      = new ArrayList<>() ;
            List<String> outs         = new ArrayList<>() ;
            List<Integer> fragments   = new ArrayList<>() ;
            List<String> formats      = new ArrayList<>() ;
            boolean entailment        = false             ;

            for ( int i = 0 ; i < args.length ; i++ ) {
                
                String token = args[i] ;
                
                switch(token) {
                    
                    case "-owl" :  owls.add(args[i+1])      ;        
                                   break ;
                    case "-ttl" :  ttl.add(args[i+1])       ;
                                   break ;
                    case "-out" :  outs.add(args[i+1])      ;
                                   break ;
                    case "-q"   :  queries.add(args[i+1])   ; 
                                   break ;
                    case "-f"   :  fragments.add(Integer
                                            .parseInt(
                                              args[i+1]) )  ;
                                   break ;
                    case "-e"   :  entailment = true        ;
                                   break ;
                    case "-F"   :  formats.add(args[i+1]
                                          .toUpperCase())   ;
                                   break ;
                }
            }
            
            System.out.println( " Owls : " )                              ;
            owls.stream().forEach( e -> System.out.println("  " + e ) )   ;
            System.out.println(" ttls  : " )                              ;
            ttl.stream().forEach( e ->  System.out.println("  " + e ) )   ;
            System.out.println( "                                   " )   ;
            
            if( owls.isEmpty() || ttl.isEmpty() ) {
                 System.out.println(" owl or ttl parameter is empty ! " ) ;
                 return                                                   ;
            }
            
            if( ( queries.isEmpty() ) ) {
                 System.out.println("  Error nbr parameters !! " )        ;
                 return                                                   ;
            }
            
            if (   queries.size() != outs.size()      || 
                 ( queries.size() != fragments.size() || 
                   queries.size() != formats.size() )
               )                                       {
                 
                 System.out.println(" Bad size List queries-outs-fragment !! ") ;
                 return ;
            }

            List<String> entryFiles = new ArrayList<>() ; 
            entryFiles.addAll(owls)                     ;
            entryFiles.addAll(ttl)                      ;
            
            /* Load Graph */
            Main instance = Main.getInstance(entryFiles , entailment) ;
            
            /* Travers Queries */
               for( int i = 0 ; i < queries.size() ; i++ ) {
                   
                   List<String> variables = getVariables(queries.get(i) )  ;
                   FORMAT       format    = FORMAT.valueOf(formats.get(i)) ;
                   String       query     =  queries.get(i)                ;
                   String       out       = outs.get(i)                    ;
                   int          fragment  = fragments.get(i)               ;
                   
                   if ( isSelectQuery(queries.get(i))                     || 
                        ( ! isSelectQuery(queries.get(i))                 &&  
                           ( ! format.toString().toLowerCase().
                                equals( FORMAT.TTL.toString().toLowerCase())
                           )
                        ) 
                      )                                                    {
                    
                   if( format == FORMAT.TTL && variables.size() != 3 ) {
                       System.out.print  (" Query must have exactly 3 variables ( subject, predicate, object ) " ) ;
                       System.out.println(" when Tuttle format is activated (-ttl ) " )                            ;
                       System.out.println(" See https://www.w3.org/TR/turtle  " )                                  ;
                       System.out.println(" Or try without -ttl parameter " )                                      ; 
                       return ;
                   }

                   System.out.println("-------------------------------------------")  ;
                   
                   System.out.println(" + Executing query : "  + query           )    ;
                   System.out.println(" + FRAGMENT        :  " + fragment        )    ;
                   System.out.println(" + Out             :  " + out             )    ;
              
                   instance.genericRequest( queries.get(i)   ,
                                            variables        ,
                                            outs.get(i)      ,  
                                            fragments.get(i) ,
                                            i                ,
                                            format         ) ; 
                   
                   System.out.println("-------------------------------------------" ) ;
                   
                }
                else {
                      System.out.println(queries.get(i) + " \n " +
                      " Not supported yet. Only Select Queries for the moment ! " )  ;
                }
               }
        }
}
