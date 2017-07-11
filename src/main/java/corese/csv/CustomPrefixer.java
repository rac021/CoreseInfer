
package corese.csv ;

import java.io.File ;
import corese.Writer ;
import java.util.Set ;
import java.util.Map ;
import java.util.List ;
import java.util.Arrays ;
import java.util.Objects ;
import java.util.HashMap ;
import java.io.IOException ;
import java.util.ArrayList ;
import java.nio.file.Files ;
import java.nio.file.Paths ;
import java.util.logging.Level ;
import java.util.stream.Stream ;
import java.util.regex.Pattern ;
import java.util.logging.Logger ;
import java.util.stream.IntStream ;
import java.util.stream.Collectors ;
import java.util.function.Consumer ;
import fr.inria.edelweiss.kgtool.load.Load ;
import fr.inria.acacia.corese.api.IDatatype ;
import fr.inria.edelweiss.kgraph.core.Graph ;
import fr.inria.edelweiss.kgram.core.Mapping ;
import fr.inria.edelweiss.kgram.core.Mappings ;
import static java.util.stream.Collectors.toSet ;
import fr.inria.edelweiss.kgraph.query.QueryProcess ;
import fr.inria.acacia.corese.exceptions.EngineException ;

/**
 *
 * @author ryahiaoui
 */

public class CustomPrefixer {
        
    private static final String 

    SPARQL_SEARCH_QUERY = " PREFIX skos:  <http://www.w3.org/2004/02/skos/core#>  \n " +
                          " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  \n " +
                          " PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>   \n " +
                          " SELECT DISTINCT ?prefLabelURI WHERE   {               \n " +
                          "    ?s skosxl:prefLabel ?prefLabelURI                . \n " +                              
                          "    ?prefLabelURI skosxl:literalForm ?literalForm    . \n " +                              
                          "    ?URI skos:note ?O                                . \n " +                              
                          "    FILTER REGEX( ?literalForm , \"^^{0}$\", \"i\" ) . \n " +
                          " } "   ;
  
    final static String SPARQL_FILE_NAME  = "sparql_validation.txt" ;
     
    private static final String EMPTY_RESULT   = "::EMPTY::" ;
    
    private static final String NULL_RESULT    = "::NULL::"  ;
    
    private static       Load   ld                           ;

    private static Map<String, Graph> ontologies = new HashMap<>() ;
    
    
    private static String getContainedSeparator(String label, List<String> separators) {
        return separators.stream()
                         .filter( string -> label.contains(string))
                         .findFirst().orElse("") ;
    }
   
    enum FORMAT { TTL, XML , CSV }  ;
 
    static int loop  = 0            ;
 
    private CustomPrefixer() {}
      
    private static void loadAndSaveOntology ( String directory   ,
                                              String namespace   ,
                                              boolean entailment ) {          
        try {
              if( ontologies.containsKey(namespace)) return            ;
              Graph g = Graph.create(entailment)                       ;    
              ld      = Load.create(g)                                 ;
              System.out.println("                              ")     ;
              
              Files.list( Paths.get(directory)).forEach( file -> {
                  
                 if( ! file.getFileName().toString().endsWith(".txt" ))  {
                   System.out.println( " Loading Ontology : " + 
                                       file.toAbsolutePath().toString()) ;
                   ld.load( file.toAbsolutePath().toString() )           ;
                 }
                 
               });
               
              
              ontologies.put(namespace, g)                             ;
        }
         catch (Exception ex)       {            
               ex.printStackTrace() ;
               Throwable[] suppressed = ex.getSuppressed() ;
               for( Throwable cause : suppressed )         {
                    System.err.println(cause.getMessage()) ;
               }
               System.out.println(" ") ;
         }
    }

    
    private static List<String> genericRequest(  String ontoKey         ,
                                                 String  request        ,
                                                 List<String> variables ) throws  IOException  {
              
        QueryProcess  exec   =  QueryProcess.create( ontologies.get(ontoKey)) ;
        Mappings      map    =  null                   ;
         List<String> lines  = new ArrayList<> ()      ; 
         
        try {
            
             map = exec.query(request )   ;
             String res    = ""           ;
              
               for ( Mapping m : map ) {

                   for(String variable : variables )               {
                       String dt =  toStringDataType(m , variable) ;
                       if(dt == null ) continue                    ;
                       res +=  dt  +  " "                          ;
                   }
                   
                   if ( ! res.isEmpty() ) lines.add(res.trim()) ;
               }
                        
        } catch ( EngineException e )          {
             System.out.println ( " -------- " ) ;
             e.printStackTrace()               ;
             System.out.println ( " -------- " ) ;
        }

       return lines ;
    }
    
    private static String toStringDataType ( Mapping m, String value ) {
          
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
               
           else if( token.equalsIgnoreCase("WHERE") )  break ;
        }
            
       return variables ;
    }
  
    private static void printEmptyMessageError( String variable , int numLine, int columnNumber ) {
        System.out.println("                                                                   " ) ;
        System.out.println(" *** ERROR                                                         " ) ;
        System.out.println("     No prefix found for variable : [ " + variable + " ] !         " ) ;
        System.out.println("     --> At CSV Line : " + numLine  + " // Column " +  +columnNumber ) ;              
        System.out.println("                                                                   " ) ;
    }
    
    private static void printNullMessageError( String uri , int numLine, int columnNumber ) {
        System.out.println("                                                                          " ) ;
        System.out.println(" *** ERROR                                                                " ) ;
        System.out.println("     No prefix assigned to URI : [ " + uri + " ] in the the prefix File ! " ) ;
        System.out.println("     --> At CSV Line : " + numLine  + " // Column " + +columnNumber         ) ;        
        System.out.println("                                                                          " ) ; 
    }
    private static void printExceptionMessage( int numLine, int columnNumber ) {
        System.out.println("                                                                          " ) ;
        System.out.println(" *** EXCEPTION                                                            " ) ;
        System.out.println("     Error Occured during processing                                      " ) ;
        System.out.println("     --> At CSV Line : " + numLine  + " // Column " + +columnNumber         ) ;        
        System.out.println("                                                                          " ) ; 
    }
    
    private static void printFileNotFoundExceptionAndExit( String path )                                  {
        System.out.println("                                                                          " ) ;
        System.out.println(" *** EXCEPTION                                                            " ) ;
        System.out.println("     Error Occured during processing                                      " ) ;
        System.out.println("     --> The File : [ " + path + " ] doesn't exist !                      " ) ;  
        System.out.println("                                                                          " ) ; 
        System.exit(0)                                                                                    ;
    }
    
    private static String extract( String ontoKey                 ,
                                   String sparql                  ,
                                   String variable                ,
                                   List<String> variables         ) throws Exception {
     
        List<String> result = genericRequest( ontoKey              , 
                                              sparql.replace("{0}" ,
                                              variable.trim() )    ,
                                              variables          ) ;

        if(result.size() > 1) {
            Set<String> collect = result.stream()
                                        .map( res -> res.trim().split(" ")[0] )
                                        .collect(toSet()) ;
            if(collect.size() > 1 ) {
                System.out.println(" Multiple Uris found for variable : [ "+ variable + " ] ")    ;
                collect.forEach( line -> System.err.println( "  - " + line.trim().split(" ")[0])) ;
                System.out.println(" + Retained --> " + result.get(0).trim().split(" ")[0])       ;
                System.out.println("                                                          " ) ;               
            }
        }
        
        if(result.isEmpty())    {
            return EMPTY_RESULT ;
        }
         
        if ( result.get(0).split(" ").length >= 1 ) {
            String uri   = result.get(0) ;            

            if ( uri == null) {
                return NULL_RESULT + uri ;
            }

            return uri ;
        } else 
        {
            System.out.println(" + Query must Have Only Two VARIABLES ") ;
            System.out.println(" + You can re test without [ -query ] argument ") ;
            System.exit(2) ;
        }
        
        throw new IllegalArgumentException(" Error while extracting Prefix:Class for variable -> "+ variable ) ;
    }
     
    private static String treatLabel( String              ontoKey          ,
                                      String              sparql           ,
                                      String              label            ,
                                      List<String>        sparqlVariables    )  {
        try {
            return  extract( ontoKey         ,
                             sparql          ,                               
                             label           ,
                             sparqlVariables   ) ;
            
        } catch( Exception ex ) {
            ex.printStackTrace();
        }
        return null ;
    }
    
    private static boolean okResult( boolean displayError, 
                                     String var          , 
                                     String result       , 
                                     int numLine         , 
                                     int columnNumber )  {
        
       if( result == null )   {
           if( displayError ) {
             printExceptionMessage(numLine, columnNumber)           ; 
           }
           return false ;
       }
       else if ( result.equals(EMPTY_RESULT)) {
           if( displayError ) {
             printEmptyMessageError(var, numLine, columnNumber )    ;
           }
           return false ;
       }
       else  if ( result.equals(NULL_RESULT)) {
          if( displayError ) {
             printNullMessageError( result.replace( NULL_RESULT, "") ,
                                    numLine, columnNumber)           ;
          }
          return false ;
        } 
        
       return true ;
    }
        
    private static String checkExternalQuery(String directory) {
        
        try {
            
          if( ! Writer.existFile(directory + File.separator + SPARQL_FILE_NAME )) {
              System.out.println( "                                  ") ;
              System.out.println( " Use Internal Default SPAQL Query ") ;
              System.out.println( "                                  ") ;
              System.out.println( SPARQL_SEARCH_QUERY                 ) ;
              System.out.println( "                                  ") ;
              System.out.println( " -- To override this query, create file named [ sparql_validation.txt ] ") ;
              System.out.println( " -- in the same folder of the ontology                                  ") ;
             return SPARQL_SEARCH_QUERY ;
          }
          
          return new String( Files.readAllBytes( Paths.get( directory + File.separator + SPARQL_FILE_NAME  ))) ;
          
        } catch( Exception ex )  {
            ex.printStackTrace() ;
        }
        
        System.out.println( "                                  ") ;
        System.out.println( " Use Internal Default SPAQL Query ") ;
        System.out.println( "                                  ") ;
        System.out.println( SPARQL_SEARCH_QUERY                 ) ;
        System.out.println( "                                  ") ;
        System.out.println( " -- To override this query, create file named [ sparql_validation.txt ] ") ;
        System.out.println( " -- in the same folder of the ontology                                  ") ;
        return SPARQL_SEARCH_QUERY ;
    }
    
    private static void treatColumn( List<String> ontologiesKey           ,
                                     Map<Integer, String> treatedColumns  , 
                                     int numLine                          ,
                                     int columnNumber                     , 
                                     String line                          , 
                                     String csv_separator                 , 
                                     List<String> separators              ,  
                                     String _ontologiesLocation        )  {    
                
      for ( String ontoKey : ontologiesKey )  {
                             
        String directory     = null ;
        String SPARQL_SEARCH = SPARQL_SEARCH_QUERY ;
                            
        try {
            directory = Writer.checkAndBuildDirectoryPath( _ontologiesLocation, ontoKey ) ;
            Writer.checkNotEmptyDirectory( directory ) ;           
            SPARQL_SEARCH = checkExternalQuery( directory ) ;
            
        } catch( Exception ex ) {
            System.out.println("      ")  ;
            System.out.println( ex.getMessage());
            System.out.println( " --> Line [ " + numLine + " ] // Column [ " + columnNumber + " ]" ) ;
            System.out.println("      ")  ;
            System.exit(2) ;
        }
                            
        loadAndSaveOntology( directory ,
                             ontoKey   ,
                             true      ) ;
         
        String columnContent =  line.replaceAll(" +", " " )
                                    .split(csv_separator)[columnNumber]  ;
        try {

          /* Ingore Empty Columns -> Valide */
                          
          if( columnContent.isEmpty()) return  ;
      
          String parser = getContainedSeparator( columnContent, separators ) ;

          if( parser != null && ! parser.isEmpty() ) {
                                                            
              String[] subColumnVariables = columnContent.trim().split(parser) ;
                              
              for( String var : subColumnVariables ) {
                               
                  String treatLabel = treatLabel( ontoKey                  , 
                                                  SPARQL_SEARCH      ,
                                                  var.trim().replaceAll(" +", " ") ,
                                                  getVariables( SPARQL_SEARCH ) )  ; 
                  
                  if( ! okResult( ontologiesKey.indexOf(ontoKey) == ontologiesKey.size() -1 , 
                                  var                                                       , 
                                  treatLabel                                                ,
                                  numLine                                                   , 
                                  columnNumber ))                                           {
                            
                            treatedColumns.put(  columnNumber , null )                      ;
                  }
                  else {
                         final String lm = treatLabel ;
                         treatedColumns.computeIfPresent( columnNumber , 
                                                         (k,v ) -> v + " " + parser + " " + lm ) ;
                         treatedColumns.computeIfAbsent( columnNumber , key -> lm )              ;  
                  }
            }
          }   
          else {

              String treatLabel = treatLabel( ontoKey ,
                                              SPARQL_SEARCH        ,
                                              columnContent.trim().replaceAll(" +", " ")  ,
                                              getVariables(SPARQL_SEARCH ) )              ; 
                                                  
                  if( ! okResult( ontologiesKey.indexOf(ontoKey) == ontologiesKey.size() -1 ,
                                  columnContent                                             , 
                                  treatLabel                                                ,
                                  numLine                                                   ,
                                  columnNumber ))                                           {
                      
                      treatedColumns.put(  columnNumber , null )      ;
                      
                   } else {
                      treatedColumns.put( columnNumber , treatLabel ) ;   
                  }                         
          }
        } catch (Exception ex) {
         Logger.getLogger(CustomPrefixer.class.getName()).log(Level.SEVERE, null, ex) ;
        }
            
        if (   treatedColumns.get(columnNumber) != null                &&
             ! treatedColumns.get(columnNumber).contains(EMPTY_RESULT) &&
             ! treatedColumns.get(columnNumber).contains(NULL_RESULT) ) {
            break ;
        }
      } ;
    }
    
    private static Map<Integer, List<String>> extractOntoByColumn ( String formula ) {
     
       Map<Integer, List<String>> map = new HashMap<>()                ;
      
       String[] sub_formula = formula.replaceAll(" +", "").split("/")  ;
       
       Arrays.asList(sub_formula).forEach( (String line) ->  {
           
           int collumn = Integer.parseInt( line.trim()
                                .split(Pattern.quote("["))[0]
                                .replaceAll(" +", " ")
                                .trim())  ;
           
           List<String> asList = Arrays.asList(line.trim()
                                       .replaceAll(" +", "")
                                       .split(Pattern.quote("["))[1]
                                       .replace("]", "").trim()
                                       .split(",")) ;
               
           map.put(collumn, asList) ;
                
           } ) ;
       
       return map ;
      
    }
    
    /* ENTRY MAIN */ 
    public static void main(String[] args) throws IOException, Exception  {

        List<String> separators    = new ArrayList<>() ;
                
        String  csv_separator      = null  ;       
        String  outCsvFile         = null  ;
        String  inCsvFile          = null  ;
        String  log                = null  ;
        Integer columnFormulaNum   = null  ;
        
        String ontologiesLocation  = null  ;
        
        for (int i = 0; i < args.length; i++) {

            String token = args[i] ;

            switch (token) {

                case "-csv" :
                    inCsvFile = args[i + 1]         ;
                    break ;                    
                case "-outCsv" :
                    outCsvFile = args[i + 1]        ;
                    break ;            
                case "-csv_sep" :
                    csv_separator = args[i + 1]     ;
                    break ;              
                case "-separator" :
                    separators.add(args[i + 1])     ;
                    break ;            
                case "-log" :
                    log = args[i+1]                 ;
                     break ;              
                case "-ontologiesLocation" :
                     ontologiesLocation = args[i+1] ;
                     break ;               
                case "-columnFormulaNum" :
                     columnFormulaNum = Integer.parseInt(args[i+1].trim()) ;
                     break ;               
            }
        }

        Objects.requireNonNull(ontologiesLocation) ;
        Objects.requireNonNull(inCsvFile)          ;
        Objects.requireNonNull(outCsvFile)         ;
        Objects.requireNonNull(csv_separator)      ;
            
        System.out.println(" -------------------------- "                  ) ;
        System.out.println(" + Info : "                                    ) ;
        System.out.println("   CSV         :        " + inCsvFile          ) ;
        System.out.println("   CSV OUT     :        " + outCsvFile         ) ;
        System.out.println("   OntologiesLocation : " + ontologiesLocation ) ;
        System.out.println("   CSV_SEP     :        " + csv_separator      ) ;
        System.out.println("   Separators  :        " + separators         ) ;
        System.out.println("   Column Formula  :    " + columnFormulaNum   ) ;
        System.out.println(" -------------------------- "                  ) ; 
        
        if( ! Writer.existDirectory(ontologiesLocation ) )        {
            printFileNotFoundExceptionAndExit(ontologiesLocation) ;
        }
        
        if( ! Writer.existFile( inCsvFile ) )            {
            printFileNotFoundExceptionAndExit(inCsvFile) ;
        }
        
        if( ! Writer.existFile( inCsvFile ) )            {
            printFileNotFoundExceptionAndExit(inCsvFile) ;
        }
        
        System.setProperty("log", ( log == null || log.isEmpty() ) ? 
                                    "coreseLogs/logs.log" : log )  ;
         
        String _csv_separator      =  csv_separator      ;
        int    _columnFormulaNum   =  columnFormulaNum   ;
      
        String _ontologiesLocation =   ontologiesLocation.endsWith("/") ? 
                                       ontologiesLocation.substring(0,ontologiesLocation.length() - 1) :
                                       ontologiesLocation                                              ;
        
        Writer.deleteFile(outCsvFile) ;
        
        /* Load Graph */
         
        final String _outCsvFile       = outCsvFile         ;
       
        List<String> collectedLines    = new ArrayList<>()  ;
           
        try ( Stream<String> lines = Files.lines(Paths.get(inCsvFile)).skip(1)) {
                
            lines.forEach (new Consumer<String>() {
                    
              int numLine = 2 ; /* Ingore Header and Count from 1 */
                 
              @Override
              public void accept(String line) {
              
                String syntaxe = line.replaceAll(" +", " " )
                                     .split(_csv_separator)[_columnFormulaNum]             ;
                
                Map<Integer, List<String>> ontoByColumnMap = extractOntoByColumn(syntaxe)  ;

                Map<Integer, String> treatedColumns = new HashMap<>() ;
                
                ontoByColumnMap.entrySet().stream().forEach(  (Map.Entry<Integer, List<String>> entry) -> {

                    int columnNum           = entry.getKey()   ;
                    List<String> ontologies = entry.getValue().stream()
                                                              .distinct()
                                                              .collect(Collectors.toList()) ;
      
                    treatColumn( ontologies                     ,
                                 treatedColumns                 , 
                                 numLine                        , 
                                 columnNum                      , 
                                 line                           ,
                                 _csv_separator                 ,
                                 separators                     , 
                                 _ontologiesLocation            ) ;
                 }) ;
                
                 String[] splited = line.replaceAll(" +", " " ).split(_csv_separator) ;
                    
                 if( ! treatedColumns.values().contains(null) ) {
                 
                     String collectLine = IntStream.range(0, splited.length)
                                                   .mapToObj( i ->   { 
                                                                return treatedColumns.get(i) != null ? 
                                                                treatedColumns.get(i) : splited[i]   ;
                                                    })
                                                      .collect(Collectors.joining(_csv_separator))    ;
                     collectedLines.add(collectLine) ;
                 }
                    
               numLine ++ ;                          
              }                       

            }) ;
       }
       
       if( ! collectedLines.isEmpty() &&  
           ( Files.lines( Paths.get(inCsvFile)).count() -1 ) == collectedLines.size() ) {
        
           Writer.checkFile( _outCsvFile                                     ) ;
           String header = Files.lines(Paths.get(inCsvFile)).findFirst().get() ;         
           Writer.writeTextFile(Arrays.asList(header), _outCsvFile           ) ;
         
           Writer.writeTextFile(collectedLines, _outCsvFile                  ) ;
           System.out.println( "                                           " ) ;
           System.out.println( " CSV Generated at path :"  + _outCsvFile     ) ;
           System.out.println( "                                           " ) ;
       }
       else {
           System.out.println( " No CSV Generated ! " ) ; 
           System.out.println( "                    " ) ;
       }
       
    }
}

