
package corese.csv ;

import corese.Main ;
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
import fr.inria.edelweiss.kgtool.load.Load ;
import fr.inria.acacia.corese.api.IDatatype ;
import fr.inria.edelweiss.kgraph.core.Graph ;
import fr.inria.edelweiss.kgram.core.Mapping ;
import fr.inria.edelweiss.kgram.core.Mappings ;
import static java.util.stream.Collectors.toSet;
import fr.inria.edelweiss.kgraph.rule.RuleEngine ;
import fr.inria.edelweiss.kgraph.query.QueryProcess ;
import fr.inria.acacia.corese.exceptions.EngineException ;

/**
 *
 * @author ryahiaoui
 */
public class Prefixer {
        
    private static final String 
            
       sparql =    "  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  \n " +
                   "  SELECT DISTINCT ?URI ?CLAZZ {                         \n " +
                   "    ?URI rdfs:label ?LABEL                              \n " +
                   "    FILTER REGEX( STR(?LABEL), \"^^{0}$\", \"i\")       \n " +
                   "    BIND ( STRAFTER ( STR(?URI) , \"#\" ) AS ?CLAZZ )   \n " +
                   " }"  ;
    
    private static volatile Prefixer _instance = null ;
     
    private static Graph        g   ;
    private static Load         ld  ;
    private static RuleEngine   re  ;

    private static String getContainedSeparator(String label, List<String> separators) {
        return separators.stream().filter( string -> label.contains(string)).findFirst().orElse(null);

    }
   
    enum FORMAT { TTL, XML , CSV }  ;
 
    static int loop  = 0            ;
 
    private static final String URI_VALIDATOR = "^((https?|ftp|file)://|(www\\.)|(<_:))[-a-zA-Z0-9+&@#/%?=~_|!:,.;µs%°]*[-a-zA-Z0-9+&@#/%=~_|]" ;
     
    private Prefixer() {}

    public static Prefixer getInstance( String fileToLoad, boolean entailment ) {
             if (_instance == null) {
                    synchronized (Main.class) {
                            if (_instance == null) {
                                    _instance = new Prefixer() ;
                                    initialize (fileToLoad , entailment) ;
                            }
                    }
            }
            return _instance ;
    }
      
    private static void initialize( String fileToLoad, boolean entailment ) {
          
        try {
              g  = Graph.create(entailment) ;             
              ld = Load.create(g)           ;
              loadFile( fileToLoad  )     ;
         }
         catch (Exception ex) {
               ex.printStackTrace() ;
               Throwable[] suppressed = ex.getSuppressed();
               for(Throwable cause : suppressed ) {
                   System.err.println(cause.getMessage()) ;
               }
               System.out.println(" ") ;
         }
    }
 
    private static void loadFile ( String fileToLoad ) {
           
        System.out.println(" Loading file : " + fileToLoad ) ;
        ld.load( fileToLoad )   ;
    }

    
    private List<String> genericRequest(  String  request        ,
                                          List<String> variables ) throws  IOException  {
              
        QueryProcess  exec   =  QueryProcess.create(g) ;
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
                   
                   if ( ! res.isEmpty() ) lines.add(res) ;
               }
                        
        } catch ( EngineException e )          {
             System.out.println ( " -------- " ) ;
             e.printStackTrace()               ;
             System.out.println ( " -------- " ) ;
        }

       return lines ;
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

    private static String readFile( String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path)));
    }
    
    private static Map<String, String> getPrefixes(String prefixFile) throws IOException {
      
      Map<String, String > prefixes = new HashMap<>();
        
      try ( Stream<String> lines = Files.lines(Paths.get(prefixFile))) {
            
           lines.forEach ( line ->    { 
                String[] splited = line.trim().replaceAll(" + ", " ").split(" ") ;
                prefixes.put(splited[2], splited[1]) ;
            }) ;
      }
       
      return prefixes ;
    }
    
    
    private static String extract( Prefixer prefixer      ,
                                   String sparql          ,
                                   String variable        ,
                                   List<String> variables ,
                                   Map<String, String> prefixMap ) throws Exception {

        List<String> result = prefixer.genericRequest( sparql.replace("{0}",variable.trim() ) , variables ) ;

        if(result.size() > 1) {
            Set<String> collect = result.stream()
                                        .map( res -> res.trim().split(" ")[0])
                                        .collect(toSet()) ;
            if(collect.size() > 1 ) {
                System.out.println(" Multiple Uris found for variable : [ "+ variable + " ] ")    ;
                collect.forEach( line -> System.err.println( "  - " + line.trim().split(" ")[0])) ;
                System.out.println(" + Retained --> " + result.get(0).trim().split(" ")[0])       ;
            }
                
        }
    
        
        if(result.isEmpty()) {
              System.out.println(" *** ERROR "                                                ) ;
              System.out.println("     No prefix found for variable : [ " + variable + " ] ! ") ;
              System.out.println("                                                           ") ;
              System.exit(0) ;
        }
         
        if ( result.get(0).split(" ").length >= 2 ) {
            String uri = result.get(0).replace("<", "").replace(">", "").split("#")[0] ;
            String Class = result.get(0).split(" ")[1].split("\\^\\^")[0].replace("\"", "") ;

            String prefix = prefixMap.get( uri + "#" ) ;

            if (prefix == null) {
                System.out.println(" *** ERROR ") ;
                System.out.println("     No prefix assigned to URI : [ " + uri + " ] in the the prefix File ! ") ;
                System.out.println("  ") ;
                System.exit(0);
            }

            return prefix + Class ;
        } else 
        {
            System.out.println(" + Query must Have Only Two VARIABLES ");
            System.out.println(" + You can re test without [ -query ] argument ");
            System.exit(2);
        }
        
        throw new IllegalArgumentException(" Error while extracting Prefix:Class for variable -> "+ variable ) ;
    }
     
    private static String treatLabels( Prefixer            prefixer         ,
                                       String              sparql           ,
                                       List<String>        labels           ,
                                       List<String>        sparqlVariables  , 
                                       Map<String, String> prefixMap        ,
                                       String              parser        )  {
       return labels.stream()
                    .map( label -> treatLabel( prefixer          , 
                                               sparql            , 
                                               label             , 
                                               sparqlVariables   , 
                                               prefixMap)        )
                    .collect( Collectors.joining(parser + " " )) ;
    }
    
    private static String treatLabel(  Prefixer            prefixer         ,
                                       String              sparql           ,
                                       String              label            ,
                                       List<String>        sparqlVariables  , 
                                       Map<String, String> prefixMap     )  {
        try {
            return  extract( prefixer        ,
                             sparql          ,                               
                             label           ,
                             sparqlVariables ,
                             prefixMap )     ;
            
        } catch( Exception ex ) {
            ex.printStackTrace();
        }
        return null ;
    }
    
    
    /* ENTRY MAIN */ 
    public static void main(String[] args) throws IOException, Exception  {

        List<Integer> columns    = new ArrayList<>() ;
        List<String> separators  = new ArrayList<>() ;
                
        String csv_separator  =  null ;
        String ontologyFile   = null  ;
        String outCsvFile     = null  ;
        String prefixFile     = null  ;
        String inCsvFile      = null  ;
        String queryFile      = null  ;
        String log            = null  ;
        
        boolean priority      = false ;
        
        for (int i = 0; i < args.length; i++) {

            String token = args[i] ;

            switch (token) {

                case "-csv" :
                    inCsvFile = args[i + 1] ;
                    break ;                    
                case "-outCsv" :
                    outCsvFile = args[i + 1] ;
                    break ;                    
                case "-prefix" :
                      prefixFile = args[i + 1] ;
                    break ;
                case "-column" :
                    columns.add(Integer.parseInt(args[i + 1])) ;
                    break ;
                case "-csv_sep":
                    csv_separator = args[i + 1] ;
                    break ;              
                case "-separator":
                    separators.add(args[i + 1]) ;
                    break ;              
                case "-ontology" :
                    ontologyFile = args[i + 1] ;
                    break ;
                case "-log"  :
                    log = args[i+1] ;
                     break ;               
                case "-priority"  :
                     priority = true;
                     break ;               
                case "-query "  :
                     queryFile = args[i + 1] ;
                     break ;
            }
        }

       Objects.requireNonNull(ontologyFile)  ;
       Objects.requireNonNull(inCsvFile)     ;
       Objects.requireNonNull(outCsvFile)    ;
       Objects.requireNonNull(prefixFile)    ;
       Objects.requireNonNull(csv_separator) ;
       
       System.out.println(" -------------------------- "     ) ;
       System.out.println(" + Info : "                       ) ;
       System.out.println("   Ontology    : " + ontologyFile ) ;
       System.out.println("   CSV         : " + inCsvFile    ) ;
       System.out.println("   CSV OUT     : " + outCsvFile   ) ;
       System.out.println("   Prefix File : " + prefixFile   ) ;
       System.out.println("   CSV_SEP     : " + csv_separator) ;
       System.out.println("   Separators  : " + separators   ) ;
       System.out.println(" -------------------------- "     ) ;
 
        System.setProperty("log", ( log == null || log.isEmpty() ) ? 
                                    "coreseLogs/logs.log" : log )  ;
          
        String _csv_separator =  csv_separator ;
        String currentSparql   ;
        
        if(queryFile != null ) {
           currentSparql = readFile(queryFile) ;
        }
        else {
           currentSparql = sparql ;
        }
        
        /* Load Graph */
         
        Prefixer prefixer = getInstance( ontologyFile , true )         ;
        
        Map<String, String > prefixMap = getPrefixes( prefixFile )     ;
        
        List<String> sparqlVariables   = getVariables( currentSparql ) ;
     
        final String _outCsvFile       = outCsvFile                    ;
       
        Writer.checkFile( _outCsvFile ) ;
        
         try ( Stream<String> lines = Files.lines(Paths.get(inCsvFile)))  { 
               String header =   lines.findFirst().get()                  ;
               Writer.writeTextFile(Arrays.asList(header), _outCsvFile)   ;
         } ;
        
           
        try ( Stream<String> lines = Files.lines(Paths.get(inCsvFile)).skip(1)) {
                
                lines.forEach ( (String line) -> {
                  
                  Map<Integer, String> treatedColumns = new HashMap<>();
                  
                  columns.forEach( columnNumber ->    {
                    
                    try {
                        
                        String column =  line.replaceAll(" +", " " ).split(_csv_separator)[columnNumber] ;
                        String parser = getContainedSeparator( column, separators ) ;
                        
                        if( parser != null && ! parser.isEmpty() ) {                            
                            treatedColumns.put(  columnNumber , 
                                                 treatLabels( prefixer                            ,
                                                              currentSparql                       ,
                                                              Arrays.asList(column.split(parser)) ,
                                                              sparqlVariables                     , 
                                                              prefixMap                           ,
                                                              parser
                                                           ) 
                                              ) ;
                        }
                        else {
                            
                            treatedColumns.put(  columnNumber , 
                                                 treatLabel( prefixer         ,
                                                             currentSparql    ,
                                                             column           ,
                                                             sparqlVariables  , 
                                                             prefixMap                                       
                                                           )  
                                              ) ;                      
                        }
                        
                     
                    } catch (Exception ex) {
                        Logger.getLogger(Prefixer.class.getName()).log(Level.SEVERE, null, ex);
                    }
                  });
                  
                  String[] splited = line.replaceAll(" +", " " ).split(_csv_separator) ;
                                                     
                  String collect = IntStream.range(0, splited.length)
                                            .mapToObj( i ->   { 
                                                       return treatedColumns.get(i) != null      ? 
                                                              treatedColumns.get(i) : splited[i] ;
                                                      })
                                            .collect(Collectors.joining(_csv_separator)) ;
                    try {
                        Writer.writeTextFile(Arrays.asList(collect), _outCsvFile) ;
                        
                    } catch (IOException ex) {
                        Logger.getLogger(Prefixer.class.getName()).log(Level.SEVERE, null, ex);
                    }
                                                       
                })  ;
       }
    }
   
}
