
package corese.sparql ;

import corese.Writer ;
import java.util.List ;
import java.util.Arrays ;
import java.util.Objects ;
import java.io.IOException ;
import java.nio.file.Files ;
import java.nio.file.Paths ;
import java.util.ArrayList ;
import java.util.StringJoiner ;
import java.util.regex.Matcher ;
import java.util.regex.Pattern ;
import java.util.stream.Stream ;
import java.util.stream.Collectors ;


/**
 *
 * @author ryahiaoui
 */

/**
 * Exemple : 
 *  -queryPath  "./sparqlValidator.txt"
 *  -selectVars "site, ecotype"
 *  -filter     "year:=1900_null_close"
 *  -filter     "site:=paris, Nancy"
 *  @
 *  SPARQL QUERY :
 *   - One variable per LINE in the SELECT QUERY
 *   - One variable per LINE in the GROUP BY
 */

public class SparqlTemplate {
 
    static final String FILTERS_TRACE  = " \n ### FILTER_TRACE ### \n\n" ;
    final static String OPERATOR       = ":="                            ;
    final static String RANGE_DATE_SEP = "_"                             ;
    final static String VAR_SEP        = ";"                             ;
    final static String INTRA_VAR_SEP  = ","                             ;
    final static String NULL_DATE      = "null"                          ;
    final static String CLOSE_RANGE    = "close"                         ;
        
    public static void main(String[] args) throws IOException {
             
       List<String> filters     = new ArrayList<>() ;
       String       queryPath   = null              ;
       String       selectVars  = null              ;
       String       outQuery    = null              ;
        
       for (int i = 0; i < args.length; i++) {

          String token = args[i] ;

          switch (token) {

              case "-queryPath" :
                  queryPath = args[i + 1].trim()   ;
                  break                    ;
              case "-selectVars" :
                  selectVars = args[i + 1].trim()  ;
                  break
              case "-outQuery" :
                  outQuery = args[i + 1].trim()    ;
                  break                            ;
              case "-filters" :
                  filters.addAll (
                     arr( args [ i+1 ].trim() ) )  ;
                  break                            ;
          }
       }
           
     Objects.requireNonNull( queryPath , selectVars ) ;
     
     String query = new String(Files.readAllBytes(Paths.get(queryPath))) ;
     
     query        = cleanAndAddTraceFilter(query)    ;     
     
     List<String> listVars = varToList( selectVars ) ;
     
     for( String var : listVars ) {
        query = unCommentSelectVariable( query, var ) ;
     }
              
     for ( String filter : filters ) {

         if( filter.contains(RANGE_DATE_SEP) ) {
             List list = parsIntervalFilter(filter) ;
             query = addFilterVariable( query                 , 
                                        (String)  list.get(0) , 
                                        (Integer) list.get(1) , 
                                        (Integer) list.get(2) ,
                                        (String)  list.get(3) ) ;
         } 
         else {
              List list   = parsListFilter(filter) ;
              String var  = (String) list.get(0)   ;
              List values = (List) list.get(1)     ;
              query = addFilterVariable( query , var, values) ;
         }
     }
     
     if( ( outQuery == null )  ||
         ( outQuery != null && outQuery.trim().equalsIgnoreCase("console") ) ) {
         System.out.println("                        ") ;
         System.out.println(" Query ****************" ) ;
         System.out.println("                        ") ;
         System.out.println( query) ;
         System.out.println("                        ") ;
         System.out.println(" **********************" ) ;
         System.out.println("                        ") ;
     }
     else {
         
         Writer.checkFile( outQuery )                           ;
         
         System.out.println("                               " ) ;
         System.out.println(" Save Query                    " ) ;
         Writer.writeTextFile(Arrays.asList(query), outQuery  ) ;
         System.out.println("                               " ) ;
         System.out.println(" Location : " + outQuery         ) ;
         System.out.println("                               " ) ;
     }

    }

    private static String unCommentSelectVariable( String query , String variable ) { 
        
     // Pattern p = Pattern.compile( "#( *\\?" + variable.trim() + " ) *\n" ,
     //                              Pattern.CASE_INSENSITIVE )             ;
     
      Pattern p = Pattern.compile( "\\s*#\\s*\\?" + variable + "\\s*\n"  ,
                                   Pattern.CASE_INSENSITIVE )            ;
     
      Matcher m = p.matcher(query )            ;
      String _query = query                    ;
      List<String> changed = new ArrayList<>() ;
      
      while ( m.find() ) {
         String found = m.group() ;
         if( !changed.contains(found)) {
           _query = _query.replace( found, found.replaceFirst("#", "") ) ;
           changed.add(found) ;
         }
      } 
      return _query ;
    }
    
    private static String addFilterVariable( String query        , 
                                             String variable     , 
                                             List<String> values ) {
        
        StringJoiner sj = new StringJoiner(" || ") ;

        values.forEach( value ->  {
          sj.add( "str(?" + variable + ") = \"" + value +"\"" ) ;
        }) ;
       return  query.replace( FILTERS_TRACE ,  
                              " \n FILTER ( " + sj.toString() + 
                              " ) . "                         + 
                              FILTERS_TRACE )                 ;
    }
    
    private static String addFilterVariable( String  query    , 
                                             String  variable , 
                                             Integer min      , 
                                             Integer max      , 
                                             String  close    ) {
        
        String maxComparator = " < " ;
        String minComparator = " > " ;
        
        if( close != null && 
            close.trim().equalsIgnoreCase(CLOSE_RANGE)) {
          maxComparator = " <= " ;
          minComparator = " >= " ;
        }
        
        if( max == null ) {
            if( min != null ) {
            return query.replace( FILTERS_TRACE,  " \n FILTER ( "       +  
                                  "?" + variable + minComparator + min  + 
                                 " ) . " + FILTERS_TRACE )              ;
            }
            
            return query ;
        }
        else if ( min == null ) {
        
          return query.replace( FILTERS_TRACE,  " \n FILTER ( " +  "?" + 
                                variable + maxComparator  +  max       +
                                " ) . " + FILTERS_TRACE )              ;
        }
        
        StringJoiner sj = new StringJoiner(" && ")       ;
        sj.add( "?" + variable + minComparator + min  )  ;
        sj.add( "?" + variable + maxComparator + max  )  ;
        
       return  query.replace( FILTERS_TRACE,  " \n FILTER ( "           + 
                              sj.toString() + " ) . " + FILTERS_TRACE ) ;
    }
    
    private static String cleanAndAddTraceFilter( String content ) {
      return addFilterTrace ( content.replaceAll("\t", " "     )
                                     .replaceAll("\n", " \n  " )
                                     .replaceAll(" +", " "     ) ) ; 
    }
    
    private static String addFilterTrace( String query ) {
        
      if( query.toUpperCase().contains("GROUP BY"))      {
          
          String before = query.split("(?i)GROUP BY")[0] ;
          String after  = query.split("(?i)GROUP BY")[1] ;
          return before.substring( 0 , before.lastIndexOf("}")) + 
                                   FILTERS_TRACE                +
                                   " } \n GROUP BY \n "         + 
                                   after                        ;
      }
      else {
          return query.substring( 0, query.lastIndexOf("}")) + 
                                  FILTERS_TRACE + " } \n "   ;
      }
    }
    
    private static  List parsListFilter( String arg )            {
    
      String variable     = arg.trim().split(OPERATOR)[0].trim() ;
      
      List<String> values = Stream.of(arg.trim().split(OPERATOR)[1]
                                                .trim().split(INTRA_VAR_SEP))
                                  .map( v -> v.trim() )
                                  .collect(Collectors.toList()) ;
                                  
      return Arrays.asList(variable, values) ;
      
    }
    
    private static List parsIntervalFilter( String arg )      {

      String variable = arg.trim().split(OPERATOR)[0].trim()  ;
      
      Integer min  = arg.trim().split(OPERATOR)[1].trim()
                        .split(RANGE_DATE_SEP)[0].equalsIgnoreCase(NULL_DATE) ? 
                     null :
                     Integer.parseInt(arg.trim().split(OPERATOR)[1]
                            .trim().split(RANGE_DATE_SEP)[0]) ;
    
      Integer max  = arg.trim().split(OPERATOR)[1].trim()
                        .split(RANGE_DATE_SEP)[1].equalsIgnoreCase(NULL_DATE) ? 
                     null :
                     Integer.parseInt(arg.trim().split(OPERATOR)[1]
                            .trim().split(RANGE_DATE_SEP)[1]) ;
      
      String close = arg.trim().split(OPERATOR)[1]
                               .trim().split(RANGE_DATE_SEP)[2] ;
      
      return Arrays.asList(variable, min, max, close)           ;
    }
    
    private static List varToList ( String arg ) {
     return  Stream.of(arg.trim().trim().split(INTRA_VAR_SEP))
                   .map( v -> v.trim() )
                   .collect(Collectors.toList()) ;
    }
 
    private static List<String> arr ( String param )     {        
     return Arrays.asList( param.trim()
                                .replaceAll ( " + ", " " )
                                .split(VAR_SEP))         ;
   }
   
}


/*
    # SPARQL Exemple
    # SPARQL TEMPLATE QUERY 
    # ONE VARIABLE PER LINE IN SELECT CLAUSE
    # ONE VARIABLE PER LINE IN GROUP BY CLAUSE
      
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
      
    SELECT 
         #  ?infra
         #  ?infraName
    
    WHERE  {

           ......
    }

    GROUP BY 
    
          #  ?infra
          #  ?infraName
*/

