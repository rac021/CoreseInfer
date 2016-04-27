
# CoreseInfer

inferred triples Generator ( using OWL + N-Triples input )

Steps : 
 1- mvn clean install 
 2- Args :
    - owl : owl path file 
    - nt  : N-triple path file 
    - out : output path file  ( Required )
    - q   : Sparql Query  ( Required )
    - f   : Fragment ( nbr triples by file )  if = 0 no fragmentation. ( Required )
    - ilv : if "t" : ignore All literal values ( in Subject ) , else "f" ( false )  ( Required )
    - F  : output FOrmat ( n3, xml ) ( Required )
    - e  : enable entailment if "t" , default FALSE ( not Required ).
  3- Exp :
      java -Xms1024M -Xmx2048M -cp lib/ontop-materializer-1.16.jar ontop.Main            \
      -owl root-ontology.owl -nt pools.rdf -q " SELECT DISTINCT ?S ?P ?O { ?S ?P ?O } "  \ 
      -out out/coreseInferedTriples.nt -f 100000 -ilv t -F n3                            \
      -q " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>                          \ 
           PREFIX : <http://www.anaee/fr/soere/ola#>                                     \ 
           PREFIX oboe-core: <http://ecoinformatics.org/oboe/oboe.1.0/oboe-core.owl#>    \ 
           SELECT ?uriVariableSynthesis ?measu ?value  {                                 \ 
           ?uriVariableSynthesis a oboe-core:Observation ;                               \  
           oboe-core:ofEntity :VariableSynthesis ; oboe-core:hasMeasurement ?measu .     \ 
           ?measu oboe-core:hasValue ?value . Filter ( regex( ?value, 'ph', 'i')) } "    \
           -out out/portail/coreseInferedTriples.nt -f 0 -ilv t -F xml                   \
      -e
      
