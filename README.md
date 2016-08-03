

# CoreseInfer

inferred triples Generator

Steps : 

 **1-** `mvn clean install assembly:single `

 **2- Arguments :**
 
    - owl : owl path file.  ( Required if -nt is Empty ) 
    
    - ttl : turtle path file.  ( Required if -owl is Empty ) 
    
    - out : output path file.  ( Required )
    
    - q   : Sparql Query.  ( Required )
    
    - f   : Fragment ( nbr triples by file )  if = 0 no fragmentation. ( Required )
    
    - F   : output Format ( ttl, xml, csv ). ( Required )
    
    - e   : enable entailment if "t" , default FALSE. ( not Required ).
    
  **3- Example :**
  
 ```
❯     java -Xms1024M -Xmx2048M -cp CoreseInferMaven-1.0.0-jar-with-dependencies.jar corese.Main \

      -owl root-ontology.owl -ttl pools.rdf -q " SELECT DISTINCT ?S ?P ?O { ?S ?P ?O } "        \
      
      -out out/coreseInferedTriples.ttl -f 100000 -F ttl                                        \
      
      -q " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>                                 \
      
           PREFIX : <http://www.anaee/fr/soere/ola#>                                            \
           
           PREFIX oboe-core: <http://ecoinformatics.org/oboe/oboe.1.0/oboe-core.owl#>           \
           
           SELECT ?uriVariableSynthesis ?measu ?value  {                                        \
           
           ?uriVariableSynthesis a oboe-core:Observation ;                                      \
           
           oboe-core:ofEntity :VariableSynthesis ; oboe-core:hasMeasurement ?measu .            \
           
           ?measu oboe-core:hasValue ?value . Filter ( regex( ?value, 'ph', 'i')) } "           \
           
           -out out/portail/coreseInferedTriples.ttl -f 0 -F xml                                \
           
      -e

```
