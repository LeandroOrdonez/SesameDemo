package openrdf.sesame.demo;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.OpenRDFException;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.http.HTTPRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;

/**
 *
 * @author Leandro Ordonez <leandro.ordone.ante@gmail.com>
 */
public class SesameDemo {

    public static void issueTupleQuery(Repository repo, String queryString, FileOutputStream fileOut) {
        try {
            RepositoryConnection con = repo.getConnection();
            try {
                TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
                if (fileOut != null) {
                    SPARQLResultsXMLWriter sparklWriter = new SPARQLResultsXMLWriter(fileOut);
                    tupleQuery.evaluate(sparklWriter);
                } else {
                    TupleQueryResult result = tupleQuery.evaluate();
                    try {
                        List<String> bindingNames = result.getBindingNames();
                        while (result.hasNext()) {
                            BindingSet bindingSet = result.next();
                            Value valueOfX = bindingSet.getValue(bindingNames.get(0));
                            Value valueOfP = bindingSet.getValue(bindingNames.get(1));
                            Value valueOfY = bindingSet.getValue(bindingNames.get(2));
                            // do something interesting with the values here...
                            System.out.println(bindingNames.get(0) + ": " + valueOfX.stringValue());
                            System.out.println(bindingNames.get(1) + ": " + valueOfP.stringValue());
                            System.out.println(bindingNames.get(2) + ": " + valueOfY.stringValue());
                            //-------------------------------------------------
                        }
                    } catch (Exception ex) {
                        Logger.getLogger(SesameDemo.class.getName()).log(Level.SEVERE, null, ex);
                    } finally {
                        result.close();
                    }
                }
            } catch (MalformedQueryException ex) {
                Logger.getLogger(SesameDemo.class.getName()).log(Level.SEVERE, null, ex);
            } catch (QueryEvaluationException ex) {
                Logger.getLogger(SesameDemo.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                con.close();
            }
        } catch (OpenRDFException e) {
            // handle exception
        }
    }

    public static void issueGraphQuery(Repository repo, String queryString, FileOutputStream fileOut) {
        try {
            RepositoryConnection con = repo.getConnection();
            try {
                RDFWriter writer = (fileOut != null) ? Rio.createWriter(RDFFormat.RDFXML, fileOut) : Rio.createWriter(RDFFormat.RDFXML, System.out);
                con.prepareGraphQuery(QueryLanguage.SPARQL, queryString).evaluate(writer);
            } catch (RepositoryException ex) {
                Logger.getLogger(SesameDemo.class.getName()).log(Level.SEVERE, null, ex);
            } catch (MalformedQueryException ex) {
                Logger.getLogger(SesameDemo.class.getName()).log(Level.SEVERE, null, ex);
            } catch (QueryEvaluationException ex) {
                Logger.getLogger(SesameDemo.class.getName()).log(Level.SEVERE, null, ex);
            } catch (RDFHandlerException ex) {
                Logger.getLogger(SesameDemo.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                con.close();
            }
        } catch (RepositoryException ex) {
            Logger.getLogger(SesameDemo.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void addStatementTest(Repository repo) {
        try {
            ValueFactory factory = repo.getValueFactory();
            //create some resources and literals to make statements out of them
            //-- Classes
            URI documentClass = factory.createURI("http://www.example.org/category-structure.rdf#Document");
            URI categoryClass = factory.createURI("http://www.example.org/category-structure.rdf#Category");
            URI membershipRelClass = factory.createURI("http://www.example.org/category-structure.rdf#Membership_Relation");
            URI termRelClass = factory.createURI("http://www.example.org/category-structure.rdf#Term_Relation");
            URI termClass = factory.createURI("http://www.example.org/category-structure.rdf#Term");

            //-- Instances
            URI document1 = factory.createURI("http://www.example.org/documents/1");
            URI category3 = factory.createURI("http://www.example.org/categories/3");
            URI membershipRel3 = factory.createURI("http://www.example.org/membership_relations/3");
            URI termRel3 = factory.createURI("http://www.example.org/term_relations/3");
            URI term1 = factory.createURI("http://www.example.org/terms/1");

            //-- Properties
            URI is_member_of = factory.createURI("http://www.example.org/category-structure.rdf#is_member_of");
            URI category_value = factory.createURI("http://www.example.org/category-structure.rdf#category_value");
            URI membership_probability = factory.createURI("http://www.example.org/category-structure.rdf#membership_probability");
            URI has_term = factory.createURI("http://www.example.org/category-structure.rdf#has_term");
            URI term_value = factory.createURI("http://www.example.org/category-structure.rdf#term_value");
            URI term_probability = factory.createURI("http://www.example.org/category-structure.rdf#term_probability");
            URI has_content = factory.createURI("http://www.example.org/category-structure.rdf#has_content");

            //-- Literals
            //Literal membership_probability_literal = factory.createLiteral(0.7);
            //Literal term_probability_literal = factory.createLiteral(0.4);
            
            RepositoryConnection con = repo.getConnection();
            try{
                con.add(document1, RDF.TYPE, documentClass);
                con.add(document1, is_member_of, membershipRel3);
                con.add(category3, RDF.TYPE, categoryClass);
                con.add(category3, has_term, termRel3);
                con.add(membershipRel3, RDF.TYPE, membershipRelClass);
                con.add(membershipRel3, category_value, category3);
                con.add(membershipRel3, membership_probability, factory.createLiteral(0.55));
                con.add(termRel3, RDF.TYPE, termRelClass);
                con.add(termRel3, term_value, term1);
                con.add(termRel3, term_probability, factory.createLiteral(0.7));                
                con.add(term1, RDF.TYPE, termClass);
                con.add(term1, has_content, factory.createLiteral("HolaMundo!!!"));
                
            } finally {
                con.close();
            }
        } catch (RepositoryException ex) {
            Logger.getLogger(SesameDemo.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void main(String[] args) {
        try {
            String sesameServer = "http://localhost:8080/openrdf-sesame/";
            String repositoryID = "WebAPIModel";
            Repository repo = new HTTPRepository(sesameServer, repositoryID);
            repo.initialize();
//            System.out.println("Done!");

            //--------------------- Tuple Query ---------------------------
            String queryString1 = "SELECT ?x ?p ?y WHERE { ?x ?p ?y } ";
            //Print the results from issuing the query in System console.
            SesameDemo.issueTupleQuery(repo, queryString1, null);
            //Save the results from issuing the query into a file.
            FileOutputStream out1 = new FileOutputStream("src/outcome/tuple-query-result.srx");
            SesameDemo.issueTupleQuery(repo, queryString1, out1);

            //--------------------- Graph Query ---------------------------
            String queryString2 = "CONSTRUCT { ?s ?p ?o } WHERE {?s ?p ?o }";
            //Print the results from issuing the query in System console.
            SesameDemo.issueGraphQuery(repo, queryString2, null);
            //Save the results from issuing the query into a file.
            FileOutputStream out2 = new FileOutputStream("src/outcome/graph-query-result.srx");
            SesameDemo.issueGraphQuery(repo, queryString2, out2);
            
            //--------------- Adding some statements ----------------------
            addStatementTest(repo);

            //Shutdown the repository when all the operations have been done
            repo.shutDown();

        } catch (RepositoryException ex) {
            Logger.getLogger(SesameDemo.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(SesameDemo.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
//            System.exit(0);
        }
    }
}
