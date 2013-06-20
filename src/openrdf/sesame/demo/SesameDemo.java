package openrdf.sesame.demo;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.http.HTTPRepository;

/**
 *
 * @author Leandro Ordonez <leandro.ordone.ante@gmail.com>
 */
public class SesameDemo {
    
    public static void issueTupleQuery(Repository repo, String queryString) {
        try {
                RepositoryConnection con = repo.getConnection();
                try {
                    TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

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

    public static void main(String[] args) {
        try {
            String sesameServer = "http://localhost:8080/openrdf-sesame/";
            String repositoryID = "WebAPIModel";
            Repository repo = new HTTPRepository(sesameServer, repositoryID);
            repo.initialize();
            System.out.println("Ready!");
            String queryString = "SELECT ?x ?p ?y WHERE { ?x ?p ?y } ";
            SesameDemo.issueTupleQuery(repo, queryString);
        } catch (RepositoryException ex) {
            Logger.getLogger(SesameDemo.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            System.exit(0);
        }
    }
}
