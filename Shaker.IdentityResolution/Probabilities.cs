using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Reflection;
using VLDS.Shaker.DatabaseModel;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace VLDS.Shaker.IdentityResolution
{
    public class Probabilities
    {
        private IShakerDatabaseFacade shakerDb;

        public Probabilities(IShakerDatabaseFacade shakerDb)
        {
            if (shakerDb == null)
                throw new ArgumentNullException("shakerDb");

            this.shakerDb = shakerDb;
        }

        //Probability of a match between two records
        private static double p_match(List<column_match_m_u> matchVector, int E, int N1, int N2)
        {
            double p = 0.00;
            double x_rand = Probabilities.x_matchRandomly(E, N1, N2);
            double product_x_sub_i = x_rand;

            foreach (column_match_m_u matchColumn in matchVector)
            {  
                if (matchColumn.match > 1)
                    product_x_sub_i = product_x_sub_i * 1;
                else if (matchColumn.match > 0.00)
                    product_x_sub_i = product_x_sub_i * x_agree(matchColumn.m_prob, matchColumn.u_prob);
                else if (matchColumn.match <= 0.00)
                    product_x_sub_i = product_x_sub_i * x_disagree(matchColumn.m_prob, matchColumn.u_prob);
            }

            p = product_x_sub_i / (product_x_sub_i + 1);
            return p;
        }
        
        private static double x_matchRandomly(int E, int N1, int N2)
        {
            double x_return = E / (float)((N1 * N2) - E);
            return x_return;
        }

        private static double x_agree(double m, double u)
        {
            double x_return = m / u;
            return x_return;
        }

        private static double x_disagree(double m, double u)
        {
            double x_return = (1 - m) / (1 - u);
            return x_return;
        }

        private static List<column_match_m_u> transposeBlockMatch(BLOCKING_MATCH blockMatch, List<string> matchColumns, IDictionary<String,PARAMETER> parameters)
        {
            
            List<column_match_m_u> colMatchMU = new List<column_match_m_u>();
            
            foreach (string colName in matchColumns)
            {
                column_match_m_u newMMU = new column_match_m_u();
                newMMU.columnName = colName;
                PropertyInfo pI = blockMatch.GetType().GetProperty(colName);
                int val = (int)pI.GetValue(blockMatch, null);
                newMMU.match = (double)val;
                newMMU.m_prob = parameters[colName].COLUMN_M.Value;
                newMMU.u_prob = parameters[colName].COLUMN_U.Value;
                colMatchMU.Add(newMMU);
                
            }
            
            return colMatchMU;
        }

        public List<PROBABILITY> GetProbabilities(BLOCKING_SCHEME blockingScheme, List<BLOCKING_MATCH> BlockMatches, List<string> matchColumns, IDictionary<string,PARAMETER> parameters)
        {
            //List<PROBABILITY> probs = new List<PROBABILITY>();
            ConcurrentBag<PROBABILITY> probs = new ConcurrentBag<PROBABILITY>();

            DateTime now = DateTime.Now;

            int identifiers1Count = shakerDb.GetTableSize("SHAKER.IDENTIFIERS_1");
            int identifiers2Count = shakerDb.GetTableSize("SHAKER.IDENTIFIERS_2");
            int E;
            if (identifiers1Count > identifiers2Count)
                E = identifiers2Count;
            else
                E = identifiers1Count;

            //foreach (BLOCKING_MATCH blockMatch in BlockMatches)
            Parallel.ForEach(BlockMatches, blockMatch =>
            {
                List<column_match_m_u> matchVector = transposeBlockMatch(blockMatch, matchColumns, parameters);
                double probability = p_match(matchVector, E, identifiers1Count, identifiers2Count);

                PROBABILITY matchProb = new PROBABILITY();
                matchProb.PROB = (float)probability;
                matchProb.MATCH_TYPE_CODE = "P";
                matchProb.BLOCKING_SCHEME_ID = blockingScheme.BLOCKING_SCHEME_ID;
                matchProb.CREATED_DATE = now;

                if (blockingScheme.BLOCKING_SCHEME_TYPE == "PERSON")
                {
                    matchProb.UNIQUE_ENTITY_ID_1 = blockMatch.PERSON_UNIQUE_ENTITY_ID_1;
                    matchProb.UNIQUE_ENTITY_ID_2 = blockMatch.PERSON_UNIQUE_ENTITY_ID_2;
                }
                else
                {
                    matchProb.UNIQUE_ENTITY_ID_1 = blockMatch.PLACE_UNIQUE_ENTITY_ID_1;
                    matchProb.UNIQUE_ENTITY_ID_2 = blockMatch.PLACE_UNIQUE_ENTITY_ID_2;
                }

                probs.Add(matchProb);

            });

            return probs.ToList<PROBABILITY>();
        }
    }
}