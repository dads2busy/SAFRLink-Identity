using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using VLDS.Shaker.Common;
using VLDS.Shaker.DatabaseModel;

namespace VLDS.Shaker.IdentityResolution
{
    public class Sample
    {
        private IShakerDatabaseFacade shakerDb;

        public Sample(IShakerDatabaseFacade shakerDb)
        {
            if (shakerDb == null)
                throw new ArgumentNullException("shakerDb");

            this.shakerDb = shakerDb;
        }

        public ICollection<BLOCKING_MATCH> GetIdentifiersSampleJoin(string iDName, float zConfidenceLevel = 1.96F, float eConfidenceError = .05F) // iDName Is Either PERSON_UNIQUE_ENTITY_ID Or PLACE_UNIQUE_ENTITY_ID
        {
            // Get Row Count From Each Identifier Table
            var tableSize1 = shakerDb.GetTableSize("IDENTIFIERS_1");
            var tableSize2 = shakerDb.GetTableSize("IDENTIFIERS_2");

            // Get Required Sample Size
            int sampleSize1 = (int)StatisticsUtils.GetSampleSize((long)tableSize1, zConfidenceLevel, eConfidenceError);
            int sampleSize2 = (int)StatisticsUtils.GetSampleSize((long)tableSize2, zConfidenceLevel, eConfidenceError);

            // Create Enumerated Lists From 1 To Row Count For Each Table
            IList<int> tableList1 = Enumerable.Range(1, tableSize1).ToList();
            IList<int> tableList2 = Enumerable.Range(1, tableSize2).ToList();

            // Get Random Samples From Enumerated Lists
            Random rand = new Random();
            IList<int> sampleList1 = tableList1.TakeRandom(sampleSize1, rand);
            IList<int> sampleList2 = tableList2.TakeRandom(sampleSize2, rand);

            // Create Cross Join Of Two Random Samples
            List<BLOCKING_ID> crossJoin = (from r1 in sampleList1
                                                  from r2 in sampleList2
                                                  select new BLOCKING_ID { UNIQUE_ENTITY_ID1 = r1.ToString(), UNIQUE_ENTITY_ID2 = r2.ToString() }).ToList();

            // Fill BLOCKING_IDS Table With Result Of Cross Join
            var a = shakerDb.FillBlockIds(crossJoin);


            // Create Database Connection
            PetaPoco.Database db = new PetaPoco.Database("ConnectionString");
            
            // Create Query Joining IDENTIFIERS_1 And IDENTIFIERS_2 Using BLOCKING_IDS Table
            PetaPoco.Sql pSQL1 = new PetaPoco.Sql();
            pSQL1.Append("SELECT a." + iDName + " " + iDName + "_1, c." + iDName + " " + iDName + "_2");

            string[] fields;
            if (iDName == "PERSON_UNIQUE_ENTITY_ID")
                fields = Enum.GetNames(typeof(Enumerations.PersonFrequencyFields));
            else
                fields = Enum.GetNames(typeof(Enumerations.PlaceFrequencyFields));

            foreach (string colName in fields)
            {
                pSQL1.Append(", a." + colName + " " + colName + "_1, c." + colName + " " + colName + "_2");
            }

            pSQL1.Append("FROM SHAKER.IDENTIFIERS_1 a JOIN SHAKER.BLOCKING_IDS b ON a.SHAKER_TEMP_ID = b.UNIQUE_ENTITY_ID1");
            pSQL1.Append("JOIN SHAKER.IDENTIFIERS_2 c ON b.UNIQUE_ENTITY_ID2 = c.SHAKER_TEMP_ID");

            // Execute Query, Return IEnumerable of Joined Identifiers
            var query0 = db.Query<BlockJoin>(pSQL1);


            // Create List Of BLOCKING_MATCH Objects For Storing Match Status Of Each Pair Of Identifiers
            ConcurrentBag<BLOCKING_MATCH> iBM = new ConcurrentBag<BLOCKING_MATCH>();
            
            // Create Jaro-Winkler Utility For Comparing Name Columns
            SimMetricsMetricUtilities.JaroWinkler jw = new SimMetricsMetricUtilities.JaroWinkler();

            //foreach (BlockJoin bJ in query0)
            Parallel.ForEach<BlockJoin>(query0, bJ =>
            {
                // Create BLOCKING_MATCH Object
                BLOCKING_MATCH bM = new BLOCKING_MATCH();
                // Copy Over Internal IDs
                bM.PERSON_UNIQUE_ENTITY_ID_1 = bJ.PERSON_UNIQUE_ENTITY_ID_1;
                bM.PERSON_UNIQUE_ENTITY_ID_2 = bJ.PERSON_UNIQUE_ENTITY_ID_2;
                bM.PLACE_UNIQUE_ENTITY_ID_1 = bJ.PLACE_UNIQUE_ENTITY_ID_1;
                bM.PLACE_UNIQUE_ENTITY_ID_2 = bJ.PLACE_UNIQUE_ENTITY_ID_2;
                // Compare Each Pair Of Columns
                foreach (string colName in fields)
                {
                    string col1 = colName + "_1";
                    string col2 = colName + "_2";
                    string pi1 = "";
                    string pi2 = "";
                    if (bJ.GetType().GetProperty(col1).GetValue(bJ, null) != null && bJ.GetType().GetProperty(col2).GetValue(bJ, null) != null)
                    {
                        pi1 = bJ.GetType().GetProperty(col1).GetValue(bJ, null).ToString();
                        pi2 = bJ.GetType().GetProperty(col2).GetValue(bJ, null).ToString();
                        // If Column Is A Name Column, Use Jaro-Winkler, Otherwise Use Direct Comparison, Set BLOCKING_MATCH Column to 1 Or 0
                        if (Regex.IsMatch(colName, ".*NAME.*"))
                        {
                            double sim = jw.GetSimilarity(pi1, pi2);
                            if (sim > .97)
                                bM.GetType().GetProperty(colName).SetValue(bM, 1, null);
                            else
                                bM.GetType().GetProperty(colName).SetValue(bM, 0, null);
                        }
                        else
                        {
                            if (pi1 == pi2)
                                bM.GetType().GetProperty(colName).SetValue(bM, 1, null);
                            else
                                bM.GetType().GetProperty(colName).SetValue(bM, 0, null);
                        }
                    }
                }
                // Add BLOCKING_MATCH Object To List Of BLOCKING_MATCH Objects
                iBM.Add(bM);
            }
            );

            //var count = iBM.Count;
            // Fill BLOCKING_MATCH Table
            var m = shakerDb.FillBlockingMatch(iBM);

            // Return List Of BLOCKING_MATCH Objects
            return new List<BLOCKING_MATCH>(iBM);
        }
    }
}