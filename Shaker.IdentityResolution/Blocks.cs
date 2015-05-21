using System;
using System.Collections.Generic;
using System.Linq;
using VLDS.Shaker.DatabaseModel;
using SD.Tools.Algorithmia.GeneralDataStructures;
using SimMetricsMetricUtilities;

namespace VLDS.Shaker.IdentityResolution
{
    public class Blocks
    {
        private IShakerDatabaseFacade shakerDb;

        public Blocks(IShakerDatabaseFacade shakerDb)
        {
            if (shakerDb == null)
                throw new ArgumentNullException("shakerDb");

            this.shakerDb = shakerDb;
        }

        /// <summary>
        /// Dictionary key is identifier column name.  Value is a Comparer or null if comparison is done in database.
        /// This pulls data from tables DEMOGRAPHIC_LOG_FIELD_CONFIG and BLOCKING_SCHEME.
        /// </summary>
        /// <param name="hashKey"></param>
        /// <returns></returns>
        public IDictionary<string, IEqualityComparer<string>> GetIdentifierComparers(string iDType, string hashKey)
        {
            Dictionary<string,DemographicLogFieldConfig> fieldConfigs = shakerDb.GetDemographicLogFieldConfigs().GetValues(iDType, true)
                .ToDictionary(fc => fc.FieldName);
            List<BLOCKING_SCHEME> blockingSchemes = shakerDb.GetBlockingSchemes(iDType);

            HashSet<string> blockingSchemeIdentifiers = new HashSet<string>();
            blockingSchemeIdentifiers.UnionWith(blockingSchemes.Select(s => s.IDENTIFIER_1));
            blockingSchemeIdentifiers.UnionWith(blockingSchemes.Select(s => s.IDENTIFIER_2));

            IDictionary<string, IEqualityComparer<string>> results = new Dictionary<string, IEqualityComparer<string>>();

            foreach (string blockingIdentifier in blockingSchemeIdentifiers)
            {
                DemographicLogFieldConfig fieldConfig = fieldConfigs[blockingIdentifier];
                IEqualityComparer<string> comparer = null;

                if (fieldConfig.HashAlgorithmName.Contains("PENPADCHAFF"))
                {
                    if (!fieldConfig.CompareThreshhold.HasValue)
                    {
                        string msg = string.Format("Found DemographicLogFieldConfig {0} with hash algorithm = {1} but no CompareThreshhold.", fieldConfig.FieldName, fieldConfig.HashAlgorithmName);
                        throw new InvalidOperationException(msg);
                    }

                    comparer = new PadNChaffComparer(fieldConfig.CompareThreshhold.Value);
                }

                results.Add(blockingIdentifier, comparer);
            }

            return results;
        }

        public void MakeBlock(BLOCKING_SCHEME blockingScheme, List<string> matchColumns, IEqualityComparer<string> comparer1, IEqualityComparer<string> comparer2)
        {
            var iDName = blockingScheme.BLOCKING_SCHEME_TYPE + "_UNIQUE_ENTITY_ID";

            IEnumerable<BLOCKING_ID> iDs = shakerDb.Block(blockingScheme, comparer1, comparer2);
            shakerDb.FillBlockIds(iDs);

            IEnumerable<BLOCKING_MATCH> blockIdents = GetBlockMatch(iDName, blockingScheme, matchColumns);
            shakerDb.FillBlockingMatch(blockIdents);
        }

        private IEnumerable<BLOCKING_MATCH> GetBlockMatch(string iDName, BLOCKING_SCHEME blockingScheme, List<string> fields)
        {
            // JPJ 2012-12-13 Now we only compare the columns from MATCH_COLUMN (rather than using all fields from Enumerations.PersonFrequencyFields).
            // This is because the probability is currently only calcualted based on the MATCH_COLUMNs anyway (see Probabilities.transposeBlockMatch).
            /*
            string[] fields;

            if (iDName == "PERSON_UNIQUE_ENTITY_ID")
                fields = Enum.GetNames(typeof(Enumerations.PersonFrequencyFields));
            else
                fields = Enum.GetNames(typeof(Enumerations.PlaceFrequencyFields));    // TODO: EXCLUDE MATCH_ID COLS
             */

            var query0 = shakerDb.GetBlockJoin(iDName);

            List<BLOCKING_MATCH> iBM = new List<BLOCKING_MATCH>();

            foreach (BlockJoin bj in query0)
            {
                BLOCKING_MATCH bm = new BLOCKING_MATCH();
                bm.PERSON_UNIQUE_ENTITY_ID_1 = bj.PERSON_UNIQUE_ENTITY_ID_1;
                bm.PERSON_UNIQUE_ENTITY_ID_2 = bj.PERSON_UNIQUE_ENTITY_ID_2;
                bm.PLACE_UNIQUE_ENTITY_ID_1 = bj.PLACE_UNIQUE_ENTITY_ID_1;
                bm.PLACE_UNIQUE_ENTITY_ID_2 = bj.PLACE_UNIQUE_ENTITY_ID_2;

                foreach (string colName in fields)
                {
                    string col1 = colName + "_1";
                    string col2 = colName + "_2";
                    string pi1 = "";
                    string pi2 = "";

                    if (bj.GetType().GetProperty(col1).GetValue(bj, null) != null && bj.GetType().GetProperty(col2).GetValue(bj, null) != null)
                    {
                        pi1 = bj.GetType().GetProperty(col1).GetValue(bj, null).ToString();
                        pi2 = bj.GetType().GetProperty(col2).GetValue(bj, null).ToString();

                        if (pi1 == "" || pi2 == "")
                            bm.GetType().GetProperty(colName).SetValue(bm, 2, null);
                        else if (colName == "PLACE_NAME")
                        {
                            SimMetricsMetricUtilities.JaroWinkler jw = new SimMetricsMetricUtilities.JaroWinkler();

                            double sim = jw.GetSimilarity(pi1, pi2);
                            if (sim > .9)
                                bm.GetType().GetProperty(colName).SetValue(bm, 1, null);
                            else
                                bm.GetType().GetProperty(colName).SetValue(bm, 0, null);
                        }
                        else if (colName == "PLACE_CONTACT_FIRST_NAME" || colName == "PLACE_CONTACT_LAST_NAME" || colName == "PERSON_FIRST_NAME" || colName == "PERSON_MIDDLE_NAMES" || colName == "PERSON_LAST_NAME")
                        {
                            SimMetricsMetricUtilities.JaroWinkler jw = new SimMetricsMetricUtilities.JaroWinkler();

                            double sim = jw.GetSimilarity(pi1, pi2);
                            if (sim > .9)
                                bm.GetType().GetProperty(colName).SetValue(bm, 1, null);
                            else
                                bm.GetType().GetProperty(colName).SetValue(bm, 0, null);
                        }
                        else if (colName == "PLACE_STREET_NAME")
                        {

                            SimMetricsMetricUtilities.JaroWinkler jw = new SimMetricsMetricUtilities.JaroWinkler();

                            double sim = jw.GetSimilarity(pi1, pi2);
                            if (sim > .9)
                                bm.GetType().GetProperty(colName).SetValue(bm, 1, null);
                            else
                                bm.GetType().GetProperty(colName).SetValue(bm, 0, null);
                        }
                        else
                        {
                            if (pi1 == pi2)
                                bm.GetType().GetProperty(colName).SetValue(bm, 1, null);
                            else
                                bm.GetType().GetProperty(colName).SetValue(bm, 0, null);
                        }
                    }
                }

                iBM.Add(bm);
            }
            return iBM;
        }

        // TODO: We will use hashKey when we compare using ChafferCompare.CompareChaffed.  
        // But right now we are comparing One-time-pads so we use JaroWinkler
        private class PadNChaffComparer : IEqualityComparer<string>
        {
            private JaroWinkler jaroWinkler = new JaroWinkler();
            private double EqualityThreshhold { get; set; }

            public PadNChaffComparer(double equalityThreshhold)
            {
                EqualityThreshhold = equalityThreshhold;
            }

            public bool Equals(string str1, string str2)
            {
                // nulls and empty strings are NOT equal!!!!  otherwise our blocking matching could result in huge matches.
                // If an identifier field is blank it should be considered unequal to any other identifier field (blank or not).
                if (string.IsNullOrWhiteSpace(str1) || string.IsNullOrWhiteSpace(str2))
                    return false;

                bool result = jaroWinkler.GetSimilarity(str1, str2) >= EqualityThreshhold;
                return result;
            }

            public int GetHashCode(string str)
            {
                if (str == null)
                    str = "";

                return str.GetHashCode();
            }
        }
    }
}