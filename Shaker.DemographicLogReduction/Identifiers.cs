using System;
using System.Collections.Generic;
using System.Linq;
using VLDS.Shaker.DatabaseModel;
using log4net;
using System.Diagnostics;
using System.Reflection;

namespace VLDS.Shaker.DemographicLogReduction
{
    public class Identifiers
    {
        private static readonly ILog logger = LogManager.GetLogger(typeof(Identifiers));
        private IShakerDatabaseFacade shakerDb;

        public Identifiers(IShakerDatabaseFacade shakerDb)
        {
            if (shakerDb == null)
                throw new ArgumentNullException("shakerDb");

            this.shakerDb = shakerDb;
        }

        /// <summary>
        /// 1. DROP/CREATE IDENTIFIERS_{N} (where N is fillNum).
        /// 2. Gets the Rank 1 values from _FIELD_FREQS.
        /// 3. Gets list of records from the DL with maximum value for each column for a person (UEID).
        /// 4. Replace max records with Rank 1 values to get the final identifiers.
        /// 5. Insert the final identifiers in IDENTIFIERS_{N}.
        /// </summary>
        /// <param name="iDType"></param>
        /// <param name="fillNum"></param>
        /// <param name="iDsRank"></param>
        /// <returns></returns>
        public string GetIdentifiers(string iDType, string fillNum, int iDsRank)
        {
            logger.Debug("BEGIN GetIdentifiers(...)");
            var log = "SHAKER.IDENTIFIERS_LOG_" + fillNum;
            var log_freq = "SHAKER.IDENTIFIERS_LOG_" + fillNum + "_FREQ";
            var fillTable = "SHAKER.IDENTIFIERS_" + fillNum;
            
            // DROP AND RECREATE IDENTIFIER TABLE AND INDEXES
            shakerDb.DropCreateIdentifierTable(fillTable);

            // GET RANK LIST OF IDENTIFIERS (WHERE THERE ARE MULTIPLE VALUES)
            List<List<IDENTIFIERS_LOG_1_FREQ>> rankingLists = GetIdentifierRankLists(iDType + "_UNIQUE_ENTITY_ID", log_freq);

            // GET MAX VALUE OF EACH COLUMN FOR EACH PERSON
            List<IDENTIFIERS_1> maxRecords = shakerDb.GetMaxRecords(iDType, log);

            // CHANGE COLUMN VALUES IN maxRecords WHERE THERE IS AN ENTRY IN rankingLists
            List<IDENTIFIERS_1> finalIdentifiers = new List<IDENTIFIERS_1>();
            if (rankingLists.Count != 0)
                finalIdentifiers = GetFinalIdentifiers(rankingLists[iDsRank - 1], maxRecords, iDType + "_UNIQUE_ENTITY_ID");
            else
                finalIdentifiers = maxRecords;
            
            // BULK COPY TO IDENTIFIER TABLE WITH SINGLE RECORD PER PERSON OF RANKED IDENTIFIERS
            shakerDb.FillIdentifiers(fillTable, finalIdentifiers);

            string result = fillTable + ": " + shakerDb.GetTableSize(fillTable).ToString() + Environment.NewLine;

            logger.Debug("END GetIdentifiers(...)");

            return result;
        }
        
        /// <summary>
        /// Only returns 1 Rank for now (size of returned list always 1).  
        /// 
        /// We need to work with multiple ranks at some point.  Because we are only using 1 rank means that we always 
        /// use the column value with the highest frequency despite the maximum date.  The only case where the maximum 
        /// date comes into play for rank 1 is when there are multiple values with the same max frequency.
        /// </summary>
        /// <param name="internalIDCol"></param>
        /// <param name="freqLog"></param>
        /// <returns></returns>
        protected List<List<IDENTIFIERS_LOG_1_FREQ>> GetIdentifierRankLists(string internalIDCol, string freqLog)
        {
            logger.Debug("BEGIN GetIdentifierRankLists(string,string)");

            // GET LIST OF IDENTIFIER LOG FREQUENCY RECORDS (OBJECTS).  
            // ORDERED BY DESCENDING UNIQUE_ENTITY_ID, COLUMN_NAME, FREQUENCY, DATE_RECORDED.
            List<IDENTIFIERS_LOG_1_FREQ> query0 = this.shakerDb.GetLogFrequencies(internalIDCol, freqLog);

            logger.Debug("GetIdentifierRankLists - Got " + query0.Count + " log frequencies.");

            // GET UNIQUE UNIQUE_ENTITY_ID, COLUMN_NAME COMBINATIONS TO USE IN RANKING

            List<unqCombo> iDs = this.shakerDb.GetLogFrequenciesUnique(internalIDCol, freqLog);

            logger.Debug("GetIdentifierRankLists - Got " + iDs.Count + " UEID/COLNAME combinations.");

            List<List<IDENTIFIERS_LOG_1_FREQ>> listsOfRanks = new List<List<IDENTIFIERS_LOG_1_FREQ>>();

            if (iDs.Count == query0.Count)
            {
                listsOfRanks.Add(query0);
            }
            else
            {
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();

                // Populate a dictionary keyed by unqCombo.  This will only hold first Rank.
                Dictionary<unqCombo, IDENTIFIERS_LOG_1_FREQ> theDict = new Dictionary<unqCombo, IDENTIFIERS_LOG_1_FREQ>(iDs.Count);
                foreach (IDENTIFIERS_LOG_1_FREQ freq in query0)
                {
                    unqCombo idAndColname = new unqCombo(freq.PERSON_UNIQUE_ENTITY_ID, freq.COLUMN_NAME);
                    if (!theDict.ContainsKey(idAndColname))
                        theDict.Add(new unqCombo(freq.PERSON_UNIQUE_ENTITY_ID, freq.COLUMN_NAME), freq);
                }
                listsOfRanks.Add(new List<IDENTIFIERS_LOG_1_FREQ>(theDict.Values));

                // FOR EACH UNIQUE COMBINATION, FIND THE HIGHEST RANKING VALUE (WHICH IS THE FIRST OCCURENCE BECAUSE OF THE SORTING)
                // **THIS CODE SHOWS HOW TO GET AND USE ANONYMOUS OBJECT TYPE**
                // JPJ 2012-12-06 commented this out.  Not sure why it gets exponentially slower the larger the input list.
                /*
                while (query0.Count > 0)
                {
                    var valsRank = new List<IDENTIFIERS_LOG_1_FREQ>();
                    foreach (unqCombo o in iDs)
                    {
                        int s = -1;
                        if (internalIDCol == "PLACE_UNIQUE_ENTITY_ID")
                        {
                            s = query0.FindIndex(a => a.PLACE_UNIQUE_ENTITY_ID == o.UNIQUE_ENTITY_ID
                                && a.COLUMN_NAME == o.COLUMN_NAME);
                        }
                        else
                        {
                            s = query0.FindIndex(a => a.PERSON_UNIQUE_ENTITY_ID == o.UNIQUE_ENTITY_ID
                                && a.COLUMN_NAME == o.COLUMN_NAME);
                        }

                        if (s != -1)
                        {
                            valsRank.Add(query0[s]);
                            //query0.RemoveAt(s);
                        }
                    }
                    listsOfRanks.Add(valsRank);
                    break;  // JPJ - only one rank for now
                }
                */

                stopwatch.Stop();
                TimeSpan elapsed = stopwatch.Elapsed;
                logger.Info(string.Format("Frequency ranking took {0}.", elapsed));
            }

            logger.Debug("END GetIdentifierRankLists(string,string)");
            return listsOfRanks;
        }

        protected List<IDENTIFIERS_1> GetFinalIdentifiers(List<IDENTIFIERS_LOG_1_FREQ> rankList, List<IDENTIFIERS_1> maxRecords, string iDColumn)
        {
            logger.Debug("BEGIN GetFinalIdentifiers(...)");

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            // Populate a dictionary with appropriate UEID as key for fast lookup in rankList loop.
            Dictionary<string, IDENTIFIERS_1> maxRecordsDict = new Dictionary<string, IDENTIFIERS_1>(maxRecords.Count);
            foreach (IDENTIFIERS_1 maxRecord in maxRecords)
            {
                string key = null;
                if (iDColumn == "PLACE_UNIQUE_ENTITY_ID")
                {
                    key = maxRecord.PLACE_UNIQUE_ENTITY_ID;
                }
                else if (iDColumn == "PERSON_UNIQUE_ENTITY_ID")
                {
                    key = maxRecord.PERSON_UNIQUE_ENTITY_ID;
                }

                if (maxRecordsDict.Keys.Contains(key))
                    throw new InvalidOperationException("Duplicate entry for key " + key + " in maxRecordsDict!");

                maxRecordsDict.Add(key, maxRecord);
            }

            // LOOP OVER rankList, REPLACE VALUES IN maxPersonRecords WHERE APPLICABLE
            foreach (IDENTIFIERS_LOG_1_FREQ r in rankList)
            {
                string property = r.COLUMN_NAME.Trim();  
                string idCola = "a." + iDColumn;
                string idColr = "r." + iDColumn;
                IDENTIFIERS_1 maxRecord = null;
                if (iDColumn == "PLACE_UNIQUE_ENTITY_ID")
                {
                    maxRecordsDict.TryGetValue(r.PLACE_UNIQUE_ENTITY_ID, out maxRecord);
                }
                if (iDColumn == "PERSON_UNIQUE_ENTITY_ID")
                {
                    maxRecordsDict.TryGetValue(r.PERSON_UNIQUE_ENTITY_ID, out maxRecord);
                }
                //index = maxRecords.FindIndex(a => idCola == idColr); //a.PERSON_UNIQUE_ENTITY_ID == r.PERSON_UNIQUE_ENTITY_ID);
                if (maxRecord != null)
                {
                    PropertyInfo info = maxRecord.GetType().GetProperty(property);
                    info.SetValue(maxRecord, r.VALUE.Trim(), null);
                }  
            }

            stopwatch.Stop();
            TimeSpan elapsed = stopwatch.Elapsed;
            logger.Info("Get Final Identifiers took " + elapsed + ".");

            // RETURN maxPersonRecords, NOW WITH REPLACED VALUES
            logger.Debug("END GetFinalIdentifiers(...)");
            return maxRecords;
        }

    }
}