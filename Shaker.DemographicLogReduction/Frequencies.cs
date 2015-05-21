using System;
using System.Diagnostics;
using VLDS.Shaker.DatabaseModel;

namespace VLDS.Shaker.DemographicLogReduction
{
    public class Frequencies
    {
        private IShakerDatabaseFacade shakerDb;

        public Frequencies(IShakerDatabaseFacade shakerDb)
        {
            if (shakerDb == null)
                throw new ArgumentNullException("shakerDb");

            this.shakerDb = shakerDb;
        }

        /// <summary>
        /// 1. DROP/CREATE table IDENTIFIERS_LOG_{N}_FREQ (parameter FREQUENCY_LOG).
        /// 2. Iterate over each column defined in Enumerations.PersonFrequencyFields and perform these steps:
        ///    2.1 DROP/CREATE tables _FIELD_FREQ_IDS and _FIELD_FREQS.
        ///    2.2 INSERTS into _FIELD_FREQ_ID the UEIDs from IDENTIFIERS_LOG_{N} which have multiple values for the current column (e.g. FIRST_NAME).
        ///    2.3 For those UEIDs _FIELD_FREQ_ID INSERTS into _FIELD_FREQS the Frequency and Maximum Date of each Value for each UEID+COLNAME+VALUE.
        /// </summary>
        /// <param name="iDType"></param>
        /// <param name="fillNum"></param>
        /// <returns></returns>
        public string GetFrequencies(string iDType, string fillNum)
        {
            var iDName = iDType + "_UNIQUE_ENTITY_ID";
            var recDateName = iDType + "_RECORD_DATE";
            string log = "SHAKER.IDENTIFIERS_LOG_" + fillNum;
            var log_freq = "SHAKER.IDENTIFIERS_LOG_" + fillNum + "_FREQ";

            var freqreturn = this.shakerDb.GenerateLogFrequencies(iDType, log_freq, log, iDName, recDateName);

            return "Log " + fillNum + " Unique Person/Place, Multiple Values" + Environment.NewLine + freqreturn;
        }

        // Currently Unused
        public string GetFrequencies(string iDType)
        {
            var iDName = iDType + "_UNIQUE_ENTITY_ID";
            var recDateName = iDType + "_RECORD_DATE";
            string log1, log2;
            log1 = "SHAKER.IDENTIFIERS_LOG_1";
            log2 = "SHAKER.IDENTIFIERS_LOG_2";

            var log_freq1 = "SHAKER.IDENTIFIERS_LOG_1_FREQ";
            var log_freq2 = "SHAKER.IDENTIFIERS_LOG_2_FREQ";

            var freq1return = this.shakerDb.GenerateLogFrequencies(iDType, log_freq1, log1, iDName, recDateName);
            var freq2return = this.shakerDb.GenerateLogFrequencies(iDType, log_freq2, log2, iDName, recDateName);

            return "Log 1 Unique Person/Place, Multiple Values" + Environment.NewLine + freq1return + Environment.NewLine + "Log 2 Unique Person/Place, Multiple Values" + Environment.NewLine + freq2return;
        }
        
    }
}