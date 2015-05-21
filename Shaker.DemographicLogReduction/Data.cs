using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Configuration;
using VLDS.Shaker.DatabaseModel;

namespace VLDS.Shaker.DemographicLogReduction
{
    public class Data
    {
        private IShakerDatabaseFacade shakerDb;

        public Data(IShakerDatabaseFacade shakerDb)
        {
            if (shakerDb == null)
                throw new ArgumentNullException("shakerDb");

            this.shakerDb = shakerDb;
        }

        public int GetData1()
        {
            shakerDb.DropCreateIdentifierLog("SHAKER.IDENTIFIERS_LOG_1");

            PetaPoco.Sql pSQL = PetaPoco.Sql.Builder
                .Append("SELECT [PLACE_UNIQUE_ENTITY_ID],[PLACE_NAME],[PLACE_STREET_NUMBER],[PLACE_STREET_NAME],left([PLACE_ZIP_CODE], 5) PLACE_ZIP_CODE,[PLACE_CONTACT_FIRST_NAME],[PLACE_CONTACT_LAST_NAME],[PLACE_PHONE],[PLACE_EMAIL],[PLACE_RECORD_DATE],GetDate() as [CREATED_DATE]")
                .Append("FROM [QRIS_IDENTIFIER_LOG]");

            PetaPoco.Database db = new PetaPoco.Database("HANDSConnectionString");
            var query = db.Query<IDENTIFIERS_LOG_1>(pSQL);
            
            shakerDb.FillIdentifiers("SHAKER.IDENTIFIERS_LOG_1", query);
            return 0;
        }

        public int GetData2()
        {
            shakerDb.DropCreateIdentifierLog("SHAKER.IDENTIFIERS_LOG_2");

            PetaPoco.Sql pSQL = PetaPoco.Sql.Builder
                .Append("SELECT [PLACE_UNIQUE_ENTITY_ID],[PLACE_NAME],[PLACE_STREET_NUMBER],[PLACE_STREET_NAME],[PLACE_ZIP_CODE],[PLACE_PHONE],[PLACE_RECORD_DATE],[PLACE_CONTACT_FIRST_NAME],[PLACE_CONTACT_LAST_NAME],GetDate() as [CREATED_DATE]")
                .Append("FROM [LICENSING_LOG]");

            PetaPoco.Database db = new PetaPoco.Database("DOLPConnectionString");
            var query = db.Query<IDENTIFIERS_LOG_1>(pSQL);
            //HANDSConnectionString.LICENSING_LOG

            shakerDb.FillIdentifiers("SHAKER.IDENTIFIERS_LOG_2", query);
            return 0;
        }

    }
}