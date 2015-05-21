using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using VLDS.Shaker.Common;
using VLDS.Shaker.DatabaseModel;

namespace VLDS.Shaker.IdentityResolution
{
    public class Parameters
    {
        private IShakerDatabaseFacade shakerDb;

        public Parameters(IShakerDatabaseFacade shakerDb)
        {
            if (shakerDb == null)
                throw new ArgumentNullException("shakerDb");

            this.shakerDb = shakerDb;
        }

        // REPEATEDLY CALLS Parameters(List<column_m_u> matchCols, double prop) UNTIL PARAMETERS SETTLE AT THRESHOLD
        public List<column_m_u> GetParameters(List<column_m_u> matchCols)
        {
            bool a = false;
            bool b = false;
            double m1 = 1;
            double m2 = 0;
            double u1 = 1;
            double u2 = 0;

            ParametersCalc emp = new ParametersCalc(this.shakerDb, matchCols);
            ParametersCalc retParms;
            List<column_m_u> mu = emp.parameters;
            List<column_m_u> mui = mu;
            double pp = emp.p;
            // LOOP THOUGH EM UNTIL BOTH m AND u PROBABILITIES SETTLE AT 8 DECIMAL PLACES
            do
            {
                ParametersCalc emp2 = new ParametersCalc(this.shakerDb, mui, pp);
                List<column_m_u> mu2 = emp2.parameters;
                mui = mu2;
                pp = emp2.p;
                m1 = m2;
                m2 = Math.Round(mu2[0].column_m, 8);
                u1 = u2;
                u2 = Math.Round(mu2[0].column_u, 8);
                a = check(m1, m2);
                b = check(u1, u2);
                retParms = emp2;
            }
            while (a == false || b == false);

            return retParms.parameters;
        }

        static bool check(double d1, double d2)
        {
            return d1 == d2;
        }

    private class ParametersCalc
    {
        private IShakerDatabaseFacade shakerDatabase;

        // VARIABLES
        List<column_m_u> matchColumns { get; set; } // COLUMNS FOR WHICH TO GET m AND u PROBABILITIES
        List<BLOCKING_MATCH> matchRecords { get; set; } // BLOCKING MATCH RECORDS FOR COLUMNS IN matchColumns
        List<List<m_u_pair>> parms_m = new List<List<m_u_pair>>(); // LIST OF m VALUE LISTS
        List<List<m_u_pair>> parms_u = new List<List<m_u_pair>>(); // LIST OF u VALUE LISTS
        DataTable dt { get; set; }
        public List<column_m_u> parameters = new List<column_m_u>();
        
        private double u = .1; // INITIAL u VALUE
        private double m = .9; // INTIIAL m VALUE
        public double p = .5; // INITIAL p VALUE

        // GO THROUGH ALL STEPS OF SINGLE PASS TO DETERMINE PARAMETERS
        public ParametersCalc(IShakerDatabaseFacade shakerDatabase, List<column_m_u> matchCols, double prop = .5)
        {
            this.shakerDatabase = shakerDatabase;
            p = prop;
            matchColumns = matchCols;

            // GET LIST OF BLOCKING MATCH RECORDS (OBJECTS) FOR COLUMNS IN matchColumns
            matchRecords = this.shakerDatabase.GetBlockingMatch(matchColumns.Select(x => x.columnName));

            // CONVERT LIST OF BLOCKING MATCH OBJECTS TO A DATATABLE
            ConvertToDataTable();

            // CREATE LISTS OF m AND u PARAMETER LISTS TO SEND TO CalculateParemeters
            CreateParmsMU();

            // CALCULATE m AND u PROBABILITIES FOR GIVEN PARAMETER LISTS
            CalculateParameters();

            // CALCULATE PROPORTION VARIABLE p
            p = Calculate_p(); 
        }

        void ConvertToDataTable()
        {
            // CONVERT LIST OF BLOCKING MATCH OBJECTS TO A DATATABLE
            dt = Utilities.ToDataTable<BLOCKING_MATCH>(matchRecords);
        }

        void CreateParmsMU()
        {          
            foreach (DataRow matchRecord in dt.Rows)
            {
                var record_m = new List<m_u_pair>();
                var record_u = new List<m_u_pair>();
                foreach (column_m_u colmu in matchColumns)
                {
                    foreach (DataColumn matchField in dt.Columns)
                    {
                        if (matchField.ColumnName == colmu.columnName)
                        {
                            var kv_m = new m_u_pair();
                            kv_m.match = (int)matchRecord[matchField.ColumnName];
                            if (colmu.column_m == 0)
                                kv_m.m_u = m;
                            else
                                kv_m.m_u = colmu.column_m;
                            record_m.Add(kv_m);

                            var kv_u = new m_u_pair();
                            kv_u.match = (int)matchRecord[matchField.ColumnName];
                           if (colmu.column_u == 0)
                               kv_u.m_u = u;
                           else
                               kv_u.m_u = colmu.column_u;
                            record_u.Add(kv_u);
                        }
                    }
                }
                parms_m.Add(record_m);
                parms_u.Add(record_u);
            }
        }

        void CalculateParameters()
        {
            // FOR EACH COLUMN OBJECT IN matchColumns, GET AND UPDATE OBJECT WITH m AND u PROBABILITIES
            foreach (column_m_u colmu in matchColumns)
            {
                column_m_u cmu = new column_m_u();
                cmu.column_m = CalculateField_m(colmu.columnName); // GET m
                cmu.column_u = CalculateField_u(colmu.columnName); // GET u
                cmu.columnName = colmu.columnName;
                
                // ADD TO PARAMETER LIST
                parameters.Add(cmu);
            }
        }

        double CalculateField_m(string field)
        {
            double m_field = 0;
            double numerator = 0;
            double denominator = 0;

            int recCnt = parms_m.Count;

            for (int i = 0; i < recCnt; i++)
            {
                var m_rec_n = (int)dt.Rows[i][field] * Record_m(parms_m[i], parms_u[i], p);
                numerator += m_rec_n;
            }

            for (int i = 0; i < recCnt; i++)
            {
                var m_rec_d = Record_m(parms_m[i], parms_u[i], p);
                denominator += m_rec_d;
            }

            m_field = numerator / denominator;
            return m_field;
        }

        double CalculateField_u(string field)
        {
            double u_field = 0;
            double numerator = 0;
            double denominator = 0;

            int recCnt = parms_m.Count;

            for (int i = 0; i < recCnt; i++)
            {
                var u_rec_n = (int)dt.Rows[i][field] * Record_u(parms_m[i], parms_u[i], p);
                numerator += u_rec_n;
            }

            for (int i = 0; i < recCnt; i++)
            {
                var u_rec_d = Record_u(parms_m[i], parms_u[i], p);
                denominator += u_rec_d;
            }

            u_field = numerator / denominator;
            return u_field;
        }

        double Calculate_p()
        {
            double pCalc = 0;
            int recCnt = parms_m.Count;
            double numerator = 0;
            double denominator = recCnt;
            
            for (int i = 0; i < recCnt; i++)
            {
                var m_rec_d = Record_m(parms_m[i], parms_u[i], p);
                numerator += m_rec_d;
            }
            pCalc = numerator / denominator;
            return pCalc;
        }

        double Record_m(List<m_u_pair> record_m, List<m_u_pair> record_u, double p)
        {
            double mPow = Calculate_Parameters_m_u_Power(record_m);
            double uPow = Calculate_Parameters_m_u_Power(record_u);
            double mDen = Calculate_Parameters_m_u_Denominator(p, p * mPow, uPow);
            return (p * mPow / mDen);
        }

        double Record_u(List<m_u_pair> record_m, List<m_u_pair> record_u, double p)
        {
            double mPow = Calculate_Parameters_m_u_Power(record_m);
            double uPow = Calculate_Parameters_m_u_Power(record_u);
            double mDen = Calculate_Parameters_m_u_Denominator(p, p * mPow, uPow);
            return (p * uPow / mDen);
        }

        double Calculate_Parameters_m_u_Power(List<m_u_pair> record)
        {
            double result = 1;
            foreach (m_u_pair field in record)
            {
                double x = Math.Pow(field.m_u, field.match) * Math.Pow((1 - field.m_u), (1 - field.match));
                result = result * x;
            }
            return result;
        }

        double Calculate_Parameters_m_u_Denominator(double p, double p_m_power, double u_power)
        {
            return (p_m_power + ((1 - p) * u_power));
        }

    }
    }
}