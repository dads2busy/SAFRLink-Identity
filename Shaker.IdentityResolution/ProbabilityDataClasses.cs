using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace VLDS.Shaker.IdentityResolution
{
    public class column_match_m_u
    {
        public string columnName { get; set; }
        public double match { get; set; }
        public double m_prob { get; set; }
        public double u_prob { get; set; }
    }
}