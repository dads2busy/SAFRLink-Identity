Date Created:  Nov 6, 2012
Created By: Jerome Jacobsen

The two projects in this solution are VT IPG code stored in Subversion.  They are used in the Shaker solution which is stored in VITA TFS.

Notes:
- These two solutions were copied over from CS_SHAKER solution stored in VT Subversion repository.  This was done because of the dependencies
  that these two projects had on the TFS Shaker project which no longer allowed the CS_SHAKER solution to build.  The CS_SHAKER has now been
  reverted back to its last working version.  The projects of the same name in that version of CS_SHAKER do not work with the TFS Shaker.
- This solution and the two individual projects will not build here because of their dependencies on projects in the Shaker solution in TFS.  
  They have to be included in the TFS Shaker solution in order to build.
