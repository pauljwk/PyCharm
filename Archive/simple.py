import arcpy

# KDRaster = r'G:\WORKING\DelMe.gdb\Con_KernelD_9'
# arcpy.env.overwriteOutput = False

arcpy.AddMessage("OverWrite: {}".format(arcpy.env.overwriteOutput))

KDRaster = arcpy.GetParameterAsText(0)

arcpy.AddMessage(KDRaster)
arcpy.AddMessage(arcpy.Exists(KDRaster))

KDRaster = str(KDRaster)
arcpy.AddMessage(KDRaster)
arcpy.AddMessage(arcpy.Exists(KDRaster))

# arcpy.SetParameterAsText(0,KDRaster)

# KDRaster = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb/test_KD'
#
# arcpy.AddMessage(KDRaster)
# arcpy.AddMessage(arcpy.Exists(KDRaster))
