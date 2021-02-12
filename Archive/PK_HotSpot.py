import arcpy
import os
import sys
from arcpy.sa import *
arcpy.ImportToolbox(r'G:\Working\ArcMap Stuff\pkRasterHelper.tbx')

InPoints = arcpy.GetParameterAsText(0)
PopulationField = arcpy.GetParameterAsText(1)
KDExtent = arcpy.GetParameterAsText(2)

CellSize = arcpy.GetParameter(3)
SearchRadius = arcpy.GetParameter(4)
PopRaster = arcpy.GetParameterAsText(5)         # Required and typed
KDvLevel = arcpy.GetParameterAsText(6)
HotSpotBuffer = arcpy.GetParameterAsText(7)

HoldKDRaster = arcpy.GetParameterAsText(8)
KDRaster = arcpy.GetParameterAsText(9)
MinusRaster = arcpy.GetParameterAsText(10)
MinusKDv = arcpy.GetParameterAsText(11)

OutPoints = arcpy.GetParameterAsText(12)
OutBuffers = arcpy.GetParameterAsText(13)
OutZones = arcpy.GetParameterAsText(14)

out_gdb = arcpy.GetParameterAsText(15)
LogFile = arcpy.GetParameterAsText(16)

# InPoints = r'L:\CoreData\NCIS\LITS_20200729\NCIS.gdb\Incidents_xy'
# PopulationField = ''
# KDExtent = r'L:\CoreData\Sites\SitesExtents.gdb\ACT_Extent'
#
# CellSize = 200
# SearchRadius = 2000
# PopRaster = r'G:\BaseMaps\GNAF-AllGeoms-Population-ATS-2020\Population_KD_Resources.gdb\Rescale_Con_KD_ComboPopRes_Tot_P_50_2k'
# KDvLevel = 1
# HotSpotBuffer = '2.2 Kilometers'
#
# HoldKDRaster = True
# KDRaster = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\incidents_xy_200_2k_ACT'
# MinusRaster = ''
# MinusKDv = ''
#
# OutPoints = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\Incidents_xy_HotIncidents'
# OutBuffers = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\Incidents_xy_HotBuffers'
# OutZones = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\Incidents_xy_HotZones'
#
# out_gdb = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb'
# LogFile = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\Incidents_xy_KDMinusPop3.txt'

txtFile = open(LogFile, "w")

txtFile.write("KDMinusPop Parameters {}".format("\n"))
txtFile.write("InPoints = {}{}".format(InPoints, "\n"))
txtFile.write("PopulationField = {}{}".format(PopulationField, "\n"))
txtFile.write("KDExtent = {}{}".format(KDExtent, "\n"))
txtFile.write("CellSize = {}{}".format(CellSize, "\n"))
txtFile.write("SearchRadius = {}{}".format(SearchRadius, "\n"))
txtFile.write("PopRaster = {}{}".format(PopRaster, "\n"))
txtFile.write("KDvLevel = {}{}".format(KDvLevel, "\n"))
txtFile.write("HotSpotBuffer = {}{}".format(HotSpotBuffer, "\n"))
txtFile.write("HoldKDRaster = {}{}".format(HoldKDRaster, "\n"))
txtFile.write("KDRaster = {}{}".format(KDRaster, "\n"))
txtFile.write("MinusRaster = {}{}".format(MinusRaster, "\n"))
txtFile.write("OutPoints = {}{}".format(OutPoints, "\n"))
txtFile.write("OutBuffers = {}{}".format(OutBuffers, "\n"))
txtFile.write("OutZones = {}{}".format(OutZones, "\n"))
txtFile.write("LogFile = {}{}".format(LogFile, "\n"))
txtFile.write("out_gdb = {}{}".format(out_gdb, "\n"))

arcpy.env.overwriteOutput = False

with arcpy.EnvManager(scratchWorkspace=out_gdb, workspace=out_gdb):
    # arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(3857)
    # Parallel processing doesn't ALWAYS help!
    # parallel_processing = '12'

    arcpy.CheckOutExtension("spatial")
    arcpy.CheckOutExtension("ImageAnalyst")

    try:
        row_count = arcpy.GetCount_management(InPoints)[0]
        txtFile.write("Analyses Points: {} - {}{}".format(InPoints, row_count, "\n"))

        if len(KDRaster) < 1:
            KDRaster = '{}_{}'.format(InPoints, "tmp")

        if len(PopulationField) < 1:
            PopulationField = 'NONE'

        if len(KDExtent) < 1:
            KDExtent = r'L:\CoreData\Sites\SitesExtents.gdb\Aust_Extent_Main_pk'

        if arcpy.Exists(KDRaster):
            arcpy.AddMessage("KDRaster Exists - Reusing: {}".format(KDRaster))
            txtFile.write("KDRaster Exists - Reusing: {}{}".format(KDRaster, "\n"))

        else:

            # arcpy.AddMessage(KDRaster)
            arcpy.AddMessage("Building KernelDensity - InPoints: {} - TargetRaster: {} - PopField: {}".format(InPoints, KDRaster, PopulationField))
            txtFile.write("Building KernelDensity - InPoints: {} - TargetRaster: {} - PopField: {}{}".format(InPoints, KDRaster, PopulationField, "\n"))

            arcpy.env.extent = KDExtent
            kd_result = KernelDensity(in_features=InPoints,
                                      population_field=PopulationField,
                                      cell_size=CellSize,
                                      search_radius=SearchRadius,
                                      area_unit_scale_factor='SQUARE_KILOMETERS',
                                      out_cell_values='DENSITIES',
                                      method='PLANAR',
                                      in_barriers=None)

            txtFile.write("Finished Building KD - {}{}".format(KDRaster, "\n"))

            con_result = Con(kd_result, kd_result, None, "VALUE > 0")

            max_val = arcpy.GetRasterProperties_management(con_result, "MAXIMUM")
            txtFile.write("Finished Con - {} - Max Value: {}{}".format(KDRaster, max_val, "\n"))

            with arcpy.EnvManager(compression="NONE", pyramid="NONE"):
                out_raster = arcpy.sa.RescaleByFunction(in_raster=con_result,
                                                        transformation_function=[["LINEAR", 0, "", max_val, "", 0, max_val, ""]],
                                                        from_scale=0,
                                                        to_scale=20)

                out_raster.save(KDRaster)

                txtFile.write("Finished ReScale - {}{}".format(KDRaster, "\n"))

        # The 'topic' KD exists as a value-range 0-20 Raster known and saved as: KDRaster
        if arcpy.Exists(KDRaster):

            Min_Raster = "{}_Minus".format(MinusRaster)
            if arcpy.Exists(Min_Raster):
                txtFile.write("Minus Raster Exists - Reusing: {}{}".format(Min_Raster, "\n"))
            else:
                txtFile.write("Building Minus: {}{}".format(Min_Raster, "\n"))
                min_result = arcpy.ia.Minus(KDRaster, PopRaster)
                min_result.save(Min_Raster)

            Con_Raster = "{}_ConMinus".format(MinusRaster)
            if arcpy.Exists(Con_Raster):
                txtFile.write("ConMinus Raster Exists - Reusing: {}{}".format(Con_Raster, "\n"))
            else:
                txtFile.write("Building ConMinus: {}{}".format(Con_Raster, "\n"))
                con_result = arcpy.ia.Con(Min_Raster, Min_Raster, None, "VALUE > 0")
                con_result.save(Con_Raster)

            rescale_raster = "{}_RescaleConMinus".format(MinusRaster)
            if arcpy.Exists(rescale_raster):
                txtFile.write("RescaleConMinus Raster Exists - Reusing: {}{}".format(rescale_raster, "\n"))
            else:
                txtFile.write("Building RescaleConMinus: {}{}".format(rescale_raster, "\n"))
                max_val = arcpy.GetRasterProperties_management(Con_Raster, "MAXIMUM")
                txtFile.write("Building RescaleConMinus: {}, MaxVal: {}{}".format(Min_Raster, max_val, "\n"))
                rescale_result = arcpy.sa.RescaleByFunction(in_raster=Con_Raster,
                                                            transformation_function=[["LINEAR", 0, "", max_val, "", 0, max_val, ""]],
                                                            from_scale=0,
                                                            to_scale=20)

                rescale_result.save(rescale_raster)
                rescale_result.save(MinusRaster)

            int_raster = "{}_IntRescaleConMinus".format(MinusRaster)
            if arcpy.Exists(int_raster):
                txtFile.write("IntRescaleConMinus Raster Exists - Reusing: {}{}".format(int_raster, "\n"))
            else:
                txtFile.write("Building IntRescaleConMinus: {}{}".format(int_raster, "\n"))
                int_result = Int(rescale_raster)
                int_result.save(int_raster)

            if arcpy.Exists(MinusKDv):
                txtFile.write("KDv Exists - Reusing: {}{}".format(MinusKDv, "\n"))
            else:
                txtFile.write("Building KDv: {}{}".format(int_raster, "\n"))
                kdv_result = arcpy.conversion.RasterToPolygon(int_raster,
                                                              MinusKDv,
                                                              "SIMPLIFY",
                                                              "Value",
                                                              "SINGLE_OUTER_PART",
                                                              None)
            kdv_result.save(MinusKDv)

        else:
            txtFile.write(KDRaster + ' does not exist')
    finally:

        # if not HoldKDRaster:
            # arcpy.Delete_management(KDRaster)
            # arcpy.Delete_management(min_raster)
            # arcpy.Delete_management(con_raster)
            # arcpy.Delete_management(rescale_raster)

        txtFile.write(arcpy.GetMessages())
        txtFile.close()
