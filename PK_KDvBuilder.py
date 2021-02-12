import arcpy, time
import os
import sys
from arcpy.sa import *
arcpy.ImportToolbox(r'G:\Working\ArcMap Stuff\pkRasterHelper.tbx')

InPoints = arcpy.GetParameterAsText(0)
PopulationField = arcpy.GetParameterAsText(1)
KDExtent = arcpy.GetParameterAsText(2)

CellSize = arcpy.GetParameter(3)
SearchRadius = arcpy.GetParameter(4)

HoldKDRaster = arcpy.GetParameterAsText(5)
KDRaster = arcpy.GetParameterAsText(6)
KDVector = arcpy.GetParameterAsText(7)

out_gdb = arcpy.GetParameterAsText(8)
LogFile = arcpy.GetParameterAsText(9)

# InPoints = r'F:\Suicide_Vs_Population\LITS_20200729\KDs_Incidents_2006_2017.gdb\Away_Tot_P'
# PopulationField = ''
# KDExtent = ""
#
# CellSize = 50
# SearchRadius = 2000
#
# HoldKDRaster = True
# KDRaster = r'F:\Suicide_Vs_Population\LITS_20200729\KDs_Incidents_2006_2017.gdb\KDr_Away_Tot_P'
# KDVector = r'F:\Suicide_Vs_Population\LITS_20200729\KDs_Incidents_2006_2017.gdb\KDv_Away_Tot_P'
#
# out_gdb = r'G:\Working\Default.gdb'
# LogFile = r'F:\Suicide_Vs_Population\LITS_20200729\KDv_Builder_away_Tot_P_50_2k.txt'

txtFile = open(LogFile, "w")

txtFile.write("KDMinusPop Parameters {}".format("\n"))
txtFile.write("InPoints = {}{}".format(InPoints, "\n"))
txtFile.write("PopulationField = {}{}".format(PopulationField, "\n"))
txtFile.write("KDExtent = {}{}".format(KDExtent, "\n"))
txtFile.write("CellSize = {}{}".format(CellSize, "\n"))
txtFile.write("SearchRadius = {}{}".format(SearchRadius, "\n"))

txtFile.write("HoldKDRaster = {}{}".format(HoldKDRaster, "\n"))
txtFile.write("KDRaster = {}{}".format(KDRaster, "\n"))
txtFile.write("KDVector = {}{}".format(KDVector, "\n"))

txtFile.write("Scratch = {}{}".format(out_gdb, "\n"))
txtFile.write("LogFile = {}{}".format(LogFile, "\n"))

arcpy.env.overwriteOutput = True

with arcpy.EnvManager(scratchWorkspace=out_gdb, workspace=out_gdb):
    # arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(3857)
    # Parallel processing doesn't ALWAYS help!
    # parallel_processing = '12'

    arcpy.CheckOutExtension("spatial")
    arcpy.CheckOutExtension("ImageAnalyst")

    try:
        row_count = arcpy.GetCount_management(InPoints)[0]
        arcpy.AddMessage("Starting KDv Builder using Analyses Points: {} - {} - {}".format(InPoints,
                                                                                           row_count,
                                                                                           time.strftime("%X")))
        txtFile.write("Starting KDv Builder using Analyses Points: {} - {} - {}".format(InPoints,
                                                                                        row_count,
                                                                                        time.strftime("%X"),
                                                                                        "\n"))

        if len(KDRaster) < 1:
            KDRaster = '{}_{}'.format(InPoints, "tmp")

        KDrAlias = os.path.split(KDRaster)[1]
        ## The result of os.path.split is a 'list' (zero indexed) and can be completed thusly:
        ## Be warned that Feature Datasets are a complication.....
        # KDrGDB, KDRName = os.path.split(KDRaster)
        KDvAlias = os.path.split(KDVector)[1]

        if len(PopulationField) < 1:
            PopulationField = 'NONE'

        if len(KDExtent) < 1:
            arcpy.AddMessage("Building Custom Extent using Analyses Points: {} - {} - {}".format(InPoints,
                                                                                                 row_count,
                                                                                                 time.strftime("%X")))
            txtFile.write("Building Custom Extent using Analyses Points: {} - {} - {}".format(InPoints,
                                                                                              row_count,
                                                                                              time.strftime("%X"),
                                                                                              "\n"))

            KDextent_tight = "{}_ExtentTight".format(InPoints)
            KDextent = "{}_Extent".format(InPoints)
            arcpy.management.MinimumBoundingGeometry(InPoints, KDextent_tight,
                                                     "ENVELOPE",
                                                     "ALL")

            arcpy.analysis.Buffer(in_features=KDextent_tight,
                                  out_feature_class=KDextent,
                                  buffer_distance_or_field=SearchRadius,
                                  line_side="FULL",
                                  line_end_type="ROUND",
                                  dissolve_option="NONE",
                                  dissolve_field=None,
                                  method="PLANAR")

            if arcpy.Exists(KDextent_tight):
                arcpy.Delete_management(KDextent_tight)

            arcpy.AddMessage("Custom Extent Built: {} - {}".format(KDextent,
                                                                   time.strftime("%X")))
            txtFile.write("Custom Extent Built: {} - {}{}".format(InPoints,
                                                                  time.strftime("%X"),
                                                                  "\n"))

        if not arcpy.Exists(KDRaster):

            arcpy.AddMessage("Starting Raw KD - {} - {}".format(KDRaster,
                                                                time.strftime("%X")))
            txtFile.write("Starting Raw KD - {} - {}{}".format(KDRaster,
                                                               time.strftime("%X"),
                                                               "\n"))

            arcpy.env.extent = KDExtent
            kd_result = KernelDensity(in_features=InPoints,
                                    population_field=PopulationField,
                                    cell_size=CellSize,
                                    search_radius=SearchRadius,
                                    area_unit_scale_factor='SQUARE_KILOMETERS',
                                    out_cell_values='DENSITIES',
                                    method='PLANAR',
                                    in_barriers=None)

            arcpy.AddMessage("Finished Building KD: {} - {}".format(KDRaster,
                                                                    time.strftime("%X")))
            txtFile.write("Finished Building KD: {} - {}{}".format(KDRaster,
                                                                   time.strftime("%X"),
                                                                   "\n"))

            con_result = Con(kd_result, kd_result, None, "VALUE > 0")

            max_val = arcpy.GetRasterProperties_management(con_result, "MAXIMUM")

            arcpy.AddMessage("Finished Con: {} - Max Value: {} - {}".format(KDRaster,
                                                                            max_val,
                                                                            time.strftime("%X")))
            txtFile.write("Finished Con: {} - Max Value: {} - {}{}".format(KDRaster,
                                                                           max_val,
                                                                           time.strftime("%X"),
                                                                           "\n"))

            with arcpy.EnvManager(compression="NONE", pyramid="NONE"):
                out_raster = arcpy.sa.RescaleByFunction(in_raster=con_result,
                                                        transformation_function=[["LINEAR", 0, "", max_val, "", 0, max_val, ""]],
                                                        from_scale=0,
                                                        to_scale=20)

                out_raster.save(KDRaster)

                arcpy.AddMessage("Finished ReScaling KD, Saving as: {} - {}".format(KDRaster,
                                                                                    time.strftime("%X")))
                txtFile.write("Finished ReScaling KD, Saving as: {} - {}{}".format(KDRaster,
                                                                                   time.strftime("%X"),
                                                                                   "\n"))

                if arcpy.Exists(con_result):
                    txtFile.write("Deleting: {} - {}{}".format(con_result,
                                                               time.strftime("%X"),
                                                               "\n"))
                    arcpy.Delete_management(con_result)

                if arcpy.Exists(kd_result):
                    txtFile.write("Deleting: {} - {}{}".format(kd_result,
                                                               time.strftime("%X"),
                                                               "\n"))
                    arcpy.Delete_management(kd_result)

                if arcpy.Exists(KDRaster):
                    # arcpy.AlterAliasName(OutputKDr, KDrAlias)
                    arcpy.Rename_management(KDRaster, KDrAlias)

                if not arcpy.Exists(KDVector):
                    arcpy.AddMessage("Converting to Integer: {} - {}".format(KDRaster,
                                                                             time.strftime("%X")))
                    txtFile.write("Converting to Integer: {} - {}{}".format(KDRaster,
                                                                            time.strftime("%X"),
                                                                            "\n"))

                    int_result = "{}_Tmp".format(KDRaster)
                    arcpy.Int_3d(KDRaster, int_result)

                    arcpy.AddMessage("Building KDv: {}".format(KDVector))
                    txtFile.write("Building KDv: {}{}".format(KDVector, "\n"))

                    kdv_result = arcpy.RasterToPolygon_conversion(in_raster=int_result,
                                                                  out_polygon_features=KDVector,
                                                                  simplify="NO_SIMPLIFY",
                                                                  raster_field="Value",
                                                                  create_multipart_features="SINGLE_OUTER_PART",
                                                                  max_vertices_per_feature=None)

                    arcpy.Delete_management(int_result)

                    ResidualCon = "{}{}KernelD_GNAF2".format(out_gdb, "\\")
                    if arcpy.Exists(ResidualCon):
                        txtFile.write("Deleting: {}{}".format(ResidualCon, "\n"))
                        arcpy.Delete_management(ResidualCon)

                    ResidualCon = "{}{}Con_KernelD_1".format(out_gdb, "\\")
                    if arcpy.Exists(ResidualCon):
                        txtFile.write("Deleting: {}{}".format(ResidualCon, "\n"))
                        arcpy.Delete_management(ResidualCon)

                if arcpy.Exists(KDVector):
                    arcpy.AlterAliasName(KDVector, KDvAlias)

                arcpy.AddMessage(
                    "Finished: {2} - KD Raster = {0}{2} - KD Vector = {1}{2} Time - {3}{2}".format(KDRaster,
                                                                                                   KDVector, "\n",
                                                                                                   time.strftime(
                                                                                                       "%X")))
                txtFile.write(
                    "Finished: {2} - KD Raster = {0}{2} - KD Vector = {1}{2} Time - {3}{2}".format(KDRaster,
                                                                                                   KDVector, "\n",
                                                                                                   time.strftime(
                                                                                                       "%X")))
    finally:

        if HoldKDRaster == 'false' and arcpy.Exists(KDRaster):
            arcpy.Delete_management(KDRaster)

        arcpy.CheckInExtension("Spatial")
        arcpy.CheckInExtension("ImageAnalyst")
        txtFile.write(arcpy.GetMessages())
        txtFile.close()
