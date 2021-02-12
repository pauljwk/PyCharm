mport
arcpy
import os
import sys
from arcpy.sa import *

arcpy.ImportToolbox(r'G:\Working\ArcMap Stuff\pkRasterHelper.tbx')

KDRaster = arcpy.GetParameterAsText(0)
InEvents = arcpy.GetParameterAsText(
    1)  # Required and typed This Feature Layer needs to be either pre-selected to match the KDRaster or a Feature Class that was the underpinning of the KDRaster.
PopRaster = arcpy.GetParameterAsText(2)  # Required and typed
ConRaster = arcpy.GetParameterAsText(3)  # ConRaster Optional save or delete working

KDvLevel = arcpy.GetParameterAsText(4)
BufferSize = arcpy.GetParameterAsText(5)

DiffRaster = arcpy.GetParameterAsText(6)
DiffKDv = arcpy.GetParameterAsText(7)

OutPoints = arcpy.GetParameterAsText(8)
OutBuffers = arcpy.GetParameterAsText(9)
OutZones = arcpy.GetParameterAsText(10)
OutEvents = arcpy.GetParameterAsText(11)

out_gdb = arcpy.GetParameterAsText(12)
LogFile = arcpy.GetParameterAsText(13)

# KDRaster = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\KD_incidents_xy_50_2k'
# InEvents = r"L:\CoreData\NCIS\LITS_20200729\NCIS.gdb\Incidents_xy"
# PopRaster = r'P:\BaseMaps\GNAF-AllGeoms-Population-ATS-2020\Population_KD_Resources.gdb\Rescale_Con_KD_ComboPopRes_Tot_P_50_2k'
#
# KDvLevel = 1
# BufferSize = '2.2 Kilometers'
#
# DiffRaster = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\KDr_PopDiff_incidents_50_2k'
# DiffKDv = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\KDv_PopDiff_Incidents_50_2k'
#
# OutPoints = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\HotPoints_PopDiff_Incidents_50_2k'
# OutBuffers = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\HotBuff_PopDiff_Incidents_50_2k'
# OutZones = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\HotZones_PopDiff_Incidents_50_2k'
# OutEvents = r"L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\HotEvents_PopDiff_Incidents_50_2k"
#
# out_gdb = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb'
# LogFile = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\PK_KDMinusPop.txt'

txtFile = open(LogFile, "w")

txtFile.write("KDMinusPop Parameters {}".format("\n"))
txtFile.write("KDRaster = {}{}".format(KDRaster, "\n"))
txtFile.write("InEvents = {}{}".format(InEvents, "\n"))
txtFile.write("PopRaster = {}{}".format(PopRaster, "\n"))
txtFile.write("KDvLevel = {}{}".format(KDvLevel, "\n"))
txtFile.write("BufferSize = {}{}".format(BufferSize, "\n"))

txtFile.write("DiffRaster = {}{}".format(DiffRaster, "\n"))
txtFile.write("DiffKDv = {}{}".format(DiffKDv, "\n"))

txtFile.write("OutPoints = {}{}".format(OutPoints, "\n"))
txtFile.write("OutBuffers = {}{}".format(OutBuffers, "\n"))
txtFile.write("OutZones = {}{}".format(OutZones, "\n"))
txtFile.write("OutEvents = {}{}".format(OutEvents, "\n"))

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

        if arcpy.Exists(KDRaster) and arcpy.Exists(PopRaster):

            ConExtent = "Con_{}".format(DiffRaster)
            KDextent = arcpy.management.MinimumBoundingGeometry(in_features=InEvents,
                                                                out_feature_class=ConExtent,
                                                                "ENVELOPE",
                                                                "NONE")
            arcpy.env.extent = KDExtent

            arcpy.AddMessage("Calculating Raster Minus - {} - {}".format(KDRaster, PopRaster))
            txtFile.write("Calculating Raster Minus - {} - {}{}".format(KDRaster, PopRaster, "\n"))
            diff_result = arcpy.ia.Minus(KDRaster, PopRaster)

            if not arcpy.Exists(ConRaster):
                arcpy.AddMessage("Calculating Raster Con - {} - {}".format(KDRaster, PopRaster))
                txtFile.write("Calculating Raster Con - {} - {}{}".format(KDRaster, PopRaster, "\n"))
                con_result = Con(diff_result, diff_result, None, "VALUE > 0")

                con_result.save(ConRaster)

            max_val = arcpy.GetRasterProperties_management(ConRaster, "MAXIMUM")

            with arcpy.EnvManager(compression="NONE", pyramid="NONE"):
                arcpy.AddMessage("Rescaling DiffRaster - {} - {}".format(KDRaster, PopRaster))
                txtFile.write("Rescaling DiffRaster  - {} - {}{}".format(KDRaster, PopRaster, "\n"))
                out_raster = arcpy.sa.RescaleByFunction(in_raster=ConRaster,
                                                        transformation_function=[
                                                            ["LINEAR", 0, "", max_val, "", 0, max_val, ""]],
                                                        from_scale=0,
                                                        to_scale=20)

                # out_raster.save(KDRaster)
                #
                # arcpy.AddMessage("Finished PopDiff Raster - {}".format(KDRaster))
                # txtFile.write("Finished PopDiff Raster - {}{}".format(KDRaster, "\n"))

                # arcpy.AddMessage("Converting to Integer: {}".format(KDRaster))
                # txtFile.write("Converting to Integer: {}{}".format(KDRaster, "\n"))

                int_result = Int(out_raster)
                int_result.save(DiffRaster)

                arcpy.AddMessage("Building KDv: {}".format(DiffKDv))
                txtFile.write("Building KDv: {}{}".format(DiffKDv, "\n"))
                kdv_result = arcpy.RasterToPolygon_conversion(in_raster=int_result,
                                                              out_polygon_features=DiffKDv,
                                                              simplify="NO_SIMPLIFY",
                                                              raster_field="Value",
                                                              create_multipart_features="SINGLE_OUTER_PART",
                                                              max_vertices_per_feature=None)

                # SelectExpression = "gridcode >= {}".format(KDvLevel)
                # arcpy.AddMessage("Selecting records where: {}".format(SelectExpression))
                # txtFile.write("Selecting records where: {}{}".format(SelectExpression, "\n"))
                # arcpy.management.SelectLayerByAttribute(in_layer_or_view=kdv_result,
                #                                         selection_type="NEW_SELECTION",
                #                                         where_clause=SelectExpression,
                #                                         invert_where_clause=None)
                #
                # DissolvedKDV = "{}_Dissolved_{}".format(DiffKDv, KDvLevel)
                # arcpy.AddMessage("Dissolving: {} to {}".format(DiffKDv, DissolvedKDV))
                # txtFile.write("Dissolving: {} to {}{}".format(DiffKDv, DissolvedKDV, "\n"))
                # arcpy.gapro.DissolveBoundaries(kdv_result, DissolvedKDV, "SINGLE_PART")

                DissolvedKDV = "{}_Dissolved_{}".format(DiffKDv, KDvLevel)
                SelectExpression = "gridcode >= {}".format(KDvLevel)
                arcpy.AddMessage("Dissolving Selected records {3}From: {0} {3}where: {1} {3}To: {2}".format(DiffKDv,
                                                                                                            SelectExpression,
                                                                                                            DissolvedKDV,
                                                                                                            "\n"))
                txtFile.write("Dissolving Selected records {3}From: {0} {3}where: {1} {3}To: {2}{3}".format(DiffKDv,
                                                                                                            SelectExpression,
                                                                                                            DissolvedKDV,
                                                                                                            "\n"))
                arcpy.pkRasterHelper.Dissolve_Selected_KDv(DiffKDv, DissolvedKDV, SelectExpression)

                # HotPoints = DiffKDv.replace("KDv_", "HotPoints_")
                arcpy.AddMessage("Finding Centroids of Dissolved HotZones: {}".format(DissolvedKDV))
                txtFile.write("Finding Centroids of Dissolved HotZones: {}{}".format(DissolvedKDV, "\n"))
                arcpy.management.FeatureToPoint(in_features=DissolvedKDV,
                                                out_feature_class=OutPoints,
                                                point_location="CENTROID")

                # HotBuffers = DiffKDv.replace("KDv_", "HotBuffers_")
                arcpy.AddMessage(
                    "Building Buffers of HotPoints: {0}{2}- Size: {1}".format(OutBuffers, BufferSize, "\n"))
                txtFile.write(
                    "Building Buffers of HotPoints: {0}{2}- Size: {1}{2}".format(OutBuffers, BufferSize, "\n"))
                arcpy.analysis.Buffer(in_features=OutPoints,
                                      out_feature_class=OutBuffers,
                                      buffer_distance_or_field=BufferSize,
                                      line_side="FULL",
                                      line_end_type="ROUND",
                                      dissolve_option="NONE",
                                      dissolve_field=None,
                                      method="PLANAR")

                arcpy.AddMessage(
                    "Selecting Events from {0}{2} that are coincident with HotBuffers: {1}".format(InEvents, OutBuffers,
                                                                                                   "\n"))
                txtFile.write(
                    "Selecting Events from {0}{2} that are coincident with HotBuffers: {1}{2}".format(InEvents,
                                                                                                      OutBuffers, "\n"))
                Selection = arcpy.management.SelectLayerByLocation(in_layer=InEvents,
                                                                   overlap_type="WITHIN_A_DISTANCE",
                                                                   select_features=OutPoints,
                                                                   search_distance=BufferSize,
                                                                   selection_type="SUBSET_SELECTION",
                                                                   invert_spatial_relationship="NOT_INVERT")
                # InEvents_sel = "SubSet_{}".format(InEvents)
                # arcpy.CopyFeatures_management(Selection, InEvents_sel)

                # OutEvents = DiffKDv.replace("KDv_", "HotEvents_"))
                arcpy.AddMessage(
                    "Intersecting (Identity) Events from {0}{3} with HotBuffers UID's: {1}{3}To: {2}".format(InEvents,
                                                                                                             OutBuffers,
                                                                                                             OutEvents,
                                                                                                             "\n"))
                txtFile.write(
                    "Intersecting (Identity) Events from {0}{3} with HotBuffers UID's: {1}{3}To: {2}{3}".format(
                        InEvents, OutBuffers, OutEvents, "\n"))
                arcpy.analysis.Identity(in_features=Selection,
                                        identity_features=OutBuffers,
                                        out_feature_class=OutEvents,
                                        join_attributes="ALL",
                                        cluster_tolerance=None,
                                        relationship="NO_RELATIONSHIPS")

                OutCount = DiffKDv.replace("KDv_", "EventsCount_")
                arcpy.AddMessage(
                    "Counting Events from {0}{3} with HotBuffers UID's: {1}{3}To: {2}".format(InEvents, OutBuffers,
                                                                                              OutEvents, "\n"))
                txtFile.write(
                    "Counting Events from {0}{3} with HotBuffers UID's: {1}{3}To: {2}{3}".format(InEvents, OutBuffers,
                                                                                                 OutEvents, "\n"))
                arcpy.analysis.Statistics(in_table=OutEvents,
                                          out_table=OutCount,
                                          statistics_fields=[["OBJECTID", "COUNT"]],
                                          case_field="ORIG_FID")

                arcpy.AddMessage(
                    "Finalizing HotPoints: {0}{2} with Frequencies from {1}".format(OutPoints, OutCount, "\n"))
                txtFile.write(
                    "Finalizing HotPoints: {0}{2} with Frequencies from {1}{2}".format(OutPoints, OutCount, "\n"))
                arcpy.management.JoinField(in_data=OutPoints,
                                           in_field="ORIG_FID",
                                           join_table=OutCount,
                                           join_field="ORIG_FID",
                                           fields="FREQUENCY")

                arcpy.AddMessage("Finished {8}"
                                 " - Events Raster: {0} Minus Population Raster: {1} = Differential Raster: {2}{8}"
                                 " - Differential KDv: {3}{8}"
                                 " - Collected HotSpot Events: {4}{8}"
                                 " - Hot Spots: {5}{8}"
                                 " - Hot Buffers: {6}{8}"
                                 " - Hot Zones: {7}{8}"
                                 .format(KDRaster, PopRaster, DiffRaster, DiffKDv, OutEvents, OutPoints, OutBuffers,
                                         OutZones, "\n"))
                txtFile.write("Finished {8}"
                              " - Events Raster: {0} Minus Population Raster: {1} = Differential Raster: {2}{8}"
                              " - Differential KDv: {3}{8}"
                              " - Collected HotSpot Events: {4}{8}"
                              " - Hot Spots: {5}{8}"
                              " - Hot Buffers: {6}{8}"
                              " - Hot Zones: {7}{8}"
                              .format(KDRaster, PopRaster, DiffRaster, DiffKDv, OutEvents, OutPoints, OutBuffers,
                                      OutZones, "\n"))

        else:

            txtFile.write('{} or {} does not exist'.format(KDRaster, PopRaster))

    finally:

        # arcpy.Delete_management(con_result)
        # arcpy.Delete_management(min_raster)
        # arcpy.Delete_management(con_raster)
        # arcpy.Delete_management(rescale_raster)

        txtFile.write(arcpy.GetMessages())
        txtFile.close()
