import os
import arcpy, time
from arcpy.sa import *
import sys

KDRaster = arcpy.GetParameterAsText(0)         # Required & Typed Raster Dataset needs to exist
PopRaster = arcpy.GetParameterAsText(1)        # Required and typed
InEvents = arcpy.GetParameterAsText(2)         # Required and typed This point Feature Layer will define the KDExtent and be the Events Selection Source NB DO NOT use a selection.
ThemeFilter = arcpy.GetParameterAsText(3)      # "ExclusionCode NOT IN (1) AND Incident_Year IN (2018, 2017, 2016, 2015, 2014, 2013, 2012, 2011, 2010, 2009) AND Incident_Residence_Match = 'No' "

KDvLevel = arcpy.GetParameterAsText(4)         # MinHotSpotCount
BufferSize = arcpy.GetParameterAsText(5)

DiffRaster = arcpy.GetParameterAsText(6)        # Optional (Input) if exists use if not exists build and save as: KDr_PopDiff
DiffKDv = arcpy.GetParameterAsText(7)           # Optional (Input) if exists use if not exists build and save as: KDv_PopDiff
TopicPoints = arcpy.GetParameterAsText(8)       # Optional (Input) if exists use if not exists build and save as: KDv_PopDiff

out_gdb = arcpy.GetParameterAsText(9)
LogFile = arcpy.GetParameterAsText(10)

HoldWorking = arcpy.GetParameterAsText(11)

# KDRaster = r'F:\BuildSuicideRasters\LITS20200729\Incidents_06_17.gdb\KDr_Incidents_Away_Tot_P_50_2k'
# InEvents = r"L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\Events"
# ThemeFilter = "ExclusionCode NOT IN (1) AND Incident_Year IN (2018, 2017, 2016, 2015, 2014, 2013, 2012, 2011, 2010, 2009) AND Incident_Residence_Match = 'No' "
# PopRaster = r'P:\BaseMaps\GNAF-AllGeoms-Population-ATS-2020\Population_KD_Resources.gdb\Rescale_Con_KD_ComboPopRes_Tot_P_50_2k'

# KDvLevel = 1
# BufferSize = '2.2 Kilometers'
#
# DiffRaster = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\KDr_PopDiff_incidents_50_2k'
# DiffKDv = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\KDv_PopDiff_Incidents_50_2k'
# TopicPoints = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb\Points_PopDiff_Incidents_50_2k'
#
# out_gdb = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\SuicideHotSpotBuilder.gdb'
# LogFile = r'L:\Work\Papers\NationalHotSpots\SuicideHotSpotBuilder\PK_KDMinusPop.txt'
# HoldWorking = False

if len(DiffRaster) > 0:
    DiffRaster = "{}\{}".format(out_gdb, DiffRaster)
else:
    DiffRaster = "{}\KDr_PopDiff".format(out_gdb)

if len(DiffKDv) > 0:
    DiffKDv = "{}\{}".format(out_gdb, DiffKDv)
else:
    DiffKDv = "{}\KDv_PopDiff".format(out_gdb)

# interim outputs that become procedural inputs
ConRaster = "{}\KDr_PopDiffCon".format(out_gdb)
OutPoints = "{}\HotSpots_PopDiff_{}".format(out_gdb, KDvLevel)
OutBuffers = "{}\HotBuffers_PopDiff_{}".format(out_gdb, KDvLevel)
OutZones = "{}\HotZones_PopDiff_{}".format(out_gdb, KDvLevel)
OutEvents = "{}\HotEvents_PopDiff_{}".format(out_gdb, KDvLevel)
InPoints = "{}\FilterPoints_{}".format(out_gdb, KDvLevel)

DiffRasterInt = "{}\KDr_PopDiff_Int".format(out_gdb)
DiffRasterRaw = "{}\KDr_PopDiff_Raw".format(out_gdb)
DifFRasterSlice = "{}\KDr_PopDiff_Slice".format(out_gdb)

OutCount = ""
KDextent = ""
DissolvedKDV = ""

txtFile = open(LogFile, "w")

txtFile.write("KDMinusPop Parameters {}".format("\n"))
txtFile.write("KDRaster = {}{}".format(KDRaster, "\n"))
txtFile.write("InEvents = {}{}".format(InEvents, "\n"))
txtFile.write("PopRaster = {}{}".format(PopRaster, "\n"))
txtFile.write("ThemeFilter = {}{}".format(ThemeFilter, "\n"))

txtFile.write("KDvLevel = {}{}".format(KDvLevel, "\n"))
txtFile.write("BufferSize = {}{}".format(BufferSize, "\n"))

txtFile.write("DiffRaster = {}{}".format(DiffRaster, "\n"))
txtFile.write("DiffKDv = {}{}".format(DiffKDv, "\n"))
txtFile.write("TopicPoints = {}{}".format(TopicPoints, "\n"))

txtFile.write("OutPoints = {}{}".format(OutPoints, "\n"))
txtFile.write("OutBuffers = {}{}".format(OutBuffers, "\n"))
txtFile.write("OutZones = {}{}".format(OutZones, "\n"))
txtFile.write("OutEvents = {}{}".format(OutEvents, "\n"))

txtFile.write("Scratch = {}{}".format(out_gdb, "\n"))
txtFile.write("LogFile = {}{}".format(LogFile, "\n"))
txtFile.write("HoldWorking = {}{}".format(HoldWorking, "\n"))

arcpy.env.overwriteOutput = True

with arcpy.EnvManager(scratchWorkspace=out_gdb, workspace=out_gdb):
    # arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(3857)
    # Parallel processing doesn't ALWAYS help!
    # parallel_processing = '12'

    arcpy.CheckOutExtension("spatial")
    arcpy.CheckOutExtension("ImageAnalyst")

    if not arcpy.Exists(out_gdb):
        gdb_path, gdb_name = os.path.split(out_gdb)
        arcpy.CreateFileGDB_management(gdb_path, gdb_name)

    try:

        if arcpy.Exists(KDRaster) and arcpy.Exists(PopRaster) and arcpy.Exists(InEvents):

            arcpy.SelectLayerByAttribute_management(InEvents,"CLEAR_SELECTION")
            arcpy.env.extent = 'MAXOF'

            if arcpy.Exists(DiffRaster):
                arcpy.AddMessage("Using Existing Raster Math Results: {}".format(DiffRaster))
                txtFile.write("Using Existing Raster Math Results: {}{}".format(DiffRaster, "\n"))

            else:

                arcpy.AddMessage("Raster Math Raster Does Not exist: {}".format(DiffRaster))
                txtFile.write("Raster Math Raster Does Not exist:  {}{}".format(DiffRaster, "\n"))

                if not arcpy.Exists(TopicPoints):
                    arcpy.AddMessage("Saving Points FC: {} from: {} - {}".format(TopicPoints,
                                                                                InEvents,
                                                                                time.strftime("%X")))
                    txtFile.write("Saving Points FC: {} from: {} - {}{}".format(TopicPoints,
                                                                                InEvents,
                                                                                time.strftime("%X"),
                                                                                "\n"))

                    filtered_points = arcpy.MakeFeatureLayer_management(in_features=InEvents,
                                                                        out_layer='lyr',
                                                                        where_clause=ThemeFilter)
                    arcpy.CopyFeatures_management(filtered_points,
                                                  TopicPoints)
                else:
                    arcpy.AddMessage("Using Existing Points FC: {} - {}".format(TopicPoints,
                                                                                time.strftime("%X")))
                    txtFile.write("Using Existing Points FC: {} - {}{}".format(TopicPoints,
                                                                               time.strftime("%X"),
                                                                               "\n"))
                    filtered_points = TopicPoints

                KDextent = "{}_Extent".format(DiffRaster)
                KDextent_tight = "{}_Extent_Tight".format(DiffRaster)

                if not arcpy.Exists(KDextent):
                    arcpy.AddMessage("Creating Extent: {} - {}".format(filtered_points,
                                                                       time.strftime("%X")))
                    txtFile.write("Creating Extent: {0} - {1}{2}".format(filtered_points,
                                                                         time.strftime("%X"),
                                                                         "\n"))

                    arcpy.management.MinimumBoundingGeometry(filtered_points, KDextent_tight, "ENVELOPE", "ALL")

                    arcpy.analysis.Buffer(in_features=KDextent_tight,
                                          out_feature_class=KDextent,
                                          buffer_distance_or_field="2 Kilometers",
                                          line_side="FULL",
                                          line_end_type="ROUND",
                                          dissolve_option="NONE",
                                          dissolve_field=None,
                                          method="PLANAR")

                    if arcpy.Exists(KDextent_tight):
                        arcpy.Delete_management(KDextent_tight)

                    arcpy.AddMessage("Extent Built: {0} - {1}{2}".format(KDextent,
                                                                         time.strftime("%X"),
                                                                         "\n"))
                    txtFile.write("Extent Built: {0} - {1}{2}".format(KDextent,
                                                                      time.strftime("%X"),
                                                                      "\n"))

                arcpy.env.extent = KDextent

                arcpy.AddMessage("Raster Math Extent: {}".format(KDextent))
                txtFile.write("Raster Math Extent: {}{}".format(KDextent, "\n"))

                arcpy.AddMessage("Calculating Raster Minus - {} - {}".format(KDRaster, PopRaster))
                txtFile.write("Calculating Raster Minus - {} - {}{}".format(KDRaster, PopRaster, "\n"))

                diff_result = arcpy.ia.Minus(KDRaster, PopRaster)

                arcpy.AddMessage("Calculating Raster Con - {}".format(ConRaster))
                txtFile.write("Calculating Raster Con - {}{}".format(ConRaster, "\n"))

                con_result = arcpy.sa.Con(diff_result, diff_result, None, "VALUE > 0")

                con_result.save(ConRaster)

                max_val = arcpy.GetRasterProperties_management(ConRaster, "MAXIMUM")

                with arcpy.EnvManager(compression="NONE", pyramid="NONE"):
                    arcpy.AddMessage("Rescaling DiffRaster - {} - {}".format(KDRaster, PopRaster))
                    txtFile.write("Rescaling DiffRaster  - {} - {}{}".format(KDRaster, PopRaster, "\n"))
                    out_raster = arcpy.sa.RescaleByFunction(in_raster=ConRaster,
                                                            transformation_function=[["LINEAR", 0, "", max_val, "", 0, max_val, ""]],
                                                            from_scale=0,
                                                            to_scale=20)

                    out_raster.save(DiffRasterRaw)

                    arcpy.AddMessage("Converting to Integer: {}".format(KDRaster))
                    txtFile.write("Converting to Integer: {}{}".format(KDRaster, "\n"))

                    # arcpy.Int_3d(out_raster, int_result)
                    # int_result.save(DiffRasterInt)

                    int_result = arcpy.sa.Slice(out_raster, 21, "NATURAL_BREAKS", 0);
                    int_result.save(DifFRasterSlice)

                    arcpy.AddMessage("Building KDv: {}".format(DiffKDv))
                    txtFile.write("Building KDv: {}{}".format(DiffKDv, "\n"))
                    kdv_result = arcpy.RasterToPolygon_conversion(in_raster=int_result,
                                                                  out_polygon_features=DiffKDv,
                                                                  simplify="NO_SIMPLIFY",
                                                                  raster_field="Value",
                                                                  create_multipart_features="SINGLE_OUTER_PART",
                                                                  max_vertices_per_feature=None)

                arcpy.Delete_management(con_result)

            arcpy.ClearEnvironment("extent")

            DissolvedKDV = "{}_Dissolved_{}".format(DiffKDv, KDvLevel)
            SelectExpression = "gridcode >= {}".format(KDvLevel)

            # featCount = arcpy.management.GetCount(DiffKDv)
            # arcpy.AddMessage("Count of Selected Features BEFORE: {}".format(featCount))
            # txtFile.write("Count of Selected Features BEFORE: {}{}".format(featCount, "\n"))

            arcpy.AddMessage("Selecting records where: {}".format(SelectExpression))
            txtFile.write("Selecting records where: {}{}".format(SelectExpression, "\n"))
            SelectedKDv = arcpy.management.SelectLayerByAttribute(in_layer_or_view = DiffKDv,
                                                                  selection_type = "NEW_SELECTION",
                                                                  where_clause = SelectExpression,
                                                                  invert_where_clause = None)

            # featCount = arcpy.management.GetCount(SelectedKDv)
            # arcpy.AddMessage("Count of Selected Features AFTER: {}".format(featCount))
            # txtFile.write("Count of Selected Features AFTER: {}{}".format(featCount, "\n"))

            arcpy.AddMessage("Dissolving Selected records {3}From: {0} {3}where: {1} {3}To: {2}".format(SelectedKDv, SelectExpression, DissolvedKDV, "\n"))
            txtFile.write("Dissolving Selected records {3}From: {0} {3}where: {1} {3}To: {2}{3}".format(SelectedKDv, SelectExpression, DissolvedKDV, "\n"))
            arcpy.gapro.DissolveBoundaries(SelectedKDv, DissolvedKDV, "SINGLE_PART")

            # HotPoints = DiffKDv.replace("KDv_", "HotPoints_")
            arcpy.AddMessage("Finding Centroids of Dissolved HotZones: {}".format(DissolvedKDV))
            txtFile.write("Finding Centroids of Dissolved HotZones: {}{}".format(DissolvedKDV, "\n"))
            HotSpotLayer = arcpy.management.FeatureToPoint(in_features = DissolvedKDV,
                                            out_feature_class = OutPoints,
                                            point_location = "CENTROID")

            arcpy.SetParameter(12, HotSpotLayer)

            # HotBuffers = DiffKDv.replace("KDv_", "HotBuffers_")
            arcpy.AddMessage("Building Buffers of HotPoints: {0}{2}- Size: {1}".format(OutBuffers, BufferSize, "\n"))
            txtFile.write("Building Buffers of HotPoints: {0}{2}- Size: {1}{2}".format(OutBuffers, BufferSize, "\n"))
            arcpy.analysis.Buffer(in_features = OutPoints,
                                  out_feature_class = OutBuffers,
                                  buffer_distance_or_field = BufferSize,
                                  line_side = "FULL",
                                  line_end_type = "ROUND",
                                  dissolve_option = "NONE",
                                  dissolve_field = None,
                                  method = "PLANAR")

            arcpy.AddMessage("Selecting Events from {0}{2} that are coincident with HotBuffers: {1}".format(filtered_points, OutBuffers, "\n"))
            txtFile.write("Selecting Events from {0}{2} that are coincident with HotBuffers: {1}{2}".format(filtered_points, OutBuffers, "\n"))
            SelectedEvents = arcpy.management.SelectLayerByLocation(in_layer = filtered_points,
                                                                    overlap_type = "WITHIN_A_DISTANCE",
                                                                    select_features = OutPoints,
                                                                    search_distance = BufferSize,
                                                                    selection_type = "NEW_SELECTION",
                                                                    invert_spatial_relationship = "NOT_INVERT")
            # filtered_points_sel = "SubSet_{}".format(filtered_points)
            arcpy.CopyFeatures_management(SelectedEvents, OutEvents)

            # OutEvents = DiffKDv.replace("KDv_", "HotEvents_"))
            arcpy.AddMessage("Intersecting (Identity) Events from {0}{3} with HotBuffers UID's: {1}{3}To: {2}".format(filtered_points, OutBuffers, OutEvents, "\n"))
            txtFile.write("Intersecting (Identity) Events from {0}{3} with HotBuffers UID's: {1}{3}To: {2}{3}".format(filtered_points, OutBuffers, OutEvents, "\n"))
            arcpy.analysis.Identity(in_features = SelectedEvents,
                                    identity_features = OutBuffers,
                                    out_feature_class = OutEvents,
                                    join_attributes = "ALL",
                                    cluster_tolerance = None,
                                    relationship = "NO_RELATIONSHIPS")

            OutCount = OutEvents.replace("HotEvents_", "EventsCount_")
            arcpy.AddMessage("Counting Events from {0}{3} with HotBuffers UID's: {1}{3}To: {2}".format(filtered_points, OutBuffers, OutEvents, "\n"))
            txtFile.write("Counting Events from {0}{3} with HotBuffers UID's: {1}{3}To: {2}{3}".format(filtered_points, OutBuffers, OutEvents, "\n"))
            arcpy.analysis.Statistics(in_table = OutEvents,
                                      out_table = OutCount,
                                      statistics_fields = [["OBJECTID", "COUNT"]],
                                      case_field = "ORIG_FID")

            arcpy.AddMessage("Finalizing HotPoints: {0}{2} with Frequencies from {1}".format(OutPoints, OutCount, "\n"))
            txtFile.write("Finalizing HotPoints: {0}{2} with Frequencies from {1}{2}".format(OutPoints, OutCount, "\n"))
            arcpy.management.JoinField(in_data = OutPoints,
                                       in_field = "ORIG_FID",
                                       join_table = OutCount,
                                       join_field = "ORIG_FID",
                                       fields = "FREQUENCY")

            # Removing 'undewhelming' HotSpots - Frequency < 5
            SelectExpression = "frequency < 5"
            arcpy.AddMessage("Selecting HotSpots from {} that represent fewer than 5 Events".format(OutPoints))
            txtFile.write("Selecting HotSpots from {} that represent fewer than 5 Events.{}".format(OutPoints, "\n"))
            SelectedSpots = arcpy.management.SelectLayerByAttribute(in_layer_or_view = OutPoints,
                                                                    selection_type = "NEW_SELECTION",
                                                                    where_clause = SelectExpression,
                                                                    invert_where_clause = None)

            DeletionCount = int(arcpy.GetCount_management(SelectedSpots)[0])
            if DeletionCount > 0:
                arcpy.AddMessage("Deleting {} underwhelming HotSpots".format(DeletionCount))
                txtFile.write("Deleting {} underwhelming HotSpots{}".format(DeletionCount, "\n"))
                arcpy.DeleteRows_management(SelectedSpots)

            arcpy.AddMessage("Selecting Events from {0}{2} that are coincident with Revised Hotspots: {1}".format(OutEvents, OutPoints, "\n"))
            txtFile.write("Selecting Events from {0}{2} that are coincident with Revised Hotspots: {1}{2}".format(OutEvents, OutPoints, "\n"))
            SelectedEvents = arcpy.management.SelectLayerByLocation(in_layer=OutEvents,
                                                                    overlap_type="WITHIN_A_DISTANCE",
                                                                    select_features=OutPoints,
                                                                    search_distance=BufferSize,
                                                                    selection_type="NEW_SELECTION",
                                                                    invert_spatial_relationship="INVERT")

            DeletionCount = int(arcpy.GetCount_management(SelectedEvents)[0])
            if DeletionCount > 0:
                arcpy.AddMessage("Deleting {} Events related to underwhelming HotSpots".format(DeletionCount))
                txtFile.write("Deleting {} Events related to underwhelming HotSpots{}".format(DeletionCount, "\n"))
                arcpy.DeleteRows_management(SelectedEvents)

            #arcpy.CopyFeatures_management(SelectedEvents, OutEventsTmp)

            arcpy.AddMessage("Revising Buffers of Revised Hotspots: {0}{2} - Size: {1}".format(OutBuffers, BufferSize, "\n"))
            txtFile.write("Revising Buffers of Revised Hotspots: {0}{2} - Size: {1}{2}".format(OutBuffers, BufferSize, "\n"))
            arcpy.analysis.Buffer(in_features=SelectedSpots,
                                  out_feature_class=OutBuffers,
                                  buffer_distance_or_field=BufferSize,
                                  line_side="FULL",
                                  line_end_type="ROUND",
                                  dissolve_option="NONE",
                                  dissolve_field=None,
                                  method="PLANAR")

            ZoneCountEvents = int(arcpy.GetCount_management(OutEvents)[0])
            ZoneCountHotSpots = int(arcpy.GetCount_management(OutPoints)[0])
            arcpy.AddMessage("Creating {} HotZones: representing: {} Events.{}".format(OutPoints, OutEvents, "\n"))
            txtFile.write("Creating {} HotZones: representing: {} Events.{}".format(OutPoints, OutCount, "\n"))
            arcpy.management.MinimumBoundingGeometry(OutEvents, OutZones, "ENVELOPE", "LIST", "ORIG_FID", "NO_MBG_FIELDS")

            arcpy.management.JoinField(in_data=OutEvents,
                                       in_field="ORIG_FID",
                                       join_table=OutCount,
                                       join_field="ORIG_FID",
                                       fields="FREQUENCY")

            arcpy.AddMessage("Selecting Events from {} that are duplicates from {} intersections".format(OutEvents, SelectExpression))
            txtFile.write("Selecting Events from {} that are duplicates from {} intersections{}".format(OutEvents, SelectExpression, "\n"))
            SelectedEvents = arcpy.management.SelectLayerByAttribute(in_layer_or_view=OutEvents,
                                                                      selection_type="NEW_SELECTION",
                                                                      where_clause=SelectExpression,
                                                                      invert_where_clause=None)

            DeletionCount = int(arcpy.GetCount_management(SelectedEvents)[0])
            if DeletionCount > 0:
                arcpy.AddMessage("Deleting {} Events related to underwhelming HotSpots".format(DeletionCount))
                txtFile.write("Deleting {} Events related to underwhelming HotSpots{}".format(DeletionCount, "\n"))
                arcpy.DeleteRows_management(SelectedEvents)

            arcpy.SetParameter(13, OutEvents)

            arcpy.AddMessage("Finished {8}"
                             " - Events Raster: {0} MINUS Population Raster: {1} = Differential Raster: {2}{8}"
                             " - Differential KDv: {3}{8}"
                             " - Collected HotSpot Events: {4}{8}"
                             " - Hot Spots: {5}{8}"
                             " - Hot Buffers: {6}{8}"
                             " - Hot Zones: {7}{8}".format(KDRaster, PopRaster, DiffRaster, DiffKDv, OutEvents, OutPoints, OutBuffers, OutZones, "\n"))
            txtFile.write("Finished {8}"
                             " - Events Raster: {0} MINUS Population Raster: {1} = Differential Raster: {2}{8}"
                             " - Differential KDv: {3}{8}"
                             " - Collected HotSpot Events: {4}{8}"
                             " - Hot Spots: {5}{8}"
                             " - Hot Buffers: {6}{8}"
                             " - Hot Zones: {7}{8}".format(KDRaster, PopRaster, DiffRaster, DiffKDv, OutEvents, OutPoints, OutBuffers, OutZones, "\n"))

        else:

            txtFile.write('{} or {} does not exist'.format(KDRaster, PopRaster))

    finally:

        if HoldWorking == "true":
            txtFile.write("Working Feature Classes Held:{4} - DiffRaster: {0}{4} - OutCount: {1}{4} - KDextent: {2}{4} - DissolvedKDV: {3}{4}".format(DiffRaster, OutCount, KDextent, DissolvedKDV, "\n"))
        else:
            if arcpy.Exists(DiffRaster):
                txtFile.write("Deleting: {}{}".format(DiffRaster, "\n"))
                arcpy.Delete_management(DiffRaster)

            if arcpy.Exists(OutCount):
                txtFile.write("Deleting: {}{}".format(OutCount, "\n"))
                arcpy.Delete_management(OutCount)

            if arcpy.Exists(KDextent):
                txtFile.write("Deleting: {}{}".format(KDextent, "\n"))
                arcpy.Delete_management(KDextent)

            if arcpy.Exists(DissolvedKDV):
                txtFile.write("Deleting: {}{}".format(DissolvedKDV, "\n"))
                arcpy.Delete_management(DissolvedKDV)

        txtFile.write(arcpy.GetMessages())
        txtFile.close()
