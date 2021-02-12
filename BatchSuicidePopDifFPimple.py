import os
import sys
import arcpy, time
from arcpy.sa import *
import logging

# Source Rasters
ResRastersGDB = r'F:\BuildSuicideRasters\LITS20200729\Residences_06_17.gdb'
IncRastersGDB = r'F:\BuildSuicideRasters\LITS20200729\Incidents_06_17.gdb'
PopRastersGDB = r'F:\BuildPopRasters\PopRasters.gdb'

LogFile = r'L:\Work\Papers\NationalHotSpots\SuicideVsPopulation\Log_BatchSuicideHotSpotBuilder.txt'

# Destinations
DiffRastersPath =  r'L:\Work\Papers\NationalHotSpots\SuicideVsPopulation'
out_gdb = r'L:\Work\Papers\NationalHotSpots\SuicideVsPopulation\SuicideHotSpotBuilder.gdb'

# Source Points
IncPoints = r'L:\CoreData\NCIS\LITS_20200729\NCIS.gdb\incidents_xy'
ResPoints = r'L:\CoreData\NCIS\LITS_20200729\NCIS.gdb\residence_xy'

# The Raster math conversion to vectors will deliver a vecorized raster in the gridcode range 1 - 20
# 20 represents the Most DIFFERENT locations where suicide event density exceeds population density.
# this script dissolves each of the gridcode vector polygon sets and identifies the events that contribute to the topic raster
# and ultimately to the Difference. The identification of Suicide Events is based on a buffer distance (BufferSize) around the centroids of the dissolved vectorized Difference raster.
# The hotspot count will increase with the broadening of the dissolve (gridcode - 20 - 1) and at some point the HotSpots satrt to lose relevance.
# This analyses is looking for the most relevant hotspots where events exceed underlying populations.
# The minimum hotspot count (MinHotSpotCount) for these analyses outputs should be set around 20 - 50 any more and we will be identifying increasingly marginal differences)

# BufferSize = "2.2 Kilometers"
MinHotSpotCount = 50

# configure default logging to file
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    filename=LogFile)

# add console logging
console_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(console_formatter)
logging.getLogger().addHandler(console)

# txtFile = open(LogFile, "w")

# genders = ['P', 'M', 'F']
genders = ['P']
scales = {'2k': '2 Kilometers', '1k': '1 Kilometers'}
# scales = {'1K': {'mDist': 1000,
#                  'searchradius': '1 Kilometers'},
#           '2K': {'mDist': 1000,
#                  'searchradius': '2 Kilometers'},
#           '4K': {'mDist': 4000,
#                  'searchradius': '4 Kilometers'}}
cellsizes = [50, 200, 100]
ages = ['Adult', 'Elder', 'Mature', 'MidAge', 'Youth', 'Tot']
# ages = ['Tot']
STypes = {'Incidents_Away': IncRastersGDB, 'Residences': ResRastersGDB}
# STypes = {'Residences': ResRastersGDB}

arcpy.env.overwriteOutput = True

if not arcpy.Exists(out_gdb):
    gdb_path, gdb_name = os.path.split(out_gdb)
    arcpy.CreateFileGDB_management(gdb_path, gdb_name)

with arcpy.EnvManager(scratchWorkspace=out_gdb, workspace=out_gdb):
    # arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(3857)
    # Parallel processing doesn't ALWAYS help!
    # parallel_processing = '12'

    arcpy.CheckOutExtension("spatial")
    arcpy.CheckOutExtension("ImageAnalyst")

    try:
        for SType, RastersGDB in STypes.items():
            for gender in genders:
                for scale, BufferSize in scales.items():
                    for cellsize in cellsizes:
                        for age in ages:

                            SuicideHotSpots = "SuicidePopDiff_{}_{}_{}".format(SType, age, gender)
                            DiffRastersGDB = "{}\{}.gdb".format(DiffRastersPath, SuicideHotSpots)
                            if not arcpy.Exists(DiffRastersGDB):
                                gdb_path, gdb_name = os.path.split(DiffRastersGDB)
                                arcpy.CreateFileGDB_management(gdb_path, gdb_name)

                            PointSet = "{}\{}_{}_{}".format(RastersGDB, SType, age, gender)
                            RasterTopic = '{}\KDr_{}_{}_{}_{}_{}'.format(RastersGDB, SType, age, gender, cellsize, scale)
                            RasterPop = '{}\KDr_PopRes_{}_{}_{}_{}'.format(PopRastersGDB, age, gender, cellsize, scale)
                            KDrAlias = "KDr_PopDiff_{}_{}_{}_{}_{}".format(SType, age, gender, cellsize, scale)
                            OutputKDr = "{}\{}".format(DiffRastersGDB, KDrAlias)
                            KDvAlias = "KDv_PopDiff_{}_{}_{}_{}_{}".format(SType, age, gender, cellsize, scale)
                            OutputKDv = "{}\{}".format(DiffRastersGDB, KDvAlias)

                            # txtMessage = "{7}Starting - Gender: {0} - Scale: {1} - CellSize: {2} - Age: {3}{7}" \
                            #              " - PointSet: {4}{7}" \
                            #              " - RasterTopic: {5}{7}" \
                            #              " - RasterPop: {6}{7}" \
                            #              " - Destination: {9}{7}" \
                            #              " - Start: {8}{7}".format(gender, scale, cellsize, age, PointSet, RasterTopic, RasterPop, "\n", time.strftime("%X"), OutputKDr)
                            # arcpy.AddMessage(txtMessage)
                            # txtFile.write(txtMessage)
                            txtMessage = "{7}** Starting - Gender: {0} - Scale: {1} - CellSize: {2} - Age: {3}{7}" \
                                         " - PointSet: {4}{7}" \
                                         " - RasterTopic: {5}{7}" \
                                         " - RasterPop: {6}{7}" \
                                         " - Destination: {8}{7}".format(gender, scale, cellsize, age, PointSet, RasterTopic, RasterPop, "\n", OutputKDr)
                            logging.info(txtMessage)

                            if arcpy.Exists(PointSet) and arcpy.Exists(RasterTopic) and arcpy.Exists(RasterPop):
                                logging.info("Ready to go, All the ingredients are available.")
                                logging.debug("Test Exists: {} - {} - {}".format(PointSet, RasterTopic, RasterPop))
                                # arcpy.AddMessage("")
                                # txtFile.write("Ready to go, All the ingredients are available.")
                            else:
                                logging.info("Missing bits, All the ingredients are NOT available.")
                                logging.debug("Test Exists: {} - {} - {}".format(PointSet, RasterTopic, RasterPop))
                                # arcpy.AddMessage("Missing bits, All the ingredients are NOT available.")
                                # txtFile.write("Missing bits, All the ingredients are NOT available.")
                                sys.exit(0)

                            arcpy.SelectLayerByAttribute_management(PointSet, "CLEAR_SELECTION")
                            arcpy.env.extent = 'MAXOF'

                            if arcpy.Exists(OutputKDv):
                                # arcpy.AddMessage("Using Existing Raster Math Results: {} - {}{}".format(OutputKDv, time.strftime("%X"), "\n"))
                                # txtFile.write("Using Existing Raster Math Results: {}- {}{}".format(OutputKDv, time.strftime("%X"), "\n"))
                                logging.info("Using Existing Raster Math Results: {}".format(OutputKDv))
                            else:
                                # Minus
                                # txtMessage = "Building Raster Math Rasters - Minus: {} - {}{}".format(OutputKDr, time.strftime("%X"), "\n")
                                # arcpy.AddMessage(txtMessage)
                                # txtFile.write(txtMessage)
                                logging.info("Building Raster Math Raster - Minus: {}".format(OutputKDr))

                                diff_result = arcpy.ia.Minus(RasterTopic, RasterPop)

                                # Con
                                ConRaster = "{}/ConRaster".format(out_gdb)
                                # txtMessage = "Building Raster Math Rasters - Con: {} - {}{}".format(ConRaster, time.strftime("%X"), "\n")
                                # arcpy.AddMessage(txtMessage)
                                # txtFile.write(txtMessage)
                                logging.info("Building Raster Math Raster - Con: {}".format(ConRaster))

                                con_result = arcpy.sa.Con(diff_result, diff_result, None, "VALUE > 0")
                                con_result.save(ConRaster)

                                # Rescale
                                max_val = arcpy.GetRasterProperties_management(ConRaster, "MAXIMUM")
                                with arcpy.EnvManager(compression="NONE", pyramid="NONE"):
                                    DiffRasterRaw = "{}/DiffRasterRaw".format(out_gdb)
                                    # txtMessage = "Building Raster Math Rasters - Rescale: {} - Max={} - {}{}".format(DiffRasterRaw, max_val, time.strftime("%X"), "\n")
                                    # arcpy.AddMessage(txtMessage)
                                    # txtFile.write(txtMessage)
                                    logging.info("Building Raster Math Raster - Rescale: {} - Max={}".format(DiffRasterRaw, max_val))
                                    out_raster = arcpy.sa.RescaleByFunction(in_raster=ConRaster,
                                                                            transformation_function=[["LINEAR", 0, "", max_val, "", 0, max_val, ""]],
                                                                            from_scale=0,
                                                                            to_scale=20)

                                    out_raster.save(DiffRasterRaw)

                                    # txtMessage = "Building Raster Math Rasters - Slice: {} - {}{}".format(OutputKDr, time.strftime("%X"), "\n")
                                    # arcpy.AddMessage(txtMessage)
                                    # txtFile.write(txtMessage)
                                    logging.info("Building Raster Math Raster - Slice: {}".format(OutputKDr))

                                    int_result = arcpy.sa.Slice(out_raster, 21, "NATURAL_BREAKS", 0)
                                    int_result.save(OutputKDr)

                                    # DifFRasterInt = "{}/DifFRasterInt".format(out_gdb)
                                    # arcpy.Int_3d(out_raster, DifFRasterInt)
                                    # int_result.save(DifFRasterInt)

                                # txtMessage = "Building KDv: {} - {}{}".format(OutputKDv, time.strftime("%X"), "\n")
                                # arcpy.AddMessage(txtMessage)
                                # txtFile.write(txtMessage)
                                logging.info("Building Vector from Raster: {}".format(OutputKDv))
                                kdv_result = arcpy.RasterToPolygon_conversion(in_raster=int_result,
                                                                              out_polygon_features=OutputKDv,
                                                                              simplify="NO_SIMPLIFY",
                                                                              raster_field="Value",
                                                                              create_multipart_features="SINGLE_OUTER_PART",
                                                                              max_vertices_per_feature=None)

                                arcpy.Delete_management(ConRaster)
                                arcpy.Delete_management(DiffRasterRaw)

                                # txtMessage = "Finished KDr: {} & KDv: {} - {}{}".format(OutputKDr, OutputKDv, time.strftime("%X"), "\n")
                                # arcpy.AddMessage(txtMessage)
                                # txtFile.write(txtMessage)


                            # txtMessage = "Building PopDiff HotSpots using: {} - {}{}".format(OutputKDv, time.strftime("%X"), "\n")
                            # arcpy.AddMessage(txtMessage)
                            # txtFile.write(txtMessage)
                            logging.info("Building PopDiff HotSpots using: {}".format(OutputKDv))

                            for KDvLevel in (15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1):

                                DissolvedKDV = "{}_Dissolved_{}".format(OutputKDv, KDvLevel)
                                SelectExpression = "gridcode >= {}".format(KDvLevel)

                                logging.info("{}** Evaluating {} where: {}".format("\n", OutputKDv, SelectExpression))

                                SelectedKDv = arcpy.management.SelectLayerByAttribute(in_layer_or_view=OutputKDv,
                                                                                      selection_type="NEW_SELECTION",
                                                                                      where_clause=SelectExpression,
                                                                                      invert_where_clause=None)

                                logging.info("Dissolving Selected Polygons From: {} where: {} To: {}".format(SelectedKDv, SelectExpression, DissolvedKDV))
                                arcpy.gapro.DissolveBoundaries(SelectedKDv, DissolvedKDV, "SINGLE_PART")

                                HotPoints = "{}_{}".format(OutputKDv.replace("KDv_", "HotSpots_"), KDvLevel)
                                # txtMessage = "Finding Centroids of Dissolved HotZones: {} - {}{}".format(DissolvedKDV, time.strftime("%X"), "\n")
                                # arcpy.AddMessage(txtMessage)
                                # txtFile.write(txtMessage)
                                logging.info(
                                    "Finding Centroids of Dissolved PopDiff-KDv: {}".format(SelectedKDv))
                                HotSpotLayer = arcpy.management.FeatureToPoint(in_features=DissolvedKDV,
                                                                               out_feature_class=HotPoints,
                                                                               point_location="CENTROID")

                                featCount = int(arcpy.GetCount_management(HotPoints)[0])

                                if featCount == 0:
                                    logging.info("Zero Selected records From: {} where: {} - Skip to next KDvLevel - {}".format(SelectedKDv, SelectExpression, (KDvLevel+1)))
                                    arcpy.SelectLayerByAttribute_management(OutputKDv, "CLEAR_SELECTION")
                                    if arcpy.Exists(HotPoints):
                                        logging.info("Deleting: {}".format(HotPoints))
                                        arcpy.Delete_management(HotPoints)
                                    if arcpy.Exists(DissolvedKDV):
                                        logging.info("Deleting: {}".format(DissolvedKDV))
                                        arcpy.Delete_management(DissolvedKDV)
                                    # Next KDvLevel
                                elif featCount < MinHotSpotCount:
                                    logging.info("{} Dissolved Polygons is less than {} where: {} - Skipping to next KDvLevel - {}".format(featCount, MinHotSpotCount, SelectExpression, (KDvLevel + 1)))
                                    arcpy.SelectLayerByAttribute_management(OutputKDv, "CLEAR_SELECTION")
                                    if arcpy.Exists(HotPoints):
                                        logging.info("Deleting: {}".format(HotPoints))
                                        arcpy.Delete_management(HotPoints)
                                    if arcpy.Exists(DissolvedKDV):
                                        logging.info("Deleting: {}".format(DissolvedKDV))
                                        arcpy.Delete_management(DissolvedKDV)
                                    # Next KDvLevel
                                else:
                                    logging.info("{} Dissolved Polygons are enough to Evaluate HotSpots. KDvLevel = {}".format(featCount, KDvLevel))
                                    HotBuffers = OutputKDv.replace("KDv_", "AllHotBuffers_")
                                    # txtMessage = "Building Buffers of HotPoints: {0}{3}- Size: {1} - {2}{3}".format(HotBuffers, BufferSize, time.strftime("%X"), "\n")
                                    # arcpy.AddMessage(txtMessage)
                                    # txtFile.write(txtMessage)
                                    logging.info("Building {} Buffers of {} HotSpots: {}".format(BufferSize, featCount, HotBuffers))
                                    arcpy.analysis.Buffer(in_features=HotPoints,
                                                          out_feature_class=HotBuffers,
                                                          buffer_distance_or_field=BufferSize,
                                                          line_side="FULL",
                                                          line_end_type="ROUND",
                                                          dissolve_option="NONE",
                                                          dissolve_field=None,
                                                          method="PLANAR")

                                    # OutPoints = "{}\HotSpots_PopDiff_{}".format(out_gdb, KDvLevel)
                                    OutEvents = "{}_{}".format(OutputKDv.replace("KDv_", "HotEvents_"), KDvLevel)
                                    # txtMessage = "Selecting Events from {0}{3} that are coincident with HotBuffers: {1} - {2}{3}".format(PointSet, HotBuffers, time.strftime("%X"), "\n")
                                    # arcpy.AddMessage(txtMessage)
                                    # txtFile.write(txtMessage)
                                    logging.info("Selecting Events from {} that are coincident with {} - HotBuffers: {}".format(PointSet, "\n", HotBuffers))
                                    SelectedEvents = arcpy.management.SelectLayerByLocation(in_layer=PointSet,
                                                                                            overlap_type="WITHIN_A_DISTANCE",
                                                                                            select_features=HotPoints,
                                                                                            search_distance=BufferSize,
                                                                                            selection_type="NEW_SELECTION",
                                                                                            invert_spatial_relationship="NOT_INVERT")
                                    # filtered_points_sel = "SubSet_{}".format(filtered_points)
                                    arcpy.CopyFeatures_management(SelectedEvents, OutEvents)

                                    # txtMessage = "Intersecting (Identity) Events from {0}{4} with HotBuffers UID's: {1}{4}To: {2} - {3}{4}".format(PointSet, HotBuffers, OutEvents, time.strftime("%X"), "\n")
                                    # arcpy.AddMessage(txtMessage )
                                    # txtFile.write(txtMessage)
                                    logging.info("Intersecting (Identity) Events from {0}{3} with HotBuffer's UIDs: {1}{3} To: {2}".format(PointSet, HotBuffers, OutEvents, "\n"))
                                    arcpy.analysis.Identity(in_features=SelectedEvents,
                                                            identity_features=HotBuffers,
                                                            out_feature_class=OutEvents,
                                                            join_attributes="ALL",
                                                            cluster_tolerance=None,
                                                            relationship="NO_RELATIONSHIPS")

                                    OutCount = OutEvents.replace("HotEvents_", "EventsCount_")
                                    # txtMessage = "Counting Events from {0}{4} with HotBuffers UID's: {1}{4}To: {2} - {3}{4}".format(HotPoints, HotBuffers, OutEvents, time.strftime("%X"), "\n")
                                    # arcpy.AddMessage(txtMessage)
                                    # txtFile.write(txtMessage)
                                    logging.info("Counting Events from {0}{3} with HotBuffers UID's: {1}{3} To: {2}".format(HotPoints, HotBuffers, OutEvents,"\n"))
                                    arcpy.analysis.Statistics(in_table=OutEvents,
                                                              out_table=OutCount,
                                                              statistics_fields=[["OBJECTID", "COUNT"]],
                                                              case_field="ORIG_FID")

                                    # OutPoints = "{}\HotSpots_PopDiff_{}".format(out_gdb, KDvLevel)

                                    # txtMessage = "Finalizing HotPoints: {0}{3} with Frequencies from {1} - {2}{3}".format(HotPoints, OutCount, time.strftime("%X"), "\n")
                                    # arcpy.AddMessage(txtMessage)
                                    # txtFile.write(txtMessage)
                                    logging.info("Finalizing HotPoints: {}{} with Frequencies from {}".format(HotPoints, "\n", OutCount))
                                    arcpy.management.JoinField(in_data=HotPoints,
                                                               in_field="ORIG_FID",
                                                               join_table=OutCount,
                                                               join_field="ORIG_FID",
                                                               fields="FREQUENCY")

                                    # Removing 'under-whelming' HotSpots - Frequency < 5
                                    SelectHotspots = "frequency < 5"
                                    # txtMessage = "Selecting HotSpots from {} that represent fewer than 5 Events - {}{}".format(HotPoints, time.strftime("%X"), "\n")
                                    # arcpy.AddMessage(txtMessage)
                                    # txtFile.write(txtMessage)
                                    logging.info("Selecting HotSpots from {} that represent fewer than 5 Events".format(HotPoints))
                                    SelectedSpots = arcpy.management.SelectLayerByAttribute(in_layer_or_view=HotPoints,
                                                                                            selection_type="NEW_SELECTION",
                                                                                            where_clause=SelectHotspots,
                                                                                            invert_where_clause=None)

                                    DeletionCount = int(arcpy.GetCount_management(SelectedSpots)[0])
                                    if DeletionCount > 0:
                                        # txtMessage = "Deleting {} underwhelming HotSpots - {}{}".format(DeletionCount, time.strftime("%X"), "\n")
                                        # arcpy.AddMessage(txtMessage)
                                        # txtFile.write(txtMessage)
                                        logging.info("Deleting {} underwhelming HotSpots".format(DeletionCount))
                                        arcpy.DeleteRows_management(SelectedSpots)

                                    HotSpotCount = int(arcpy.GetCount_management(HotPoints)[0])

                                    # txtMessage = "Selecting Events from {0}{3} that are coincident with Revised Hotspots: {1} - {2}{3}".format(OutEvents, HotPoints, time.strftime("%X"), "\n")
                                    # arcpy.AddMessage(txtMessage)
                                    # txtFile.write(txtMessage)

                                    # At this point we know the HotSpots and the Event Counts associated with each hotSpot
                                    # AND that the HotSpots left have a relationship with more than 5 events.

                                    # Rather than persist with Up tp 15 separate sets of HotEvents, HopSpots, HotBuffers, HotZones
                                    # we can test the record count for relevance!
                                    # and stepping backwards from gridcode = 15 to gridcode = 1 ignore this 'Level'
                                    # and delete the working analyses built so far  and step to the next level until we have exceeded a relevant set
                                    # at which point we can build the ancillary layers (HotZones, HotBuffers and escape from the KDvLevel for loop

                                    if HotSpotCount < MinHotSpotCount:
                                        # Not Enough HotSpots skip the ancillary files.

                                        logging.info("There are insufficient 'RELEVANT' HotSpots :{} so skipping this KDLevel (gridcode = {})".format(HotSpotCount, KDvLevel))

                                        # clear any selects
                                        arcpy.SelectLayerByAttribute_management(PointSet, "CLEAR_SELECTION")
                                        arcpy.SelectLayerByAttribute_management(HotPoints, "CLEAR_SELECTION")
                                        arcpy.SelectLayerByAttribute_management(OutputKDv, "CLEAR_SELECTION")
                                        arcpy.SelectLayerByAttribute_management(OutEvents, "CLEAR_SELECTION")

                                        # remove the KDvLevel layers that we are choosing to ignore
                                        if arcpy.Exists(HotPoints):
                                            logging.debug("Deleting: {}".format(HotPoints))
                                            arcpy.Delete_management(HotPoints)

                                        if arcpy.Exists(OutEvents):
                                            logging.debug("Deleting: {}".format(OutEvents))
                                            arcpy.Delete_management(OutEvents)

                                        # Remove temporary build layers
                                        if arcpy.Exists(OutCount):
                                            logging.debug("Deleting: {}".format(OutCount))
                                            arcpy.Delete_management(OutCount)

                                        if arcpy.Exists(DissolvedKDV):
                                            logging.debug("Deleting: {}".format(DissolvedKDV))
                                            arcpy.Delete_management(DissolvedKDV)

                                        if arcpy.Exists(HotBuffers):
                                            logging.debug("Deleting: {}".format(HotBuffers))
                                            arcpy.Delete_management(HotBuffers)

                                    else:

                                        logging.info("Selecting Events from {} that are coincident with Revised Hotspots: {}".format(OutEvents, HotPoints))
                                        SelectedEvents = arcpy.management.SelectLayerByLocation(in_layer=OutEvents,
                                                                                                overlap_type="WITHIN_A_DISTANCE",
                                                                                                select_features=HotPoints,
                                                                                                search_distance=BufferSize,
                                                                                                selection_type="NEW_SELECTION",
                                                                                                invert_spatial_relationship="INVERT")

                                        DeletionCount = int(arcpy.GetCount_management(SelectedEvents)[0])
                                        if DeletionCount > 0:
                                            # txtMessage = "Deleting {} Events related to underwhelming HotSpots - {}{}".format(DeletionCount, time.strftime("%X"), "\n")
                                            # arcpy.AddMessage(txtMessage)
                                            # txtFile.write(txtMessage)
                                            logging.info("Deleting {} Events related to underwhelming HotSpots".format(DeletionCount))
                                            arcpy.DeleteRows_management(SelectedEvents)

                                        # arcpy.CopyFeatures_management(SelectedEvents, OutEventsTmp)
                                        OutBuffers = "{}_{}".format(OutputKDv.replace("KDv_", "HotBuffers_"), KDvLevel)
                                        logging.info("Revising Buffers of Revised Hotspots: {} - Size: {}".format(OutBuffers, BufferSize))
                                        arcpy.analysis.Buffer(in_features=SelectedSpots,
                                                              out_feature_class=OutBuffers,
                                                              buffer_distance_or_field=BufferSize,
                                                              line_side="FULL",
                                                              line_end_type="ROUND",
                                                              dissolve_option="NONE",
                                                              dissolve_field=None,
                                                              method="PLANAR")

                                        ZoneCountEvents = int(arcpy.GetCount_management(OutEvents)[0])
                                        ZoneCountHotSpots = int(arcpy.GetCount_management(HotPoints)[0])

                                        OutZones = "{}_{}".format(OutputKDv.replace("KDv_", "HotZones_"), KDvLevel)
                                        # txtMessage = "Creating {} HotZones: representing: {} Events. - {}{}".format(HotPoints, OutEvents, time.strftime("%X"), "\n")
                                        # arcpy.AddMessage(txtMessage)
                                        logging.info("Creating {} HotZones: representing: {} Events.".format(HotPoints, OutEvents))
                                        arcpy.management.MinimumBoundingGeometry(OutEvents, OutZones, "ENVELOPE", "LIST",
                                                                                 "ORIG_FID", "NO_MBG_FIELDS")

                                        arcpy.management.JoinField(in_data=OutEvents,
                                                                   in_field="ORIG_FID",
                                                                   join_table=OutCount,
                                                                   join_field="ORIG_FID",
                                                                   fields="FREQUENCY")

                                        # txtMessage = "Selecting Events from {} that are duplicates from {} intersections - {}{}".format(OutEvents, SelectHotspots, time.strftime("%X"), "\n")
                                        # arcpy.AddMessage(txtMessage)
                                        # txtFile.write(txtMessage)
                                        logging.info("Selecting Events from {} that are duplicates from {} intersections".format(OutEvents, SelectHotspots))
                                        SelectedEvents = arcpy.management.SelectLayerByAttribute(in_layer_or_view=OutEvents,
                                                                                                 selection_type="NEW_SELECTION",
                                                                                                 where_clause=SelectHotspots,
                                                                                                 invert_where_clause=None)

                                        DeletionCount = int(arcpy.GetCount_management(SelectedEvents)[0])
                                        if DeletionCount > 0:
                                            # txtMessage = "Deleting {} Events related to underwhelming HotSpots - {}{}".format(DeletionCount, time.strftime("%X"), "\n")
                                            # arcpy.AddMessage(txtMessage)
                                            # txtFile.write(txtMessage)
                                            logging.info("Deleting {} Events related to underwhelming HotSpots.".format(DeletionCount))
                                            arcpy.DeleteRows_management(SelectedEvents)

                                        HotEventsCount = int(arcpy.GetCount_management(OutEvents)[0])

                                        # If the HotSpot count was sufficient then the principal layers: HotSpots and HotEvents are already saved and the ancillary layers (HotZones, HotBuffers, are saved too.
                                        # little point in cycling through another KDLevel so get out.
                                        logging.info("{8}** Completed {7} - Exiting this KdLevel Loop Results:{8}"
                                                     " - KDLevel = {0}{8}"
                                                     " - Collected HotSpot Events: {1} - # {2}{8}"
                                                     " - Hot Spots: {3} - # {4}{8}"
                                                     " - Hot Buffers: {5}{8}"
                                                     " - Hot Zones: {6}{8}".format(KDvLevel, OutEvents, HotEventsCount, HotPoints, HotSpotCount, OutBuffers, OutZones, OutputKDv, "\n"))

                                        arcpy.SelectLayerByAttribute_management(PointSet, "CLEAR_SELECTION")
                                        arcpy.SelectLayerByAttribute_management(HotPoints, "CLEAR_SELECTION")
                                        arcpy.SelectLayerByAttribute_management(OutputKDv, "CLEAR_SELECTION")
                                        arcpy.SelectLayerByAttribute_management(OutEvents, "CLEAR_SELECTION")

                                        if arcpy.Exists(HotBuffers):
                                            logging.info("Deleting: {}".format(HotBuffers))
                                            arcpy.Delete_management(HotBuffers)

                                        if arcpy.Exists(OutCount):
                                            logging.info("Deleting: {}".format(OutCount))
                                            arcpy.Delete_management(OutCount)

                                        if arcpy.Exists(DissolvedKDV):
                                            logging.info("Deleting: {}".format(DissolvedKDV))
                                            arcpy.Delete_management(DissolvedKDV)

                                        break

                                    # If the HotSpot Count was deemed too low we will be here without having built the ancillary files
                                    # otherwise we will be here after building the ancilary files we need to loop or break
                                    # If we are here on our way to the next KDvLevel we just need to clear selections and remove the primary layers that have been built along the way

                            arcpy.ClearEnvironment("extent")
                            # Next Theme Loop

    finally:

        logging.info("{0}** Finished - All Themes: - HotSpots, HotEvents, HotBuffers, HotZones and the KDr's & KDv's can be found here:{0}{1}".format("\n", DiffRastersPath))

        arcpy.CheckInExtension("Spatial")
        arcpy.CheckInExtension("ImageAnalyst")
        # txtFile.write(arcpy.GetMessages())
        # txtFile.close()

