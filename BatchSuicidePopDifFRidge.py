import os
import sys
import arcpy, time
from arcpy.sa import *
import logging

# Source Rasters
ResRastersGDB = r'F:\BuildSuicideRasters\LITS20200729\Residences_06_17.gdb'
IncRastersGDB = r'F:\BuildSuicideRasters\LITS20200729\Incidents_06_17.gdb'
PopRastersGDB = r'F:\BuildPopRasters\PopRasters.gdb'

LogFile = r'L:\Work\Papers\NationalHotSpots\SuicideVsPopulation\Log_BatchSuicideHotSpotBuilder-Ridge.txt'

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
# cellsizes = [50, 100, 200]
cellsizes = [50]
ages = ['Tot', 'Adult', 'Elder', 'Mature', 'MidAge', 'Youth']
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

                            txtMessage = "{7}** Starting - Gender: {0} - Scale: {1} - CellSize: {2} - Age: {3}{7}" \
                                         " - PointSet: {4}{7}" \
                                         " - RasterTopic: {5}{7}" \
                                         " - RasterPop: {6}{7}" \
                                         " - Destination: {8}{7}".format(gender, scale, cellsize, age, PointSet, RasterTopic, RasterPop, "\n", OutputKDr)
                            logging.info(txtMessage)

                            if arcpy.Exists(PointSet) and arcpy.Exists(RasterTopic) and arcpy.Exists(RasterPop):
                                logging.info("Ready to go, All the ingredients are available.")
                                logging.debug("Test Exists: {} - {} - {}".format(PointSet, RasterTopic, RasterPop))
                            else:
                                logging.info("Missing bits, All the ingredients are NOT available.")
                                logging.debug("Test Exists: {} - {} - {}".format(PointSet, RasterTopic, RasterPop))

                                sys.exit(0)

                            arcpy.SelectLayerByAttribute_management(PointSet, "CLEAR_SELECTION")
                            arcpy.env.extent = 'MAXOF'

                            if arcpy.Exists(OutputKDv):
                                logging.info("Using Existing Raster Math Results: {}".format(OutputKDv))
                            else:
                                # Minus
                                logging.info("Building Raster Math Raster - Minus: {}".format(OutputKDr))

                                diff_result = arcpy.ia.Minus(RasterTopic, RasterPop)

                                # Con
                                ConRaster = "{}/ConRaster".format(out_gdb)
                                logging.info("Building Raster Math Raster - Con: {}".format(ConRaster))

                                con_result = arcpy.sa.Con(diff_result, diff_result, None, "VALUE > 0")
                                con_result.save(ConRaster)

                                # Rescale
                                max_val = arcpy.GetRasterProperties_management(ConRaster, "MAXIMUM")
                                with arcpy.EnvManager(compression="NONE", pyramid="NONE"):
                                    DiffRasterRaw = "{}/DiffRasterRaw".format(out_gdb)
                                    logging.info("Building Raster Math Raster - Rescale: {} - Max={}".format(DiffRasterRaw, max_val))
                                    out_raster = arcpy.sa.RescaleByFunction(in_raster=ConRaster,
                                                                            transformation_function=[["LINEAR", 0, "", max_val, "", 0, max_val, ""]],
                                                                            from_scale=0,
                                                                            to_scale=20)

                                    out_raster.save(DiffRasterRaw)

                                    logging.info("Building Raster Math Raster - Slice: {}".format(OutputKDr))

                                    int_result = arcpy.sa.Slice(out_raster, 21, "NATURAL_BREAKS", 0)
                                    int_result.save(OutputKDr)

                                    # DifFRasterInt = "{}/DifFRasterInt".format(out_gdb)
                                    # arcpy.Int_3d(out_raster, DifFRasterInt)
                                    # int_result.save(DifFRasterInt)

                                logging.info("Building Vector from Raster: {}".format(OutputKDv))
                                kdv_result = arcpy.RasterToPolygon_conversion(in_raster=int_result,
                                                                              out_polygon_features=OutputKDv,
                                                                              simplify="NO_SIMPLIFY",
                                                                              raster_field="Value",
                                                                              create_multipart_features="SINGLE_OUTER_PART",
                                                                              max_vertices_per_feature=None)

                                arcpy.Delete_management(ConRaster)
                                arcpy.Delete_management(DiffRasterRaw)

                            logging.info("*** Building PopDiff HotSpots using: {} ***".format(OutputKDv))

                            for KDvLevel in ( 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1):

                                arcpy.SelectLayerByAttribute_management(PointSet, "CLEAR_SELECTION")
                                arcpy.SelectLayerByAttribute_management(OutputKDv, "CLEAR_SELECTION")
                                arcpy.env.extent = 'MAXOF'

                                # The Building of HotSpots / HotZones is based on determining whether there are sufficient events within the Pek Regions
                                # as defined by dissolving the gridcode layers of the KDv Theme PopDiff.
                                # The gridcode layers represent the peak regions (where the event kernel density exceeds the population kernel density
                                # the Theme KDv have layers where grid code is rated as 1 - 20 where 20 is extreme variation and 1 is marginal variation
                                # we start at 15 because the are always very few regions in the 16-20 gridcode levels.

                                # Step 1: Determine the Peak Regions for this pass
                                # Step 1a. Select all the Peak Region polygons in the KDvLevel (1 - 20)
                                SelectExpression = "gridcode >= {}".format(KDvLevel)
                                logging.info("{}** Evaluating {} where: {}".format("\n", OutputKDv, SelectExpression))
                                SelectedKDv = arcpy.management.SelectLayerByAttribute(in_layer_or_view=OutputKDv,
                                                                                      selection_type="NEW_SELECTION",
                                                                                      where_clause=SelectExpression,
                                                                                      invert_where_clause=None)

                                # Step 1b. Disolve thes selection into a single polygon layer

                                DissolvedKDv = "DKDv_{}".format(KDvLevel)
                                logging.info("Dissolving Selected Polygons From: {} where: {} To: {}".format(SelectedKDv, SelectExpression, DissolvedKDv))
                                arcpy.gapro.DissolveBoundaries(SelectedKDv, DissolvedKDv, "SINGLE_PART")

                                # now all the 'peak regions' are in a single polygon layer with individual polygons for each 'peak' NB At KDvlevel 1 there will be many many polygons and at KDvLevel 15 there will be very very few
                                # Before we populate the peak regions it is important to note that the peaks are the tips of icebergs that exist as a result of proximal events.
                                # On that basis we need to 'expand' our reach to gather together any contributing events to ensure that we are getting the full measure of events that contribute to thes zones
                                # of high variation. So a simple buffer, equivalent to the search radius should round up all the import eveb=nts for these peak regions.
                                # the next steps will populate the polygons with candidate points and then we will cluster those candidate points based on:
                                # cluster distance equivalent to the base raster search radius (BufferSize) and minimun cluster count = 5.
                                # We need to select the clustered events (HotEvents)
                                # Then we can build a new set of polygons based on the Minimum bounding geometry for each of the clusters (HotZones)
                                # The next step will be to evaluate how many HotZones there are.
                                # Too many will include marginal variations between events and population.
                                # If there are fewer than the nominated parameter: MinHotSpotCount we should abandon the evaluation analyses layers and step down to the next KDvLevel

                                # Step 2: Round up all the events i=that contribute to the peak regions
                                # Step 2a: buffer the peak region polygons

                                # This referewnce structure is deliberate for later use!

                                PeakRegionRef = "{}_{}".format(KDvAlias.replace("KDv_", "Ridge_PeakRegions_"),KDvLevel)
                                PeakRegions = "{}\{}".format(DiffRastersGDB, PeakRegionRef)
                                logging.info(
                                    "Buffering Peak Regions: {0} by: {1} To: {2}".format(
                                        DissolvedKDv, PeakRegions, BufferSize))
                                arcpy.analysis.Buffer(in_features=DissolvedKDv,
                                                      out_feature_class=PeakRegions,
                                                      buffer_distance_or_field=BufferSize,
                                                      line_side="FULL",
                                                      line_end_type="ROUND",
                                                      dissolve_option="NONE",
                                                      dissolve_field=None,
                                                      method="PLANAR")

                                # Step 2b. intersect the points with the peak regions (dissolved polygons)

                                OutEventsx = "HotEventsI_{}".format(KDvLevel)
                                    # "{}_{}".format(OutputKDv.replace("KDv_", "Tmp_HotEvents_"), KDvLevel)
                                logging.info("Intersecting (Identity) Events from: {0} within Peak Regions: {1} To: {2}".format(PointSet, PeakRegions, OutEventsx))
                                arcpy.analysis.Identity(in_features=PointSet,
                                                        identity_features=PeakRegions,
                                                        out_feature_class=OutEventsx,
                                                        join_attributes="ALL",
                                                        cluster_tolerance=None,
                                                        relationship="NO_RELATIONSHIPS")

                                # Step 3: Assemble the events associated with the peak regions
                                # Step 3a. Select the events that fall within the peak regions:
                                SelectPeaks = "FID_{} > 0".format(PeakRegionRef)
                                logging.info("Selecting Events from: {0} within Peak Regions: {1} where: {2}".format(OutEventsx, PeakRegions, SelectPeaks))
                                SelectedEvents = arcpy.management.SelectLayerByAttribute(in_layer_or_view=OutEventsx,
                                                                                         selection_type="NEW_SELECTION",
                                                                                         where_clause=SelectPeaks,
                                                                                         invert_where_clause=None)

                                # Step 3b. Determine the Cluster Candidate Events from the events in the peak regions
                                # https://pro.arcgis.com/en/pro-app/latest/tool-reference/spatial-statistics/densitybasedclustering.htm
                                # NB the min features is locked in as 5 in this script.
                                # NB the search distance (required for DBSCAN method is set to match the underlying search radius of the rasters that delivered the PopDiff Layers.
                                # NB the cluster_sensitvity = None results in
                                # Kullback-Leibler divergence that finds the value where adding more clusters does not add additional information.

                                ClusterLayer = "HCE_{}".format(KDvLevel)
                                arcpy.stats.DensityBasedClustering(in_features = SelectedEvents,
                                                                   output_features = ClusterLayer,
                                                                   cluster_method = "DBSCAN",
                                                                   min_features_cluster = 5,
                                                                   search_distance = BufferSize,
                                                                   cluster_sensitivity = None)

                                # Step 4: Identify how many HotZones we have now.
                                # Step 4a. Select the events that Cluster into groups greater than 5 that fall within the peak regions:
                                SelectClusters = "CLUSTER_ID > 0"
                                SelectedClusterEvents = arcpy.management.SelectLayerByAttribute(in_layer_or_view=ClusterLayer,
                                                                                                selection_type="NEW_SELECTION",
                                                                                                where_clause=SelectClusters,
                                                                                                invert_where_clause=None)

                                # Step 4b. Build Minimum Bounding Geometries around the Peak Region Clustered Events
                                OutZones = "HotZones_{}".format(KDvLevel)
                                    # "{}_{}".format(OutputKDv.replace("KDv_", "Tmp_HotZones_"), KDvLevel)
                                arcpy.management.MinimumBoundingGeometry(SelectedClusterEvents, OutZones, "RECTANGLE_BY_AREA", "LIST", "CLUSTER_ID", "MBG_FIELDS")

                                # Step 4c. Count the HotZones
                                featCount = int(arcpy.GetCount_management(OutZones)[0])
                                eventCount = int(arcpy.GetCount_management(SelectedClusterEvents)[0])

                                # Step 5: Determine whether this collection of HotZones is appropriate based on the MinHotSpotCount parameter
                                if featCount >= MinHotSpotCount or KDvLevel == 1:
                                    logging.info(
                                        "There are {} HotZones containing {} HotEvents at Level {} ".format(featCount,
                                                                                                            eventCount,
                                                                                                            KDvLevel))
                                    logging.info("Saving the Hot Outputs before moving to the next theme")

                                    # Step 6: there are sufficient HotZones for this theme. saving the reference layers.
                                    # The HotZones are already saved, but we can add counts to the attribute table.

                                    # Just need To save the Events that constitute the HotZones, and build HotSpots and HotBuffers
                                    # So we can add the clusterID onto the Events Layere and count the events into the HotSpot Layer

                                    # Step 6a. Save the pertinent events out to a dedicated layer for this Theme/KDvLevel.
                                    OutEventsx = "HotEventsC_{}".format(KDvLevel)
                                    logging.info(
                                        "Intersecting (Identity) Events from: {0} within Peak Regions: {1} To: {2}".format(
                                            SelectedEvents, OutZones, OutEventsx))
                                    arcpy.analysis.Identity(in_features=SelectedEvents,
                                                            identity_features=OutZones,
                                                            out_feature_class=OutEventsx,
                                                            join_attributes="ALL",
                                                            cluster_tolerance=None,
                                                            relationship="NO_RELATIONSHIPS")

                                    SelectClusterEvents = "CLUSTER_ID <= 0"
                                    SelectedClusterEvents = arcpy.management.SelectLayerByAttribute(in_layer_or_view=OutEventsx,
                                                                                                    selection_type="NEW_SELECTION",
                                                                                                    where_clause=SelectClusterEvents,
                                                                                                    invert_where_clause=None)

                                    arcpy.DeleteFeatures_management(SelectedClusterEvents)
                                    OutEvents = "{}_{}".format(OutputKDv.replace("KDv_", "Ridge_HotEvents_"), KDvLevel)
                                    arcpy.CopyFeatures_management(SelectedClusterEvents, OutEvents)
                                    logging.info("Relevant Events from: {0} are saved to: {1}".format(PointSet, OutEventsx))

                                    # Step 6c. Count the Collected Cluster Events into the HotZones Layer:
                                    OutZonesx = "{}_{}".format(OutputKDv.replace("KDv_", "Ridge_HotZones_"), KDvLevel)
                                    arcpy.analysis.SummarizeWithin(in_polygons=OutZones,
                                                                   in_sum_features=OutEventsx,
                                                                   out_feature_class=OutZonesx,
                                                                   keep_all_polygons="KEEP_ALL",
                                                                   sum_fields=None,
                                                                   sum_shape="ADD_SHAPE_SUM",
                                                                   shape_unit='',
                                                                   group_field="CLUSTER_ID",
                                                                   add_min_maj="NO_MIN_MAJ",
                                                                   add_group_percent="NO_PERCENT",
                                                                   out_group_table=None)

                                    # Step 6d. Generate some HotSpot Centroids.
                                    HotSpots = "{}_{}".format(OutputKDv.replace("KDv_", "Ridge_HotSpots_"), KDvLevel)
                                    logging.info(
                                        "Finding Centroids of HotZones: {}".format(OutZonesx))
                                    HotSpotLayer = arcpy.management.FeatureToPoint(in_features=OutZonesx,
                                                                                   out_feature_class=HotSpots,
                                                                                   point_location="CENTROID")

                                    HotBuffers = "{}_{}".format(OutputKDv.replace("KDv_", "Ridge_HotBuffers_"),
                                                                KDvLevel)
                                    logging.info("Building {} Buffers of {} HotSpots: {}".format(BufferSize, featCount,
                                                                                                 HotBuffers))
                                    arcpy.analysis.Buffer(in_features=HotSpots,
                                                          out_feature_class=HotBuffers,
                                                          buffer_distance_or_field=BufferSize,
                                                          line_side="FULL",
                                                          line_end_type="ROUND",
                                                          dissolve_option="NONE",
                                                          dissolve_field=None,
                                                          method="PLANAR")

                                    if arcpy.Exists(ClusterLayer):
                                        logging.debug("Deleting: {}".format(ClusterLayer))
                                        arcpy.Delete_management(ClusterLayer)

                                    if arcpy.Exists(DissolvedKDv):
                                        logging.debug("Deleting: {}".format(DissolvedKDv))
                                        arcpy.Delete_management(DissolvedKDv)

                                    if arcpy.Exists(OutZones):
                                        logging.debug("Deleting: {}".format(OutZones))
                                        arcpy.Delete_management(OutZones)

                                    if arcpy.Exists(OutEventsx):
                                        logging.debug("Deleting: {}".format(OutEventsx))
                                        arcpy.Delete_management(OutEventsx)

                                    break

                                else:
                                   logging.info("There are Insufficient HotZones: {} at Level {}, Lets try the next level. ".format(featCount, KDvLevel))
                                   # Step 6a: not enough Hot Zones ( Peak Regions are too resticted) so clear everything and try the next KDvLevel
                                   if arcpy.Exists(DissolvedKDv):
                                       logging.debug("Deleting: {}".format(DissolvedKDv))
                                       arcpy.Delete_management(DissolvedKDv)
                                   if arcpy.Exists(OutEventsx):
                                       logging.debug("Deleting: {}".format(OutEventsx))
                                       arcpy.Delete_management(OutEventsx)
                                   if arcpy.Exists(ClusterLayer):
                                       logging.debug("Deleting: {}".format(ClusterLayer))
                                       arcpy.Delete_management(ClusterLayer)
                                   if arcpy.Exists(OutZones):
                                       logging.debug("Deleting: {}".format(OutZones))
                                       arcpy.Delete_management(OutZones)
                                   if arcpy.Exists(PeakRegions):
                                       logging.debug("Deleting: {}".format(PeakRegions))
                                       arcpy.Delete_management(PeakRegions)

                            arcpy.ClearEnvironment("extent")
                            # Next Theme Loop

    finally:

        logging.info("{0}** Finished - All Themes: - HotSpots, HotEvents, HotBuffers, HotZones and the KDr's & KDv's can be found here:{0}{1}".format("\n", DiffRastersPath))

        arcpy.CheckInExtension("Spatial")
        arcpy.CheckInExtension("ImageAnalyst")
        # txtFile.write(arcpy.GetMessages())
        # txtFile.close()

