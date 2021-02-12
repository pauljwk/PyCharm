import os
import sys
import arcpy, time
from arcpy.sa import *
import logging

# topic_point_fc = arcpy.GetParameterAsText(0)        # Topic Points Input FC
# ThemeFilter = arcpy.GetParameterAsText(1)      	  # SQL statement based on fields in topic_point_fc as per: "ExclusionCode NOT IN (1) AND Incident_Year IN (2018, 2017, 2016, 2015, 2014, 2013, 2012, 2011, 2010, 2009) AND Incident_Residence_Match = 'No' "

# pop_point_fc = arcpy.GetParameterAsText(2)          # Population Points Input FC pop_point_fc NB in this case it is preferable to have a Dedicated FC or at least an active DQ
# PopFilter = arcpy.GetParameterAsText(3)      		  # Population Filter SQL statement based on fields in
# PopWeight = arcpy.GetParameterAsText(4)			  # A field in pop_point_fc as per: Tot_P_GNAF_Weight or Adult_P_GNAF_Weight

# BufferSize = arcpy.GetParameterAsText(5)			  # '2 Kilometers'
# CellSize = arcpy.GetParameterAsText(6)
# MinHotSpotCount = arcpy.GetParameter(7)			  # 50

# out_path = arcpy.GetParameterAsText(8)			  # Input text referencing an existing directory.
# out_gdb = arcpy.GetParameterAsText(9)				  # Input Text of the order: "Incidents_08_19" will be ceated if not exist
# out_name = arcpy.GetParameterAsText(10)			  # Input Text of the order "Tot_P_"
# LogFile = arcpy.GetParameterAsText(11)

# HoldWorking = arcpy.GetParameterAsText(12)		# Rasters can be big. If you are confident with the parameter selection False. True while reviewing choices.

topic_point_fc = ""
ThemeFilter = ""

pop_point_fc = ""
PopFilter = ""
PopWeight = ""

BufferSize = '2 Kilometers'
CellSize = 200
MinHotSpotCount = 50

out_path = ""
out_gdb = ""
out_name = ""
LogFile = ""

HoldWorking = True

out_gdb = "{}\{}".format(out_path,out_gdb)
if not arcpy.Exists(out_gdb):
	gdb_path, gdb_name = os.path.split(out_gdb)
	arcpy.CreateFileGDB_management(gdb_path, gdb_name)

RasterPop = "{}\KDr_PopRes_{}".format(out_gdb, out_name)
RasterTopic = "{}\KDr_{}".format(out_gdb, out_name)
TopicKDv = "{}\KDv_{}".format(out_gdb, out_name)
PointSet = "{}\{}".format(out_gdb, out_name)
OutputKDr = "{}\KDr_PopDiff_{}".format(out_gdb, out_name)
OutputKDv = "{}\KDv_PopDiff_{}".format(out_gdb, out_name)

# The Raster math conversion to vectors will deliver a vecorized raster in the gridcode range 1 - 20
# 20 represents the Most DIFFERENT locations where suicide event density exceeds population density.
# this script dissolves each of the gridcode vector polygon sets and identifies the events that contribute to the topic raster
# and ultimately to the Difference. The identification of Suicide Events is based on a buffer distance (BufferD) around the centroids of the dissolved vectorized Difference raster.
# The hotspot count will increase with the broadening of the dissolve (gridcode - 20 - 1) and at some point the HotSpots satrt to lose relevance.
# This analyses is looking for the most relevant hotspots where events exceed underlying populations.
# The minimum hotspot count (MinHotSpotCount) for these analyses outputs should be set around 20 - 50 any more and we will be identifying increasingly marginal differences)

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

arcpy.env.overwriteOutput = True

with arcpy.EnvManager(scratchWorkspace=out_gdb, workspace=out_gdb):
	# arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(3857)
	# Parallel processing doesn't ALWAYS help!
	# parallel_processing = '12'

	arcpy.CheckOutExtension("spatial")
	arcpy.CheckOutExtension("ImageAnalyst")

	try:
		txtMessage = "{0}** Starting Custom Differencing - "\
					 " - PointSet: {1}{0}" \
					 " - RasterTopic: {2}{0}" \
					 " - RasterPop: {3}{0}" \
					 " - Destination GDB: {4}{0}".format("\n", PointSet, RasterTopic, RasterPop, out_gdb)
		logging.info(txtMessage)

		# step 1
		# Build Custom Population Raster
		if arcpy.Exists(RasterPop):
			logging.info("{} Using Existing Population Raster: {}".format("\n", RasterPop))
		else:
			logging.info("{}** BUILDING Population Raster: {}".format("\n", RasterPop))

		# step 2
		# Build Custom Topic Raster (inc Custom KDv and Custom Point SubSet)
		if arcpy.Exists(RasterTopic) and arcpy.Exists(PointSet):
			logging.info("{} Using Existing Topic Raster: {} and PointSet: {}".format("\n", RasterTopic, PointSet))
		else:
			logging.info("{}** Building Existing Topic Raster: {} and PointSet: {}".format("\n", RasterTopic, PointSet))

		# step 3
		# Compare the two rasters

		logging.info("Commence KD Minus Analysis")

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
			logging.info("Building Raster Math Rasters - Con: {}".format(ConRaster))

			con_result = arcpy.sa.Con(diff_result, diff_result, None, "VALUE > 0")
			con_result.save(ConRaster)

			# Rescale
			max_val = arcpy.GetRasterProperties_management(ConRaster, "MAXIMUM")
			with arcpy.EnvManager(compression="NONE", pyramid="NONE"):
				DiffRasterRaw = "{}/DiffRasterRaw".format(out_gdb)
				logging.info("Building Raster Math Rasters - Rescale: {} - Max={}".format(DiffRasterRaw, max_val))
				out_raster = arcpy.sa.RescaleByFunction(in_raster=ConRaster,
														transformation_function=[["LINEAR", 0, "", max_val, "", 0, max_val, ""]],
														from_scale=0,
														to_scale=20)

				out_raster.save(DiffRasterRaw)

				logging.info("Building Raster Math Rasters - Slice: {}".format(OutputKDr))

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

		logging.info("Building PopDiff HotSpots using: {}".format(OutputKDv))

		for KDvLevel in (15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1):

			DissolvedKDV = "{}_Dissolved_{}".format(OutputKDv, KDvLevel)
			SelectExpression = "gridcode >= {}".format(KDvLevel)

			logging.info("Selecting records where: {}".format(SelectExpression))

			SelectedKDv = arcpy.management.SelectLayerByAttribute(in_layer_or_view=OutputKDv,
																  selection_type="NEW_SELECTION",
																  where_clause=SelectExpression,
																  invert_where_clause=None)

			logging.info("Dissolving Selected Polygons From: {} where: {} To: {}".format(SelectedKDv, SelectExpression, DissolvedKDV))
			arcpy.gapro.DissolveBoundaries(SelectedKDv, DissolvedKDV, "SINGLE_PART")

			HotPoints = "{}_{}".format(OutputKDv.replace("KDv_", "HotSpots_"), KDvLevel))
			logging.info("Finding Centroids of Dissolved PopDiff-KDv: {}".format(SelectedKDv))
			HotSpotLayer = arcpy.management.FeatureToPoint(in_features=DissolvedKDV, out_feature_class=HotPoints, point_location="CENTROID")

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
				logging.info("{} Dissolved Polygons is enough to Evaluate HotSpots. KDvLevel = {}".format(featCount, KDvLevel))
				HotBuffers = OutputKDv.replace("KDv_", "AllHotBuffers_")
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
				logging.info("Selecting Events from {} that are coincident with {} - HotBuffers: {}".format(PointSet, "\n", HotBuffers))
				SelectedEvents = arcpy.management.SelectLayerByLocation(in_layer=PointSet,
																		overlap_type="WITHIN_A_DISTANCE",
																		select_features=HotPoints,
																		search_distance=BufferSize,
																		selection_type="NEW_SELECTION",
																		invert_spatial_relationship="NOT_INVERT")
				# filtered_points_sel = "SubSet_{}".format(filtered_points)
				arcpy.CopyFeatures_management(SelectedEvents, OutEvents)

				logging.info("Intersecting (Identity) Events from {0}{3} with HotBuffer's UIDs: {1}{3} To: {2}".format(PointSet, HotBuffers, OutEvents, "\n"))
				arcpy.analysis.Identity(in_features=SelectedEvents,
										identity_features=HotBuffers,
										out_feature_class=OutEvents,
										join_attributes="ALL",
										cluster_tolerance=None,
										relationship="NO_RELATIONSHIPS")

				OutCount = OutEvents.replace("HotEvents_", "EventsCount_")
				logging.info("Counting Events from {0}{3} with HotBuffers UID's: {1}{3} To: {2}".format(HotPoints, HotBuffers, OutEvents,"\n"))
				arcpy.analysis.Statistics(in_table=OutEvents,
										  out_table=OutCount,
										  statistics_fields=[["OBJECTID", "COUNT"]],
										  case_field="ORIG_FID")

				# OutPoints = "{}\HotSpots_PopDiff_{}".format(out_gdb, KDvLevel)

				logging.info("Finalizing HotPoints: {}{} with Frequencies from {}".format(HotPoints, "\n", OutCount))
				arcpy.management.JoinField(in_data=HotPoints,
										   in_field="ORIG_FID",
										   join_table=OutCount,
										   join_field="ORIG_FID",
										   fields="FREQUENCY")

				# Removing 'under-whelming' HotSpots - Frequency < 5
				SelectHotspots = "frequency < 5"
				logging.info("Selecting HotSpots from {} that represent fewer than 5 Events".format(HotPoints))
				SelectedSpots = arcpy.management.SelectLayerByAttribute(in_layer_or_view=HotPoints,
																		selection_type="NEW_SELECTION",
																		where_clause=SelectHotspots,
																		invert_where_clause=None)

				DeletionCount = int(arcpy.GetCount_management(SelectedSpots)[0])
				if DeletionCount > 0:
					logging.info("Deleting {} underwhelming HotSpots".format(DeletionCount))
					arcpy.DeleteRows_management(SelectedSpots)

				HotSpotCount = int(arcpy.GetCount_management(HotPoints)[0])

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
						logging.info("Deleting: {}".format(HotPoints))
						arcpy.Delete_management(HotPoints)

					if arcpy.Exists(OutEvents):
						logging.info("Deleting: {}".format(OutEvents))
						arcpy.Delete_management(OutEvents)

					# Remove temporary build layers
					if arcpy.Exists(OutCount):
						logging.info("Deleting: {}".format(OutCount))
						arcpy.Delete_management(OutCount)

					if arcpy.Exists(DissolvedKDV):
						logging.info("Deleting: {}".format(DissolvedKDV))
						arcpy.Delete_management(DissolvedKDV)

					if arcpy.Exists(HotBuffers):
						logging.info("Deleting: {}".format(HotBuffers))
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
					logging.info("Creating {} HotZones: representing: {} Events.".format(HotPoints, OutEvents))
					arcpy.management.MinimumBoundingGeometry(OutEvents, OutZones, "ENVELOPE", "LIST",
															 "ORIG_FID", "NO_MBG_FIELDS")

					arcpy.management.JoinField(in_data=OutEvents,
											   in_field="ORIG_FID",
											   join_table=OutCount,
											   join_field="ORIG_FID",
											   fields="FREQUENCY")

					logging.info("Selecting Events from {} that are duplicates from {} intersections".format(OutEvents, SelectHotspots))
					SelectedEvents = arcpy.management.SelectLayerByAttribute(in_layer_or_view=OutEvents,
																			 selection_type="NEW_SELECTION",
																			 where_clause=SelectHotspots,
																			 invert_where_clause=None)

					DeletionCount = int(arcpy.GetCount_management(SelectedEvents)[0])
					if DeletionCount > 0:
						logging.info("Deleting {} Events related to underwhelming HotSpots.".format(DeletionCount))
						arcpy.DeleteRows_management(SelectedEvents)

					HotEventsCount = int(arcpy.GetCount_management(OutEvents)[0])

					# If the HotSpot count was sufficient then the principal layers: HotSpots and HotEvents are already saved and the ancillary layers (HotZones, HotBuffers, are saved too.
					# little point in cycling through another KDLevel so get out.
					logging.info("Finished {7} - Exiting this KdLevel Loop Results:{8}"
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

		logging.info("{0}** Finished - All Themes: - HotSpots, HotEvents, HotBuffers, HotZones and the KDr's & KDv's can be found here:{0}{1}".format("\n", out_gdb))

		arcpy.CheckInExtension("Spatial")
		arcpy.CheckInExtension("ImageAnalyst")
