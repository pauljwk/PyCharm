import os
import arcpy, time
from arcpy.sa import *
# arcpy.ImportToolbox(r'G:\Working\ArcMap Stuff\pkRasterHelper.tbx')

point_fc = r'L:\CoreData\NCIS\LITS_20200729\NCIS.gdb\Incidents_xy'
# point_fc = r'L:\CoreData\NCIS\LITS_20200729\NCIS.gdb\Residence_xy'
out_gdb = r'F:\BuildSuicideRasters\LITS20200729\Incidents_06_17.gdb'
# out_gdb = r'F:\BuildSuicideRasters\LITS20200729\Residences_06_17.gdb'
name_base = "Incidents_Away"
# name_base = "Residences"
LogFile = r"F:\BuildSuicideRasters\LITS20200729\BatchSuicideBuild_Inc_06_17_2.txt"
# LogFile = r"F:\BuildSuicideRasters\LITS20200729\BatchSuicideBuild_Res_06_17.txt"

txtFile = open(LogFile, "w")

## Python List uses square brackets
# genders = ['P', 'M', 'F']
genders = ['P']
## Python Dictionary uses curly brackets and key: values (including nested lists and dictionaries)
scales = {'1k': 1000, '2k': 2000}
# cellsizes = [50, 100, 200]
cellsizes = [200, 100, 50]
# ages = ['Adult', 'Elder', 'Mature', 'MidAge', 'Tot', 'Youth']
ages = {'Adult': 'Age > 24 AND Age < 45',
        'Elder': 'Age > 64',
        'Mature': 'Age > 44 AND Age < 65',
        'MidAge': 'Age > 24 AND Age < 65',
        'Youth': 'Age < 25',
        'Tot': 'Age > 0'}

PointFilter = "ExclusionCode NOT IN (1) AND " \
              "Incident_Year IN (2017, 2016, 2015, 2014, 2013, 2012, 2011, 2010, 2009, 2008, 2007, 2006) AND " \
              "Incident_Residence_Match = 'No'"


arcpy.env.overwriteOutput = True

if not arcpy.Exists(out_gdb):
    gdb_path, gdb_name = os.path.split(out_gdb)
    arcpy.CreateFileGDB_management(gdb_path, gdb_name)

with arcpy.EnvManager(scratchWorkspace=out_gdb, workspace=out_gdb):

    # arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(3857)
    # Parrallel processing doesn't ALWAYS help!
    # parallel_processing = '8'
    # arcpy.env.parallelProcessingFactor = parallel_processing

    arcpy.CheckOutExtension("spatial")
    arcpy.CheckOutExtension("ImageAnalyst")

    try:
        for gender in genders:
            for scale, searchradius in scales.items():
                for cellsize in cellsizes:
                    for age, ageRange in ages.items():
                        OutputKDr = "{}\KDr_{}_{}_{}_{}_{}".format(out_gdb, name_base, age, gender, cellsize, scale)
                        KDrAlias = "KDr_{}_{}_{}_{}_{}".format(name_base, age, gender, cellsize, scale)
                        OutputCon = "{}\KDr_Con_{}_{}_{}_{}_{}".format(out_gdb, name_base, age, gender, cellsize, scale)
                        OutputKDv = "{}\KDv_{}_{}_{}_{}_{}".format(out_gdb, name_base, age, gender, cellsize, scale)
                        KDvAlias = "KDv_{}_{}_{}_{}_{}".format(name_base, age, gender, cellsize, scale)
                        # PopWeight = "{}_{}_GNAF_Weight".format(age, gender)
                        con_result = ""
                        kd_result = ""
                        where_clause = "{} AND {}".format(PointFilter, ageRange)
                        OutputEvents = "{}\{}_{}_{}".format(out_gdb, name_base, age, gender)
                        PopWeight = 'NONE'

                        arcpy.env.extent = 'MAXOF'

                        arcpy.AddMessage("{7}Starting - Gender: {0} - Scale: {1} - CellSize: {2} - Age: {3}{7}"
                                         " - KD Raster: {4}{7}"
                                         " - KD Vector: {5}{7}"
                                         " - Population Weight: {6}{7}"
                                         " - Start: {8}{7}".format(gender,
                                                                   scale,
                                                                   cellsize,
                                                                   age,
                                                                   OutputKDr,
                                                                   OutputKDv,
                                                                   PopWeight,
                                                                   "\n",
                                                                   time.strftime("%X")))
                        txtFile.write("{7}Starting - Gender: {0} - Scale: {1} - CellSize: {2} - Age: {3}{7}"
                                      " - KD Raster: {4}{7}"
                                      " - KD Vector: {5}{7}"
                                      " - Population Weight: {6}{7}"
                                      " - Start: {8}{7}".format(gender,
                                                                scale,
                                                                cellsize,
                                                                age,
                                                                OutputKDr,
                                                                OutputKDv,
                                                                PopWeight,
                                                                "\n",
                                                                time.strftime("%X")))

                        if not arcpy.Exists(OutputEvents):
                            arcpy.AddMessage("Saving Points FC - {} - {}".format(OutputEvents,
                                                                                 time.strftime("%X")))
                            txtFile.write("Saving Points FC - {} - {}{}".format(OutputEvents,
                                                                                time.strftime("%X"),
                                                                                "\n"))

                            filtered_points = arcpy.MakeFeatureLayer_management(in_features=point_fc,
                                                                                out_layer='lyr',
                                                                                where_clause=where_clause)
                            arcpy.CopyFeatures_management(filtered_points,
                                                          OutputEvents)
                        else:
                            arcpy.AddMessage("Using Existing Points FC: {} - {}".format(OutputEvents,
                                                                                        time.strftime("%X")))
                            txtFile.write("Using Existing Points FC: {} - {}{}".format(OutputEvents,
                                                                                       time.strftime("%X"),
                                                                                       "\n"))
                            filtered_points = OutputEvents

                        KDextent_tight = "{}_ExtentTight".format(OutputEvents)
                        KDextent = "{}_Extent".format(OutputEvents)

                        if not arcpy.Exists(KDextent):
                            arcpy.AddMessage("Creating Extent: {} - {}".format(OutputEvents,
                                                                                    time.strftime("%X")))
                            txtFile.write("Creating Extent: {0} - {1}{2}".format(OutputEvents,
                                                                                 time.strftime("%X"),
                                                                                 "\n"))

                            # row_count = arcpy.GetCount_management(OutputEvents)[0]

                            arcpy.management.MinimumBoundingGeometry(OutputEvents, KDextent_tight, "ENVELOPE", "ALL")

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

                        row_count = arcpy.GetCount_management(OutputEvents)[0]
                        ## For Testing use a small extent
                        # KDextent = r'L:\CoreData\Sites\SitesExtents.gdb\ACT_Extent'
                        arcpy.env.extent = KDextent

                        if not arcpy.Exists(OutputKDr):
                            arcpy.AddMessage("Building KD: {0} using {1} points. - {2}{3}".format(OutputKDr,
                                                                                                  row_count,
                                                                                                  time.strftime("%X"),
                                                                                                  "\n"))
                            txtFile.write("Building KD: {0} using {1} points. - {2}{3}".format(OutputKDr,
                                                                                               row_count,
                                                                                               time.strftime("%X"),
                                                                                               "\n"))

                            # arcpy.AddMessage("Starting Raw KD - {} - {}".format(OutputKDr,
                            #                                                     time.strftime("%X")))
                            # txtFile.write("Starting Raw KD - {} - {}{}".format(OutputKDr,
                            #                                                    time.strftime("%X"),
                            #                                                    "\n"))

                            kd_result = arcpy.sa.KernelDensity(in_features=OutputEvents,
                            								   population_field=PopWeight,
		                                                       cell_size=cellsize,
           		                                               search_radius=searchradius,
                  		                                       area_unit_scale_factor='SQUARE_KILOMETERS',
                           			                           out_cell_values='DENSITIES',
                                    		                   method='PLANAR',
                                             		           in_barriers=None)

                            arcpy.AddMessage("Finished Raw KD: {} - {}".format(OutputKDr,
                                                                               time.strftime("%X")))
                            txtFile.write("Finished Raw KD: {} - {}{}".format(OutputKDr,
                                                                              time.strftime("%X"),
                                                                              "\n"))

                            con_result = Con(kd_result, kd_result, None, "VALUE > 0")
                            # con_result.save(OutputCon)

                            max_val = arcpy.GetRasterProperties_management(con_result, "MAXIMUM")

                            arcpy.AddMessage("Finished Con KD - {} - Max Value: {} - {}".format(OutputCon,
                                                                                                max_val,
                                                                                                time.strftime("%X")))
                            txtFile.write("Finished Con - KD {} - Max Value: {} - {}{}".format(OutputCon,
                                                                                               max_val,
                                                                                               time.strftime("%X"),
                                                                                               "\n"))

                            with arcpy.EnvManager(compression="NONE", pyramid="NONE"):
                                out_raster = arcpy.sa.RescaleByFunction(in_raster=con_result,
                                                                        transformation_function=[["LINEAR", 0, "", max_val, "", 0, max_val, ""]],
                                                                        from_scale=0,
                                                                        to_scale=20)

                                # arcpy.Rename_management(out_raster, OutputKDr)
                                out_raster.save(OutputKDr)

                            arcpy.AddMessage("Finished ReScaling KD Saving As: {} - {}".format(OutputKDr,
                                                                                               time.strftime("%X")))
                            txtFile.write("Finished ReScaling KD Saving As: {} - {}{}".format(OutputKDr,
                                                                                              time.strftime("%X"),
                                                                                              "\n"))

                            if arcpy.Exists(OutputCon):
                                txtFile.write("Deleting: {} - {}{}".format(OutputCon,
                                                                           time.strftime("%X"),
                                                                           "\n"))
                                arcpy.Delete_management(OutputCon)
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

                        else:
                            arcpy.AddMessage("Using Existing Raster: {} - {}".format(OutputKDr,
                                                                                        time.strftime("%X")))
                            txtFile.write("Using Existing Raster: {} - {}{}".format(OutputKDr,
                                                                                       time.strftime("%X"),
                                                                                       "\n"))

                        if arcpy.Exists(OutputKDr):
                            # arcpy.AlterAliasName(OutputKDr, KDrAlias)
                            arcpy.Rename_management(OutputKDr, KDrAlias)

                        if not arcpy.Exists(OutputKDv):
                            arcpy.AddMessage("Slicing to Integer KD: {} - {}".format(OutputKDr,
                                                                                        time.strftime("%X")))
                            txtFile.write("Slicing to Integer KD: {} - {}{}".format(OutputKDr,
                                                                                       time.strftime("%X"),
                                                                                       "\n"))

                            # int_result = "{}_Tmp".format(OutputKDr)
                            # arcpy.Int_3d(OutputKDr, int_result)
                            int_result = arcpy.sa.Slice(OutputKDr, 21, "NATURAL_BREAKS", 0);
                            # int_result.save("{}_Tmp".format(OutputKDr))
                            # int_result.save(OutputKDr)

                            arcpy.AddMessage("Building KDv: {} - {}".format(OutputKDv,
                                                                            time.strftime("%X")))
                            txtFile.write("Building KDv: {} - {}{}".format(OutputKDv,
                                                                           time.strftime("%X"),
                                                                           "\n"))

                            kdv_result = arcpy.RasterToPolygon_conversion(in_raster = int_result,
                                                                          out_polygon_features = OutputKDv,
                                                                          simplify = "SIMPLIFY",
                                                                          raster_field = "Value",
                                                                          create_multipart_features = "SINGLE_OUTER_PART",
                                                                          max_vertices_per_feature = None)

                            arcpy.Delete_management(int_result)

                        # kdv_result.save(KDVector)

                        ResidualCon = "{}{}KernelD_GNAF2".format(out_gdb, "\\")
                        if arcpy.Exists(ResidualCon):
                            txtFile.write("Deleting: {}{}".format(ResidualCon, "\n"))
                            arcpy.Delete_management(ResidualCon)

                        ResidualCon = "{}{}Con_KernelD_1".format(out_gdb, "\\")
                        if arcpy.Exists(ResidualCon):
                            txtFile.write("Deleting: {}{}".format(ResidualCon, "\n"))
                            arcpy.Delete_management(ResidualCon)

                        if arcpy.Exists(OutputKDv):
                            arcpy.AlterAliasName(OutputKDv, KDvAlias)

                        arcpy.AddMessage(
                            "Finished: {2} - KD Raster = {0}{2} - KD Vector = {1}{2} Time - {3}{2}".format(OutputKDr,
                                                                                                           OutputKDv,
                                                                                                           "\n",
                                                                                                           time.strftime("%X")))
                        txtFile.write(
                            "Finished: {2} - KD Raster = {0}{2} - KD Vector = {1}{2} Time - {3}{2}".format(OutputKDr,
                                                                                                           OutputKDv,
                                                                                                           "\n",
                                                                                                           time.strftime("%X")))
    finally:

        arcpy.CheckInExtension("Spatial")
        arcpy.CheckInExtension("ImageAnalyst")
        txtFile.write(arcpy.GetMessages())
        txtFile.close()
