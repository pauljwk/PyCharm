import os
import arcpy
from arcpy.sa import *
arcpy.ImportToolbox(r'G:\Working\ArcMap Stuff\pkRasterHelper.tbx')

point_fc = r'G:\BaseMaps\GNAF-AllGeoms-Population-ATS-2020\GNAF-AllGeoms-Population-ATS-2020.gdb\GNAF_2016_Residential_plus_ATSeXtra_SA1AgeSex_PopWeights'
out_gdb = r'G:\Working\GNAF_Population_AgeSex\BuildRasters\BuildRasters.gdb'
out_name = "{}\PopRes".format(out_gdb)
LogFile = r"G:\Working\GNAF_Population_AgeSex\BuildRasters\PopBuild.txt"

txtFile = open(LogFile, "w")

#Python List uses square brackets
genders = ['P', 'M', 'F']
#Python Dictionary uses curly brackets and key: values (including nested lists and dictionaries)
scales = {'1k': 1000, '2k': 2000}
cellsizes = [50, 100, 200]
ages = ['Adult', 'Elder', 'Mature', 'MidAge', 'Total', 'Youth']

with arcpy.EnvManager(scratchWorkspace=out_gdb, workspace=out_gdb):
    if not arcpy.Exists(out_gdb):
        gdb_path, gdb_name = os.path.split(out_gdb)
        arcpy.CreateFileGDB_management(gdb_path, gdb_name)

    KDExtent = r'S:\CoreData\Sites\SitesExtents.gdb\Aust_Extent_Main_pk'
    # arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(3857)

    # Parrallel processing doesn't ALWAYS help!
    # parallel_processing = '12'

    arcpy.CheckOutExtension("spatial")
    arcpy.CheckOutExtension("ImageAnalyst")
    # arcpy.env.parallelProcessingFactor = parallel_processing
    try:
        for gender in genders:
            for scale, searchradius in scales.items():
                for cellsize in cellsizes:
                    for age in ages:
                        OutPutPath =
                        OutputKDr = "KDr_{}_{}_{}_{}_{}".format(out_name, age, gender, cellsize, scale )
                        OutputCon = "KDr_Con_{}_{}_{}_{}_{}".format(out_name, age, gender, cellsize, scale )
                        OutputKDv = "KDv_{}_{}_{}_{}_{}".format(out_name, age, gender, cellsize, scale )

                        arcpy.AddMessage("Starting - {}".format(OutputKDr))
                        txtFile.write("Starting - {}{}".format(OutputKDr, "\n"))

                        row_count = arcpy.GetCount_management(point_fc)[0]
                        arcpy.AddMessage("{} Point Count = {}".format(point_fc, row_count))
                        txtFile.write("{} Point Count = {}{}".format(point_fc, row_count, "\n"))

                        arcpy.env.extent = KDExtent
                        kd_result = KernelDensity(in_features=point_fc,
                                                  population_field="NONE",
                                                  cell_size=cellsize,
                                                  search_radius=searchradius,
                                                  area_unit_scale_factor='SQUARE_KILOMETERS',
                                                  out_cell_values='DENSITIES',
                                                  method='PLANAR',
                                                  in_barriers=None)

                        arcpy.AddMessage("Finished Raw KD - {}".format(OutputKDr))
                        txtFile.write("Finished Raw KD - {}{}".format(OutputKDr, "\n"))

                        con_result = Con(kd_result, kd_result, None, "VALUE > 0")
                        con_result.save(OutputCon)

                        max_val = arcpy.GetRasterProperties_management(con_result, "MAXIMUM")
                        arcpy.AddMessage("Finished Con - {} - Max Value: {}".format(OutputCon, max_val))
                        txtFile.write("Finished Con - {} - Max Value: {}{}".format(OutputCon, max_val , "\n"))

                        with arcpy.EnvManager(compression="NONE", pyramid="NONE"):
                            out_raster = arcpy.sa.RescaleByFunction(in_raster=con_result,
                                                                    transformation_function=[["LINEAR", 0, "", max_val, "", 0, max_val, ""]],
                                                                    from_scale=0,
                                                                    to_scale=20)

                            out_raster.save(OutputKDr)

                            arcpy.AddMessage("Finished ReScale - {}".format(OutputKDr))
                            txtFile.write("Finished ReScale - {}{}".format(OutputKDr, "\n"))

                            arcpy.AddMessage("Converting to Integer: {}".format(OutputKDr))
                            txtFile.write("Converting to Integer: {}{}".format(OutputKDr, "\n"))

                            int_result = Int(OutputKDr)
                            # int_result.save(int_raster)

                            arcpy.AddMessage("Building KDv: {}".format(OutputKDv))
                            txtFile.write("Building KDv: {}{}".format(OutputKDv, "\n"))

                            kdv_result = arcpy.RasterToPolygon_conversion(in_raster = int_result,
                                                                          out_polygon_features = OutputKDv,
                                                                          simplify = "NO_SIMPLIFY",
                                                                          raster_field = "Value",
                                                                          create_multipart_features = "SINGLE_OUTER_PART",
                                                                          max_vertices_per_feature = None)
                            # kdv_result.save(KDVector)

                            # arcpy.Delete_management(kd_result)
                            # arcpy.Delete_management(OutputCon)
                            # arcpy.Delete_management(con_result)

    finally:

        arcpy.CheckInExtension("Spatial")
        txtFile.write(arcpy.GetMessages())
        txtFile.close()

