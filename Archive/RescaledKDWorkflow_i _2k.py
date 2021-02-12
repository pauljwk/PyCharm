import os
import arcpy
from arcpy.sa import *

# path to the Suicide Residences Points are found
# point_fc = r'S:\CoreData\NCIS\LITS_20200729\NCIS.gdb\Residence_xy'

# path to the Suicide Incidents Points are found
point_fc = r'S:\CoreData\NCIS\LITS_20200729\NCIS.gdb\Incidents_xy'

# the .GDB needs to exist BUT the feature class(es) as per name_base + Theme_filter[0] cannot!
out_gdb = r'F:\Suicide_Vs_Population\LITS_20200729\Suicide_I_2008-2017_2k_KDs.gdb'
# NB if you want to run parallel ensure that the .gdb's are different!

#Remember to change the radius reference in the mane base Rescale_XX_KD_xxx
name_base = 'Rescale_2k_KD_Suicide_Inc_08_17'
PointFilter = "ExclusionCode NOT IN (1) AND Incident_Year IN (2017, 2016, 2015, 2014, 2013, 2012, 2011, 2010, 2009, 2008)"

# Remember to update the Theme Filter keys: if including a gender filter as per Total_P -> Total_M

# The theme names and filters for the various analyses to be performed on the input points.
theme_filters = {'Total_P': PointFilter,
                 'Youth_P': PointFilter + " AND Age < 25",
                 'Adult_P': PointFilter + " AND Age > 24 AND Age < 45",
                 'Mature_P': PointFilter + " AND Age > 44 AND Age < 65",
                 'MidAge_P': PointFilter + " AND Age > 24 AND Age < 65",
                 'Elders_P': PointFilter + " AND Age > 64"}

cell_size = 50
search_radius = 2000

if not arcpy.Exists(out_gdb):
    gdb_path, gdb_name = os.path.split(out_gdb)
    arcpy.CreateFileGDB_management(gdb_path, gdb_name)

with arcpy.EnvManager(scratchWorkspace=out_gdb, workspace=out_gdb):
    # arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(3857)
    KDExtent = r'S:\CoreData\Sites\SitesExtents.gdb\Aust_Extent_Main_pk'
    # Parrallel processing doesn' ALWAYS help!
    # parallel_processing = '12'

    arcpy.CheckOutExtension("spatial")
    arcpy.CheckOutExtension("ImageAnalyst")
    # arcpy.env.parallelProcessingFactor = parallel_processing
    try:
        for key, where_clause in theme_filters.items():
            out_name = '{}_{}'.format(name_base, key)
            con_path = os.path.join(out_gdb, out_name + "CON")
            out_path = os.path.join(out_gdb, out_name)
            if arcpy.Exists(out_path):
                raise RuntimeError('Result already exists: ' + out_path)

            filtered_points = arcpy.MakeFeatureLayer_management(in_features=point_fc, out_layer='lyr',
                                                                where_clause=where_clause)

            row_count = arcpy.GetCount_management(filtered_points)[0]
            print("Filtered: " + out_name + " - " + row_count)

            arcpy.env.extent = KDExtent
            kd_result = KernelDensity(in_features=filtered_points,
                                      population_field="NONE",
                                      cell_size=cell_size,
                                      search_radius=search_radius,
                                      area_unit_scale_factor='SQUARE_KILOMETERS',
                                      out_cell_values='DENSITIES',
                                      method='PLANAR',
                                      in_barriers=None)

            print("Finished KD - " + out_name )
            con_result = Con(kd_result, kd_result, None, "VALUE > 0")
            con_result.save(con_path)

            print("Finished Con - " + out_name )
            max_val = arcpy.GetRasterProperties_management(con_result, "MAXIMUM")
			

            with arcpy.EnvManager(compression="NONE", pyramid="NONE"):
                out_raster = arcpy.sa.RescaleByFunction(in_raster=con_path, transformation_function=[
                    ["LINEAR", 0, "", max_val, "", 0, max_val, ""]], from_scale=0, to_scale=20)

                
                out_raster.save(out_path)

            # arcpy.Delete_management(filtered_points)
            arcpy.Delete_management(kd_result)
            arcpy.Delete_management(con_result)
            arcpy.Delete_management(con_path)
            print(out_name)

    finally:
        arcpy.CheckInExtension("Spatial")
