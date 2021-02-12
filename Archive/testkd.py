import arcpy
import os
from arcpy.sa import *
arcpy.ImportToolbox(r'G:\Working\ArcMap Stuff\pkRasterHelper.tbx')

InPoints = r'L:\CoreData\NCIS\LITS_20200729\NCIS.gdb\Incidents_xy'
PopulationField = ""
KDExtent = r'L:\CoreData\Sites\SitesExtents.gdb\ACT_Extent'

CellSize = 200
SeachRadius = 2000
PopRaster = ""
KDvLevel = 0
HotSpotBuffer = 0

HoldKDRaster = True
KDRaster = r'L:\Work\Papers\NationalHotSpots\SuicideVsPopulation\Suicide_KDs.gdb\Incidents_xy_200_2k_ACTxmhguru'
MinusRaster = ''

OutPoints = ''
OutBuffers = ''
OutZones = ''

out_gdb = ''
LogFile = r'L:\Work\Papers\NationalHotSpots\SuicideVsPopulation\Incidents_xy_KDMinusPop47.txt'


txtFile = open(LogFile,"w")

txtFile.write ("KDMinusPop Parameters {}".format("\n"))
txtFile.write ("InPoints = {}{}".format(InPoints,"\n"))
txtFile.write ("PopulationField = {}{}".format(PopulationField,"\n"))
txtFile.write ("KDExtent = {}{}".format(KDExtent,"\n"))
txtFile.write ("CellSize = {}{}".format(CellSize,"\n"))
txtFile.write ("SeachRadius = {}{}".format(SeachRadius,"\n"))
txtFile.write ("PopRaster = {}{}".format(PopRaster,"\n"))
txtFile.write ("KDvLevel = {}{}".format(KDvLevel,"\n"))
txtFile.write ("HotSpotBuffer = {}{}".format(HotSpotBuffer,"\n"))
txtFile.write ("HoldKDRaster = {}{}".format(HoldKDRaster,"\n"))
txtFile.write ("KDRaster = {}{}".format(KDRaster,"\n"))
txtFile.write ("MinusRaster = {}{}".format(MinusRaster,"\n"))
txtFile.write ("OutPoints = {}{}".format(OutPoints,"\n"))
txtFile.write ("OutBuffers = {}{}".format(OutBuffers,"\n"))
txtFile.write ("OutZones = {}{}".format(OutZones,"\n"))
txtFile.write ("LogFile = {}{}".format(LogFile,"\n"))
txtFile.write ("out_gdb = {}{}".format(out_gdb,"\n"))

with arcpy.EnvManager(scratchWorkspace=out_gdb, workspace=out_gdb):
    #arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(3857)
    # Parrallel processing doesn' ALWAYS help!
    # parallel_processing = '12'

    arcpy.CheckOutExtension("spatial")
    arcpy.CheckOutExtension("ImageAnalyst")

    try:
        row_count = arcpy.GetCount_management(InPoints)[0]
        txtFile.write ("Analyses Points: {} - {}{}".format(InPoints, row_count, "\n"))

        if len(KDRaster) < 1:
            KDRaster = '{}_{}'.format(InPoints, "tmp")

        if len(PopulationField) < 1:
            PopulationField = 'NONE'

        if len(KDExtent) < 1:
            KDExtent = r'L:\CoreData\Sites\SitesExtents.gdb\Aust_Extent_Main_pk'

        txtFile.write("KernelDensity Params: InPoints: {} - TargetRaster: {} - PopField: {}{}".format(InPoints, KDRaster, PopulationField, "\n"))

        arcpy.env.extent = KDExtent
        kd_result = KernelDensity(in_features=InPoints,
                                  population_field=PopulationField,
                                  cell_size=CellSize,
                                  search_radius=SeachRadius,
                                  area_unit_scale_factor='SQUARE_KILOMETERS',
                                  out_cell_values='DENSITIES',
                                  method='PLANAR',
                                  in_barriers=None)

        txtFile.write("Finished KD - {}{}".format(KDRaster, "\n"))

        con_result = Con(kd_result, kd_result, None, "VALUE > 0")
        #con_result.save(KDRaster)

        max_val = arcpy.GetRasterProperties_management(con_result, "MAXIMUM")
        txtFile.write("Finished Con - {} - Max Value: {}{}".format(KDRaster, max_val, "\n"))

        with arcpy.EnvManager(compression="NONE", pyramid="NONE"):
            out_raster = arcpy.sa.RescaleByFunction(in_raster=con_result, transformation_function=[
                ["LINEAR", 0, "", max_val, "", 0, max_val, ""]], from_scale=0, to_scale=20)

            out_raster.save(KDRaster)
            txtFile.write ("Finished ReScale - {}{}".format(KDRaster, "\n"))

    finally:
        txtFile.write(arcpy.GetMessages())
        txtFile.close()

