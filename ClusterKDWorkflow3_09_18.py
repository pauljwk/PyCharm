import os
import arcpy
import datetime
from arcpy.sa import *

# -----------------------------PARAMETERS-------------------------------------

# If Use Existing == True, then existing components will be used for the calculation if they
# already exist.  This can save time by preventing un-necessary re-calculation in case of
# a code crash.  However, the user is responsible for manually deleting components that may
# be partial or incorrect results.  To do a full recalculation, delete any existing target
# databases before execution, or if they contain results from other processes, delete any
# existing output data-sets before execution.

recalculate_all = False  # if true, the working and result databases will be deleted and all values recalculated from scratch.
use_existing = True  # If true, existing result files will be used instead of recalculated.  recalculate_all=True will override this.
keep_working = False  # if False the working gdb will be deleted at the end of the script execution.

# path to the gdb in which the LITS extracts are to be stored
# lits_gdb = r'L:\Work\KernelDensity\LITS_20200729\ClusterAnalyses\LITS_20200729_08_17.gdb'
lits_gdb = r'L:\Work\KernelDensity\LITS_20210112\ClusterAnalyses-2009-18_NSW\LITS.gdb'

# path to the gdb in which the cluster analysis results are to be stored.
# cluster_analysis_gdb = r'L:\Work\KernelDensity\LITS_20200729\ClusterAnalyses\ClusterAnalyses_08_17.gdb'
cluster_analysis_gdb = r'L:\Work\KernelDensity\LITS_20210112\ClusterAnalyses-2009-18_NSW\ClusterAnalyses.gdb'

# path to the gdb in which the kd working layers are to be stored - note: this gdb may be deleted at the end of the run if
# preserve working is set to false.
# kd_working_gdb = r'L:\Work\KernelDensity\LITS_20200729\ClusterAnalyses\KD_working.gdb'
kd_working_gdb = r'L:\Work\KernelDensity\LITS_20210112\ClusterAnalyses-2009-18_NSW\KD_working.gdb'

# path to the gdb in which the kd results are to be stored
# kd_analysis_gdb = r'L:\Work\KernelDensity\LITS_20200729\ClusterAnalyses\KD_Analyses_08_17.gdb'
kd_analysis_gdb = r'L:\Work\KernelDensity\LITS_20210112\ClusterAnalyses-2009-18_NSW\KD_Analyses.gdb'

# The master incident points sets to be used.  It is assumed these follow standart LIFESPAN structure.
# source_locations = {'Incidents': r"S:\CoreData\NCIS\LITS_20200729\NCIS.gdb\Incidents_xy",
#                     'Residence': r"S:\CoreData\NCIS\LITS_20200729\NCIS.gdb\Residence_xy"}

source_locations = {'Incidents': r"S:\CoreData\NCIS\LITS_20210112\NCIS.gdb\Incidents_xy",
                    'Residence': r"S:\CoreData\NCIS\LITS_20210112\NCIS.gdb\Residence_xy"}

# The master filter that will be applied to source locations to select only the desired timeframes and non-exclusion items.
# Using the IN filter option allows for non-sequential years to be included if desired.
PeriodFilter = "ExclusionCode NOT IN (1) AND Incident_Year IN (2018, 2017, 2016, 2015, 2014, 2013, 2012, 2011, 2010, 2009)"

# The theme names and filters for the various analyses to be performed on the input points.
# theme_filters = {'ALLAges_ALLIncidents': PeriodFilter + " AND Age > 5"}
# theme_filters = {'ALLAges_ALLIncidents': PeriodFilter}
theme_filters = {'ALLAges_ALLIncidents': PeriodFilter,
                 'ALLAges_AwayIncidents': PeriodFilter + " AND Incident_Residence_Match = 'No'",
                 'ALLAges_HomeIncidents': PeriodFilter + " AND Incident_Residence_Match = 'Yes'",
                 'Elders_ALLIncidents': PeriodFilter + " AND Age > 64",
                 'Elders_AwayIncidents': PeriodFilter + " AND Age > 64 AND Incident_Residence_Match ='No'",
                 'Elders_HomeIncidents': PeriodFilter + " AND Age > 64 AND Incident_Residence_Match ='Yes'",
                 'Youth_ALLIncidents': PeriodFilter + " AND Age < 25",
                 'Youth_AwayIncidents': PeriodFilter + " AND Age < 25 AND Incident_Residence_Match ='No'",
                 'Youth_HomeIncidents': PeriodFilter + " AND Age < 25 AND Incident_Residence_Match ='Yes'"}

# The cluster method to be used
cluster_method = 'DBSCAN'

# minimum cluster size for clustering operation
minimum_cluster_size = 5

kd_resolutions = {'1K': {'cell_size': 50,
                         'search_radius': 1000},
                  '2K': {'cell_size': 75,
                         'search_radius': 2000},
                  '4K': {'cell_size': 100,
                         'search_radius': 4000}}

# privacy threshold for Kernel Density operation
privacy_threshold = 5

slices = 10

# Environmental Settings
# force the scratch to be local or at a known location.  This prevents the use of the anu user profile space, which can be slow.
# If running multiple scripts in parallel, you may want to set the parameter to an empty string '' as this will allow the system to
# decide on how many CPUs to use (usually 1)
scratch_workspace = r'L:\Work\KernelDensity\LITS_20210112\ClusterAnalyses-2009-18_NSW\scratch.gdb'

# Set the number of processes to be used:
# - blank (empty)—Let each tool determine how many processes to use. This is the default.
# - 0—Do not spread operations across multiple processes.
# - n—Use the specified number of processes.
# - n%—Calculate the number of processes using the specified percentage: Number of processes = number of system cores * n / 100.
# https://pro.arcgis.com/en/pro-app/tool-reference/environment-settings/parallel-processing-factor.htm
# If running multiple script instances at once, you may want to use the blank ('') option.
parallel_processing = ''

# see note: https://pro.arcgis.com/en/pro-app/tool-reference/environment-settings/output-extent.htm
# Australia: "12476759.2654 -5589605.4073 17630228.5082 -1036490.5379"

# KDExtent = r'S:\CoreData\Sites\SitesExtents.gdb\Aust_Extent_Main_pk'
KDExtent = r'S:\CoreData\Sites\SitesExtents.gdb\NSW_Extent_tight'

# -----------------------------CODE-------------------------------------------


def execute_workflow():
    # set some environmental settings for better performance
    prepare_database(scratch_workspace)
    arcpy.env.scratchWorkspace = scratch_workspace
    arcpy.env.parallelProcessingFactor = parallel_processing

    working_helper = WorkingHelper()
    working_helper.prepare_gdb()

    # prepare the LITS extracts
    lits_helper = LITSExtractsHelper()
    lits_helper.prepare_gdb()
    lits_extracts = lits_helper.create_extracts()

    cluster_helper = ClustersHelper()
    cluster_helper.prepare_gdb()
    cluster_results = cluster_helper.create_clusters(lits_extracts)

    arcpy.env.extent = KDExtent
    kd_helper = KDHelper()
    kd_helper.prepare_gdb()
    # try to get SA license
    if arcpy.CheckExtension("Spatial") == "Available":
        arcpy.CheckOutExtension("Spatial")
    else:
        # Raise a custom exception
        raise LicenseError

    try:
        kd_results = kd_helper.create_kd_polygons(cluster_results)
    finally:
        arcpy.CheckInExtension("Spatial")

    slice_processor = SliceProcessor()
    slice_processor.process_slices(kd_results)

    print('{}: Done'.format(datetime.datetime.now()))


class LicenseError(Exception):
    pass


class LITSExtractsHelper(object):
    """
    A lits helper class intended for use within this script only.  Relies on parameters set out above.
    """
    @staticmethod
    def prepare_gdb():
        prepare_database(lits_gdb)

    @staticmethod
    def create_extracts():
        """
        Creates all point extracts for sources and themes defines in the sources
        :return: {source_id: {theme_id: {'lits_extract': lits_fc_path}}}
        :rtype:
        """
        result = {}
        print('{}: Creating LITS Extracts'.format(datetime.datetime.now()))
        for source_id, source_points in source_locations.items():

            source_result = child_dict(result, source_id)

            for theme_id, theme_filter in theme_filters.items():
                lits_fc_name = '{}_{}'.format(source_id, theme_id)
                lits_fc_path = os.path.join(lits_gdb, lits_fc_name)

                if arcpy.Exists(lits_fc_path):
                    if use_existing:
                        print('{}: Using existing LITS points: {}'.format(datetime.datetime.now(), lits_fc_path))
                    else:
                        raise RuntimeError('{}: use_existing is False and dataset exists: {}'.format(datetime.datetime.now(), lits_fc_path))
                else:
                    print('{}: Extracting LITS points: {}'.format(datetime.datetime.now(), lits_fc_path))
                    arcpy.FeatureClassToFeatureClass_conversion(in_features=source_points,
                                                                out_path=lits_gdb,
                                                                out_name=lits_fc_name,
                                                                where_clause=theme_filter)

                theme_result = child_dict(source_result, theme_id)
                theme_result['lits_extract'] = lits_fc_path

        return result

    @staticmethod
    def lits_path(source_id, theme_id):
        lits_fc_name = '{}_{}'.format(source_id, theme_id)
        return os.path.join(lits_gdb, lits_fc_name)


class ClustersHelper(object):
    @staticmethod
    def prepare_gdb():
        prepare_database(cluster_analysis_gdb)

    @staticmethod
    def create_clusters(params):
        """
        :param params: The output from the LITS Extracts process.
        :type params: {source_id: {theme_id: {'lits_extract': lits_fc_path}}}
        :return: The params with the create_clusters results appended
        :rtype: {source_id: {theme_id: {'lits_extract': lits_fc_path, kd_id: {'clusters', cluster_result_path}}}}
        """
        for source_id, source_result in params.items():
            for theme_id, theme_result in source_result.items():
                filtered_points = theme_result['lits_extract']

                for kd_id, kd_params in kd_resolutions.items():
                    search_distance = kd_params['search_radius']
                    resolution_result = child_dict(theme_result, kd_id)

                    fc_name = '{}_{}_PC_{}_{}_{}'.format(source_id, theme_id, cluster_method, minimum_cluster_size, kd_id)
                    out_path = os.path.join(cluster_analysis_gdb, fc_name)
                    if arcpy.Exists(out_path):
                        if use_existing:
                            print('{}: Using Cluster Analysis Result: {}'.format(datetime.datetime.now(), out_path))
                        else:
                            raise RuntimeError('{}: use_existing is False and dataset exists: {}'.format(datetime.datetime.now(), out_path))
                    else:
                        print('{}: Performing Cluster Analysis: {}'.format(datetime.datetime.now(), fc_name))
                        search_dist = '{} Meters'.format(search_distance)
                        arcpy.gapro.FindPointClusters(input_points=filtered_points,
                                                      out_feature_class=out_path,
                                                      clustering_method=cluster_method,
                                                      minimum_points=minimum_cluster_size,
                                                      search_distance=search_dist,
                                                      use_time="NO_TIME",
                                                      search_duration=None)

                    resolution_result['clusters'] = out_path

        return params


class KDHelper(object):
    def __init__(self):
        self.cluster_lyr = None
        self.kd_raster = None
        self.con_raster = None
        self.slice_raster = None
        self.slice_features = None

    @staticmethod
    def prepare_gdb():
        prepare_database(kd_analysis_gdb)

    def create_kd_polygons(self, params):
        for source_id, source_result in params.items():
            for theme_id, theme_result in source_result.items():
                for kd_id, kd_params in kd_resolutions.items():

                    # check to see whether the vectorised KDV is already built
                    kdv_output = os.path.join(kd_analysis_gdb,'{}_{}_{}'.format(source_id, theme_id, kd_id))
                    if arcpy.Exists(kdv_output):
                        print('{}: Using Existing Vectorised KDv: {}'.format(datetime.datetime.now(),kdv_output))
                    else:

                        search_radius = kd_params['search_radius']
                        slice_features_name = '{}_{}_{}sliced'.format(source_id, theme_id, kd_id)
                        slice_features_path = os.path.join(kd_working_gdb, slice_features_name)

                        if arcpy.Exists(slice_features_path):
                            if use_existing:
                                print('{}: Using Existing Slice Features: {}'.format(datetime.datetime.now(), slice_features_path))
                                theme_result[kd_id]['slices'] = slice_features_path
                            else:
                                raise RuntimeError('{}: use_existing is False and dataset exists: {}'.format(datetime.datetime.now(),
                                                                                                             slice_features_path))
                        else:
                            clustered_points = theme_result[kd_id]['clusters']

                            kd_raster_name = '{}_{}_{}'.format(source_id, theme_id, kd_id)
                            cell_size = kd_params['cell_size']
                            self._do_kd(clustered_points, kd_raster_name, search_radius, cell_size)

                            con_raster_name = '{}_{}_{}con'.format(source_id, theme_id, kd_id)
                            self._do_con(con_raster_name)

                            slice_raster_name = '{}_{}_{}slice'.format(source_id, theme_id, kd_id)
                            self._do_slice(slice_raster_name)

                            print('Vectorising Raster: {}'.format(slice_features_name))
                            if self.slice_raster is not None:
                                self.slice_features = arcpy.RasterToPolygon_conversion(in_raster=self.slice_raster,
                                                                                        out_polygon_features=slice_features_path,
                                                                                        simplify="SIMPLIFY",
                                                                                        raster_field="Value")

                            else:
                                print('Null Raster Skipped: {}'.format(slice_features_name))
                                slice_features_path = None

                            theme_result[kd_id]['slices'] = slice_features_path

        if not keep_working:
            if self.kd_raster is not None:
                arcpy.Delete_management(self.kd_raster)
            if self.con_raster is not None:
                arcpy.Delete_management(self.con_raster)
            if self.slice_raster is not None:
                arcpy.Delete_management(self.slice_raster)

        return params

    def _do_kd(self, clustered_points, kd_result_name, search_radius, cell_size):
        kd_result_path = os.path.join(kd_working_gdb, kd_result_name)
        if arcpy.Exists(kd_result_path):
            if use_existing:
                print('{}: Using Existing Kernel Density Result: {}'.format(datetime.datetime.now(), kd_result_path))
                self.kd_raster = kd_result_path
            else:
                raise RuntimeError('{}: use_existing is False and dataset exists: {}'.format(datetime.datetime.now(), kd_result_path))
        else:
            if self.cluster_lyr is not None:
                arcpy.Delete_management(self.cluster_lyr)
            self.cluster_lyr = arcpy.MakeFeatureLayer_management(in_features=clustered_points,
                                                                 out_layer='tmp_layer',
                                                                 where_clause='CLUSTER_ID > 0')
            row_count = arcpy.GetCount_management(self.cluster_lyr)[0]
            if row_count == 0:
                print('{}: Too Few points.  Skipped {}'.format(datetime.datetime.now(), kd_result_name))
            else:
                print('{}: Performing Kernel Density: {}'.format(datetime.datetime.now(), kd_result_name))
                if self.kd_raster is not None:
                    arcpy.Delete_management(self.kd_raster)
                # If the result already exists, return existing or raise error if return existing is not allowed.
                self.kd_raster = KernelDensity(in_features=self.cluster_lyr,
                                               population_field="NONE",
                                               cell_size=cell_size,
                                               search_radius=search_radius,
                                               area_unit_scale_factor='SQUARE_KILOMETERS',
                                               out_cell_values='DENSITIES',
                                               method='PLANAR')

                if keep_working:
                    self.kd_raster.save(kd_result_path)

    def _do_con(self, con_name):
        con_raster_path = os.path.join(kd_working_gdb, con_name)

        # If the result already exists, return existing or raise error if return existing is not allowed.
        if arcpy.Exists(con_raster_path):
            if use_existing:
                print('{}: Using Con Analysis Result: {}'.format(datetime.datetime.now(), con_raster_path))
                self.con_raster = con_raster_path
            else:
                raise RuntimeError('{}: use_existing is False and dataset exists: {}'.format(datetime.datetime.now(), con_raster_path))
        else:
            if self.kd_raster is None:
                print('{}: KD Empty: Con Skipped'.format(datetime.datetime.now()))
                return

            if self.con_raster is not None:
                arcpy.Delete_management(self.con_raster)

            print('{}: Extracting Non-Zero values: {}'.format(datetime.datetime.now(), con_name))
            self.con_raster = Con(in_conditional_raster=self.kd_raster,
                                  in_true_raster_or_constant=self.kd_raster,
                                  in_false_raster_or_constant="",
                                  where_clause="Value > 0")

            if keep_working:
                self.con_raster.save(con_raster_path)

    def _do_slice(self, slice_name):
        slice_raster_path = os.path.join(kd_working_gdb, slice_name)

        # If the result already exists, return existing or raise error if return existing is not allowed.
        if arcpy.Exists(slice_raster_path):
            if use_existing:
                print('{}: Using Slice Analysis Result: {}'.format(datetime.datetime.now(), slice_raster_path))
                self.slice_raster = slice_raster_path
            else:
                raise RuntimeError('{}: use_existing is False and dataset exists: {}'.format(datetime.datetime.now(), slice_raster_path))
        else:
            if self.con_raster is None:
                print('{}: CON Empty: Slice Skipped'.format(datetime.datetime.now()))
                return

            if self.slice_raster is not None:
                arcpy.Delete_management(self.slice_raster)

        if not (arcpy.sa.Raster(self.con_raster).maximum is None):
            print('{}: Slicing Raster: {}'.format(datetime.datetime.now(), slice_name))
            self.slice_raster = Slice(in_raster=self.con_raster,
                                    number_zones=slices,
                                    slice_type='NATURAL_BREAKS')

            if keep_working:
                self.slice_raster.save(slice_raster_path)
        else:
            if self.slice_raster is not None:
                arcpy.Delete_management(self.slice_raster)
                self.slice_raster = None
            print('{}: No Cells to Slice: Slice Skipped'.format(datetime.datetime.now()))
        return

class SliceProcessor(object):
    def __init__(self):
        self.clustered_points = None
        self.dissolved = None
        self.gridcode_analyzer = PolygonGridCodeAnalyzer2(workspace=kd_working_gdb)

    def process_slices(self, params):
        print('Performing slice privacy updates')
        result = None
        for source_id, source_result in params.items():
            for theme_id, theme_result in source_result.items():
                for kd_id in kd_resolutions.keys():
                    slice_params = theme_result[kd_id]

                    if slice_params['slices'] is None:
                        print('Skipping: {}_{}_{}'.format(source_id, theme_id, kd_id))
                    else:
                        kd_result_name = '{}_{}_{}'.format(source_id, theme_id, kd_id)
                        kd_result_path = get_existing_item(kd_analysis_gdb, kd_result_name)
                        if kd_result_path:
                            print('{}: Existing result found: {}'.format(datetime.datetime.now(), kd_result_path))
                        else:
                            if self.clustered_points is not None:
                                arcpy.Delete_management(self.clustered_points)
                            cluster_source = slice_params['clusters']
                            print('{}: Loading cluster points layer for {}_{}_{}'.format(datetime.datetime.now(), source_id, theme_id, kd_id))
                            self.clustered_points = self._get_test_points(source=cluster_source,
                                                                          out_workspace='in_memory',
                                                                          out_name='pts',
                                                                          where_clause='CLUSTER_ID > 0')
                            self.dissolved = slice_params['slices']

                            for tier in range(slices, 0, -1):
                                result = self._process_slice(source_id=source_id, theme_id=theme_id, kd_id=kd_id, tier=tier)
                                self.dissolved = result

                            kd_result_path = os.path.join(kd_working_gdb, kd_result_name)
                            print('{}: Creating Analysis Result: {}'.format(datetime.datetime.now(), kd_result_path))
                            arcpy.FeatureClassToFeatureClass_conversion(result, kd_analysis_gdb, kd_result_name)

    @staticmethod
    def _get_test_points(source, out_workspace, out_name, where_clause=None):
        out_path = os.path.join(out_workspace, out_name)
        if arcpy.Exists(out_path):
            arcpy.Delete_management(out_path)

        desc = arcpy.Describe(source)
        sr = desc.spatialReference
        shapeType = desc.shapeType

        result = arcpy.CreateFeatureclass_management(out_path=out_workspace, out_name=out_name,
                                                     geometry_type=shapeType, spatial_reference=sr)
        with arcpy.da.SearchCursor(source, ['SHAPE@'], where_clause) as s_cursor:
            with arcpy.da.InsertCursor(result, ['SHAPE@']) as i_cursor:
                for row in s_cursor:
                    new_row = [row[0]]
                    i_cursor.insertRow(new_row)

        return result

    def _process_slice(self, source_id, theme_id, kd_id, tier):

        print('Processing Slice {0}'.format(tier))

        dissolve_name = '{}_{}_{}_{}dissolve'.format(source_id, theme_id, kd_id, tier)
        dissolved = get_existing_item(kd_working_gdb, dissolve_name)
        if dissolved is None:
            tier_file_name = '{}_{}_{}_{}'.format(source_id, theme_id, kd_id, tier)
            tier_features = get_existing_item(kd_working_gdb, tier_file_name)
            if tier_features is None:
                arcpy.FeatureClassToFeatureClass_conversion(self.dissolved, kd_working_gdb, tier_file_name)
                tier_features = os.path.join(kd_working_gdb, tier_file_name)

            self.gridcode_analyzer.process_tier(gridcode_polygons=tier_features, gridcode=tier, incident_points_layer=self.clustered_points)

            wkg_featuures = tier_features + 'predissolve'
            if arcpy.Exists(wkg_featuures):
                arcpy.Delete_management(wkg_featuures)
            arcpy.CopyFeatures_management(tier_features, wkg_featuures)

            dissolve_path = os.path.join(kd_working_gdb, dissolve_name)
            print('{}: Dissolving gridcodes: {}'.format(datetime.datetime.now(), dissolve_path))
            return arcpy.Dissolve_management(tier_features, dissolve_path, 'gridcode', "", 'SINGLE_PART')

class PolygonGridCodeAnalyzer2(object):
    def __init__(self, workspace):
        self.workspace = workspace
        self.outer_rings = OuterRingPolygons(workspace='in_memory')

    def process_tier(self,  gridcode_polygons, gridcode, incident_points_layer):
        print('{}: Generating Comparison Polygons'.format(datetime.datetime.now()))
        outer_rings = self.outer_rings.create_polygons(gridcode_polygons, 'gridcode > {}'.format(gridcode))

        print('{}: Identifying Inner Polygons'.format(datetime.datetime.now()))
        # make a layer from gridcode polygons that only includes features from the gridcode tier to be processed.
        gridcodes_layer = arcpy.MakeFeatureLayer_management(in_features=gridcode_polygons,
                                                            out_layer='gridcode_layer',
                                                            where_clause='gridcode = {}'.format(gridcode))

        # select gridcode layer features that fall within outer ring of features with a higher gridcode
        arcpy.SelectLayerByLocation_management(in_layer=gridcodes_layer,
                                               overlap_type='WITHIN',
                                               select_features=outer_rings,
                                               selection_type='NEW_SELECTION')

        # switch the selection so that only polygons that did not fall within an outer polygon are selected
        # for gridcode adjustment.
        arcpy.SelectLayerByLocation_management(in_layer=gridcodes_layer,
                                               selection_type='SWITCH_SELECTION')

        print('{}: Adjusting Non-Inner polygon gridcodes'.format(datetime.datetime.now()))
        candidate_count = arcpy.GetCount_management(gridcodes_layer)[0]
        print('{} candidates - Analyzing'.format(candidate_count))

        candidate_rings = self.outer_rings.create_polygons_with_orig_oid(source=gridcodes_layer,
                                                                         name='candidate_rings',
                                                                         workspace='in_memory')

        join_path = 'in_memory/joined'
        if arcpy.Exists(join_path):
            arcpy.Delete_management(join_path)

        joined = arcpy.SpatialJoin_analysis(target_features=candidate_rings,
                                            join_features=incident_points_layer,
                                            out_feature_class=join_path,
                                            join_operation='JOIN_ONE_TO_ONE',
                                            join_type='KEEP_ALL',
                                            match_option='INTERSECT')

        shift_ids = []
        fields = ['ORIG_OID']
        where_clause = 'Join_Count < {}'.format(privacy_threshold)
        with arcpy.da.SearchCursor(joined, fields, where_clause) as cursor:
            for row in cursor:
                shift_ids.append(row[0])

        arcpy.Delete_management(joined)

        if shift_ids:
            new_gridcode = gridcode - 1
            fields = ['OID@', 'gridcode']
            with arcpy.da.UpdateCursor(gridcodes_layer, fields) as cursor:
                for row in cursor:
                    if row[0] in shift_ids:
                        row[1] = new_gridcode
                        cursor.updateRow(row)

        print('{} changes made.'.format(len(shift_ids)))

        arcpy.Delete_management(gridcodes_layer)


class PolygonGridCodeAnalyzer(object):
    def __init__(self, workspace):
        self.workspace = workspace
        self.outer_rings = OuterRingPolygons(workspace=workspace)
        self.test_points = None

    def process_tier(self, gridcode_polygons, gridcode, incident_points_layer):

        # generate a feature class and layer of features where the grid code is higer than the tier being processed.
        # this will be used as the baseline to test whether the tier polygons fill holes in higer tier polygons.
        # If the tier polygons do fall within holes of higher tier polygons, then they should no be pushed down.
        print('{}: Generating Comparison Polygons'.format(datetime.datetime.now()))
        self.outer_rings.create_polygons(gridcode_polygons, 'gridcode > {}'.format(gridcode))
        outer_ring_layer = arcpy.MakeFeatureLayer_management(in_features=self.outer_rings.path(),
                                                             out_layer='outer_rings_lyr')

        print('{}: Identifying Inner Polygons'.format(datetime.datetime.now()))
        # make a layer from gridcode polygons that only includes features from the gridcode tier to be processed.
        gridcodes_layer = arcpy.MakeFeatureLayer_management(in_features=gridcode_polygons,
                                                            out_layer='gridcode_layer',
                                                            where_clause='gridcode = {}'.format(gridcode))

        # select gridcode layer features that fall within outer ring of features with a higher gridcode
        arcpy.SelectLayerByLocation_management(in_layer=gridcodes_layer,
                                               overlap_type='WITHIN',
                                               select_features=outer_ring_layer,
                                               selection_type='NEW_SELECTION')
        within_ftrs = gridcode_polygons + 'within'
        if arcpy.Exists(within_ftrs):
            arcpy.Delete_management(within_ftrs)
        arcpy.CopyFeatures_management(gridcodes_layer, within_ftrs)

        # switch the selection so that only polygons that did not fall within an outer polygon are selected
        # for gridcode adjustment.
        arcpy.SelectLayerByLocation_management(in_layer=gridcodes_layer,
                                               selection_type='SWITCH_SELECTION')
        wkg_featuures = gridcode_polygons + 'notwithin'
        if arcpy.Exists(wkg_featuures):
            arcpy.Delete_management(wkg_featuures)
        arcpy.CopyFeatures_management(gridcodes_layer, wkg_featuures)

        print('{}: Adjusting Non-Inner polygon gridcodes'.format(datetime.datetime.now()))
        # now do pushdown tests on remaining items.
        candidate_count = arcpy.GetCount_management(gridcodes_layer)[0]
        print('{} candidates'.format(candidate_count))
        change_count = 0
        new_gridcode = gridcode - 1
        fields = ['SHAPE@', 'gridcode']
        with arcpy.da.UpdateCursor(gridcodes_layer, fields, 'gridcode = {0}'.format(gridcode)) as csr:
            for row in csr:
                outer_ring = self.outer_rings.get_outer_ring(row[0])
                arcpy.SelectLayerByLocation_management(incident_points_layer, 'INTERSECT', outer_ring)
                count_result = arcpy.GetCount_management(incident_points_layer)
                count = int(count_result.getOutput(0))
                if count < privacy_threshold:
                    row[1] = new_gridcode
                    csr.updateRow(row)
                    change_count += 1

        print('{} candidates changed'.format(change_count))
        # delete the layer, but not the feature class.
        arcpy.Delete_management(gridcodes_layer)


class OuterRingPolygons(object):
    def __init__(self, workspace):
        self.workspace = workspace
        self.reference_features_name = 'outer_ring_polygons'

    def path(self):
        return os.path.join(self.workspace, self.reference_features_name)

    def create_polygons(self, source, where_clause=None):
        reference_features = self.path()

        if arcpy.Exists(reference_features):
            arcpy.Delete_management(reference_features)

        desc = arcpy.Describe(source)
        sr = desc.spatialReference
        arcpy.CreateFeatureclass_management(out_path=self.workspace, out_name=self.reference_features_name, geometry_type='POLYGON',
                                            spatial_reference=sr)

        fields = ['SHAPE@']
        with arcpy.da.SearchCursor(source, fields, where_clause) as s_cursor:
            with arcpy.da.InsertCursor(reference_features, ['SHAPE@']) as i_cursor:
                for row in s_cursor:
                    shp = row[0]
                    if shp:
                        ring = self.get_outer_ring(shp)
                        i_cursor.insertRow([ring])

        return reference_features

    def create_polygons_with_orig_oid(self, source, name, workspace='in_memory', where_clause=None):
        out_path = os.path.join(workspace, name)
        if arcpy.Exists(out_path):
            arcpy.Delete_management(out_path)

        desc = arcpy.Describe(source)
        sr = desc.spatialReference
        result = arcpy.CreateFeatureclass_management(workspace, name, geometry_type='Polygon', spatial_reference=sr)
        arcpy.AddField_management(in_table=result, field_name='ORIG_OID', field_type='LONG')
        search_fields = ['SHAPE@', 'OID@']
        out_fields = ['SHAPE@', 'ORIG_OID']
        with arcpy.da.SearchCursor(source, search_fields, where_clause) as s_cursor:
            with arcpy.da.InsertCursor(result, out_fields) as i_cursor:
                for row in s_cursor:
                    shp = self.get_outer_ring(row[0])
                    new_row = [shp, row[1]]
                    i_cursor.insertRow(new_row)
        return result

    @staticmethod
    def get_outer_ring(geom):
        pts = arcpy.Array()
        array = geom.getPart(0)
        for pt in array:
            if not pt:
                break
            else:
                pts.append(pt)
        return arcpy.Polygon(pts, geom.spatialReference)

    def inside_polygon_oids(self, test_features):
        result = []

        if self.check_layer:
            arcpy.Delete_management(self.check_layer)

        self.check_layer = arcpy.MakeFeatureLayer_management(in_features=test_features, out_layer='test_lyr')
        arcpy.SelectLayerByLocation_management(in_layer=self.check_layer, overlap_type='WITHIN', select_features=self.reference_layer)


class WorkingHelper(object):
    @staticmethod
    def prepare_gdb():
        prepare_database(kd_working_gdb)


def prepare_database(gdb_path):
    if arcpy.Exists(gdb_path):
        if recalculate_all:
            arcpy.Delete_management(gdb_path)
        elif use_existing:
            return gdb_path
        else:
            raise RuntimeError('GDB already exists: "{}"'.format(gdb_path))

    base_path, name = os.path.split(gdb_path)
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    return arcpy.CreateFileGDB_management(base_path, name)


def child_dict(parent_dict, key):
    child = parent_dict.get(key, None)
    if child is None:
        child = {}
        parent_dict[key] = child

    return child


def get_existing_item(item_workspace, item_name):
    item_path = os.path.join(item_workspace, item_name)
    if arcpy.Exists(item_path):
        if use_existing:
            print('Using existing slice file: {}'.format(item_path))
            return item_path
        else:
            raise RuntimeError('use_existing is False and dataset exists: {}'.format(item_path))
    return None


execute_workflow()
if not keep_working and arcpy.Exists(kd_working_gdb):
    arcpy.Delete_management(kd_working_gdb)
if arcpy.Exists(scratch_workspace):
    arcpy.Delete_management(scratch_workspace)