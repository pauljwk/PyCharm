import arcpy
import os
from arcpy.sa import *
arcpy.ImportToolbox(r'G:\Working\ArcMap Stuff\pkRasterHelper.tbx')

#Python List uses square brackets
ages = ['Adult', 'Elders', 'Mature', 'MidAge', 'Total', 'Youth']  
             
#Python Dictionary uses curly brackets and key: values (including nested lists and dictionaries)
suicideloc = {'I': 'Inc', 'R': 'Res'}

# Python List 
scales = ['1k', '2k']

# Python List
genders = ['P', 'M', 'F']

for gender in genders:
	for scale in scales:
		for age in ages:
			for k, v in suicideloc.items():
				source_raster = r'F:\Suicide_Vs_Population\LITS_20200729\Suicide_{0}_2008-2017_{1}_KDs.gdb\Rescale_{1}_KD_Suicide_{2}_08_17_{3}_{4}'.format(k, scale, v, age, gender)
				if arcpy.Exists(source_raster):
					pop_raster = r'F:\BaseMaps\GNAF-AllGeoms-Population-ATS-2020\Population_KD_Resources.gdb\Rescale_Con_KD_ComboPopRes_{}_{}_50_{}'.format(age, gender, scale)
					min_raster = r'L:\Work\Papers\NationalHotSpots\SuicideVsPopulation\Suicide_Minuses.gdb\xMinus_{}_{}_{}_{}'.format(v, age, gender, scale)
					con_raster = r'L:\Work\Papers\NationalHotSpots\SuicideVsPopulation\Suicide_Minuses.gdb\xCon_Minus_{}_{}_{}_{}'.format(v, age, gender, scale)
					rescale_raster = r'L:\Work\Papers\NationalHotSpots\SuicideVsPopulation\Suicide_Minuses.gdb\xRescaleLin_Con_Minus_{}_{}_{}_{}'.format(v, age, gender, scale)
					int_raster = r'L:\Work\Papers\NationalHotSpots\SuicideVsPopulation\Suicide_Minuses.gdb\xInt_RescaleLin_Con_Minus_{}_{}_{}_{}'.format(v, age, gender, scale)
					out_vector = r'L:\Work\Papers\NationalHotSpots\SuicideVsPopulation\Suicide_Minuses.gdb\xKDv_Int_RescaleLin_Con_Minus_{}_{}_{}_{}'.format(v, age, gender, scale)
					hotspots_folder = r'L:\Work\Papers\NationalHotSpots\SuicideVsPopulation'

					if arcpy.Exists(min_raster):
						print(min_raster + ' - Already Exists')
					else:
						print('Building Min: ' + min_raster)
						min_result = arcpy.ia.Minus(source_raster, pop_raster)
						min_result.save(min_raster)

					if arcpy.Exists(con_raster):
						print(con_raster + ' - Already Exists')
					else:
						print('Building Con: ' + con_raster)
						con_result = arcpy.ia.Con(min_raster, min_raster, None, "VALUE > 0")
						con_result.save(con_raster)
						#arcpy.Delete_management(min_raster)

					if arcpy.Exists(rescale_raster):
						print(rescale_raster + ' - Already Exists')
					else:
						print('Building Rescale: ' + rescale_raster)

						max_val = arcpy.GetRasterProperties_management(con_raster, "MAXIMUM")
						print(con_raster + " - Max Value: " + str(max_val))
						##rescale_result = RescaleByFunction(con_raster, "LINEAR # 5 # # # #", 0, 20)
						rescale_result = arcpy.sa.RescaleByFunction(in_raster=con_raster, transformation_function=[
							["LINEAR", 0, "", max_val, "", 0, max_val, ""]], from_scale=0, to_scale=20)

						rescale_result.save(rescale_raster)
						#print('Built: ' + rescale_raster)
						#arcpy.Delete_management(con_raster)

					if arcpy.Exists(int_raster):
						print(int_raster + ' - Already Exists')
					else:
						print('Building Int Raster: ' + int_raster)
						int_result = Int(rescale_raster)
						int_result.save(int_raster)
						# print('Built: ' + int_raster)
						# arcpy.Delete_management(rescale_raster)

					if arcpy.Exists(out_vector):
						print(out_vector + ' - Already Exists')
					else:
						print('Building Vector Output: ' + out_vector)
						kdv_result = arcpy.conversion.RasterToPolygon(int_raster, out_vector, "SIMPLIFY", "Value", "SINGLE_OUTER_PART", None)
						##kdv_result.save(out_vector)
						# print('Built: ' + out_vector)
						# arcpy.Delete_management(int_raster)

					for x in range(15):
						hotspot_level = x + 1
						hotspot_expression = "gridcode >= {}".format(hotspot_level)
						vector_gdb,vector_name = os.path.split(out_vector)
						kdv_level_name = "{}_{}plus".format(vector_name, hotspot_level)
						out_name = "Suicide_Minuses_Hotspots_{}.gdb".format(hotspot_level)
						out_path = os.path.join(hotspots_folder,out_name)

						if not arcpy.Exists(out_path):
							arcpy.CreateFileGDB_management(hotspots_folder, out_name)

						kdv_level=os.path.join(out_path,kdv_level_name)

						if arcpy.Exists(kdv_level):
							print(kdv_level + ' - Already Exists')
						else:
							print('Building Vector Hospot Dissolves: ' + kdv_level)
							kdvl_result = arcpy.DissolveSelection_pkRasterHelper(out_vector, hotspot_expression, kdv_level)
							##kdv_result.save(kdvl_result)
							print('Built: ' + kdv_level)

				else:
					print(source_raster + ' does not exist')

