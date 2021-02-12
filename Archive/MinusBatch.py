import arcpy
import os

#Python List uses square brackets
# ages = ['Adult', 'Elders', 'Mature', 'MidAge', 'Total', 'Youth']  
ages = ['Total']  
             
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
					#min_raster = r'L:\Work\Papers\NationalHotSpots\SuicideVsPopulation\Suicide_Minuses.gdb\Minus_{}_{}_{}_{}'.format(v, age, gender, scale)
					con_raster = r'L:\Work\Papers\NationalHotSpots\SuicideVsPopulation\Suicide_Minuses.gdb\PK-Con_Minus_{}_{}_{}_{}'.format(v, age, gender, scale)
					
					print('Building Min: ' + con_raster)
					min_result = arcpy.ia.Minus(source_raster, pop_raster)
					#min_result.save(min_raster)

					if arcpy.Exists(con_raster):
						print(con_raster + ' - Already Exists')
					else:
						print('Building Con: ' + con_raster)
						con_result = arcpy.ia.Con(min_result, min_result, None, "VALUE > 0")
						con_result.save(con_raster)
						print('Built: ' + con_raster)
				
				else:
					print(source_raster + ' does not exist')
		   
