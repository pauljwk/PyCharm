import pandas as pd
from arcgis.features import GeoAccessor
from arcgis.gis import GIS

# ref: https://developers.arcgis.com/python/guide/introduction-to-the-spatially-enabled-dataframe/
# ref: https://developers.arcgis.com/python/api-reference/arcgis.features.toc.html?highlight=featurelayer#geoaccessor

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

gis = GIS(profile="agol_graphc")

l1_path = r'C:\tmp\MyProject5.gdb\SA2Centroids'
l2_path = r'C:\tmp\MyProject5.gdb\GCP2016_G01'

df1 = pd.DataFrame.spatial.from_featureclass(l1_path).astype({'SA2_MAIN16': str}) # import layer
print('------------------ df1 -----------------------------')
print(df1)

df2 = GeoAccessor\
    .from_table(l2_path, fields=['SA2_MAINCODE_2016', 'Tot_P_M'])\
    .astype({'SA2_MAINCODE_2016': str, 'Tot_P_M': int})  # import table

print('------------------ df2 -----------------------------')
print(df2)

join_df = pd.merge(left=df1, right=df2, how='left', left_on='SA2_MAIN16', right_on='SA2_MAINCODE_2016')\
    .drop('SA2_MAINCODE_2016', axis=1)

print('------------------ join -----------------------------')
print(join_df)

# join_layer = join_df.spatial.to_featurelayer(title='layername')

join_fc = join_df.spatial.to_featureclass(location=r'C:\tmp\MyProject5.gdb\TestResult')
