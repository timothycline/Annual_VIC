# Nathan Walker
# July 24, 2020
# For each NHD catchment, calculate the proportion that catchment in each grid cell,
# and the proportion of that catchment/gridcell intersection that in within administrative forest boundaries
# where the grid cells come from netCDF files showing total runoff
# This is used to assign weights, to divide runoff between catchments, and calculate runoff from USFS lands

# Before running this code, do the following (sorry, I didn't code these steps)
# 1) Open one of netCDF files for total runoff in ArcMap
# 2) Reclassify this: all values = 1, no data = 0, exporting as raster
# 3) Create a fishnet with the extent and cell size of this raster, such that each square matches a raster cell
# 4) Project this
# 5) Calculate lat/longs of centroids for each of these cells; calculate area for each of these cells; set Id equal to FID
# 6) Download latest version of catchment data from https://nhdplus.com/NHDPlus/NHDPlusV2_data.php
# 7) Download latest forest admin boundaries; dissolve forests together; repair geometry
# (the forest part of the code is optional for the VIC analysis)

import arcpy
import time

### input/output files
grid = "Fishnet/Mask_Fishnet_Proj.shp"
snapras = "Fishnet/Mask.tif"
#grid = "D:/NHD/Weights/Mask_Fishnet_Proj.shp"
#snapras = "D:/NHD/Weights/Fishnet/Mask.tif"
#forests = "D:/NHD/Forests/USFS_Admin_Forests_2020_07_30_Dissolve.shp"

# catchments
catdir = "NHDdata/NHDPlus17Catchments.gdb"
#catdir = "D:/NHD/Catchments/Catchments_2020_07_30.gdb"
arcpy.env.workspace = catdir
cats = arcpy.ListFeatureClasses()

outdir = "CatchmentWeights/Catchment_Weights_17.gdb"
#outdir = "D:/NHD/Weights/Catchment_Weights.gdb"

# Set parameters
arcpy.env.overwriteOutput = True
arcpy.env.extent = "MAXOF"
arcpy.env.snapRaster = snapras
arcpy.env.cellSize = snapras
arcpy.env.outputCoordinateSystem = snapras

# For each catchment, calculate proportion of the catchment in each grid cell,
# and proportion of that catchment/grid cell intersection in administrative forest boundaries
for cat in cats:
#for cat in cats[0:1]:
    # cat = cats[0]

    # start time
    start = time.time()

    # inputs/outputs
    cat = str(cat)
    incat = catdir + "/" + cat
    catgrid = outdir + "/" + cat + "_Grid"
    #catgridfor = outdir + "/" + cat + "_Grid_Forest"
    catcsv = cat.replace("Catchment_", "") + "_wts.csv"

    # Intersect catchments with grid
    arcpy.Intersect_analysis(in_features=[incat,grid],
                             out_feature_class= catgrid,
                             join_attributes="ALL", cluster_tolerance="-1 Unknown", output_type="INPUT")

    # Get size of this catchment/grid cell intersection
    arcpy.AddField_management(in_table= catgrid, field_name="CatGridHa", field_type="FLOAT",
                              field_precision="", field_scale="", field_length="", field_alias="",
                              field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="")
    arcpy.CalculateField_management(in_table=catgrid, field="CatGridHa",
                                    expression="!shape.area@hectares!", expression_type="PYTHON_9.3", code_block="")

    # Get proportion of catchment in this grid cell
    arcpy.AddField_management(in_table=catgrid, field_name="CatPctGrid", field_type="FLOAT",
                              field_precision="", field_scale="", field_length="", field_alias="",
                              field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="")
    arcpy.CalculateField_management(in_table=catgrid, field="CatPctGrid",
                                    expression="!CatGridHa!/100/AreaSqKM", expression_type="PYTHON_9.3", code_block="")


    # replace nulls with zeroes
    codeblock = """def updateValue(value):
        if value == None:
           return '0'
        else: return value"""
    arcpy.CalculateField_management(in_table=catgrid, field="ForPct", expression="updateValue(!ForPct! )",
                                    expression_type="PYTHON_9.3", code_block = codeblock)

    # Export this as csv file
    arcpy.TableToTable_conversion(in_rows=catgrid, out_path="D:/NHD/Weights/Weight_Tables",
                                  out_name=catcsv, where_clause="",
                                  field_mapping='FID "FID" true true false 50 Long 0 0 ,First,#,' + catgrid + ',OBJECTID,-1,-1;' +
                                                'GridID "GridID" true true false 50 Long 0 0 ,First,#,' + catgrid + ',Id,-1,-1;' +
                                                'CatID "CatID" true true false 50 Long 0 0 ,First,#,' + catgrid + ',FEATUREID' + ',-1,-1;' +
                                                'PU_Code "PU_Code" true true false 20 Text 0 0 ,First,#,' + catgrid + ',PU_Code,-1,-1;' +
                                                'Lat "Lat" true true false 4 Float 0 0 ,First,#,' + catgrid + ',Lat,-1,-1;' +
                                                'Long "Long" true true false 4 Float 0 0 ,First,#,' + catgrid + ',Long,-1,-1;' +
                                                'Area_ha "Area_ha" true true false 4 Float 0 0 ,First,#,' + catgrid + ',Area_ha,-1,-1;' +
                                                'CatGridHa "CatGridHa" true true false 4 Float 0 0 ,First,#,' + catgrid + ',CatGridHa,-1,-1;' +
                                                'CatPctGrid "CatPctGrid" true true false 4 Float 0 0 ,First,#,' + catgrid + ',CatPctGrid,-1,-1;' +
                                                'ForPct "ForPct" true true false 4 Float 0 0 ,First,#,' + catgrid + ',ForPct,-1,-1',
                                  config_keyword="")

    # Print time elapsed
    end = time.time()
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    print "Processing completed for " + cat + "; time elapsed: " + "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)
