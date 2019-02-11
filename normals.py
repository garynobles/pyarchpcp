import numpy as np

import psycopg2
from psycopg2 import sql


from open3d import *

top_point_cloud = 'pc_201407060711_dist_top'
base_point_cloud = 'pc_201407061221_dist_base'
schema = 'pc_processing'

#
# top_point_cloud = 'pc_201407061221'
# base_point_cloud = 'pc_201407071201'


conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
cur = conn.cursor()
cur.execute("""SELECT (x - 580000),(y - 4275000),(z) FROM pc_processing.{} WHERE dist >0.035;""".format(top_point_cloud))  # 0.035
top = cur.fetchall()
cur.execute("""SELECT (x - 580000),(y - 4275000),(z) FROM pc_processing.{} WHERE dist >0.035;""".format( base_point_cloud))  # 0.035
base = cur.fetchall()
cur.close()
conn.close()

print("Load a point cloud, print it, and render it")
top_pc = PointCloud()
top_pc.points = Vector3dVector(top)
print(top_pc)
print(np.asarray(top_pc.points))
draw_geometries([top_pc])
print("Compute the normals of the downsampled point cloud")
downpcd = voxel_down_sample(top_pc, voxel_size=0.01)
estimate_normals(downpcd, search_param=KDTreeSearchParamHybrid(
    radius=3, max_nn=30))

# estimate_normals(downpcd, search_param=KDTreeSearchParamHybrid(
#     radius=0.03, max_nn=30))
draw_geometries([downpcd])
print("")