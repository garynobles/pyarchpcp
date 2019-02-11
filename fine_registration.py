# https://github.com/jeffdelmerico/pointcloud_tutorial/blob/master/registration/open3d_icp.py

from open3d import *
import numpy as np
import copy
import psycopg2

schema = 'pc_processing'
# sample_top = 'pc_201406151141_dist_top'
# sample_base = 'pc_201407031017_dist_base'

sample_top = 'pc_201406151141_top'
sample_base = 'pc_201407031017_base'
conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
cur = conn.cursor()

print("connected4")
cur.execute("""SELECT (x - 580000),(y - 4275000),(z) FROM {}.{};""".format(schema, sample_top))
# cur.execute("""SELECT (x - 580000),(y - 4275000),(z) FROM {}.{} WHERE dist < 0.035;""".format(schema, sample_top))
T = cur.fetchall()

print("connected5")

# cur.execute("""SELECT (x - 580000),(y - 4275000),(z) FROM {}.{} WHERE dist < 0.035;""".format(schema, sample_base))
cur.execute("""SELECT (x - 580000),(y - 4275000),(z) FROM {}.{};""".format(schema, sample_base))
B = cur.fetchall()

cur.close()
conn.close()

# Pass xyz to Open3D.PointCloud and visualize
top = PointCloud()
top.points = Vector3dVector(T)

base = PointCloud()
base.points = Vector3dVector(B)
# write_point_cloud("../../TestData/sync.ply", pcd)
source1 = top
target1 = base

# Open3D: www.open3d.org

# The MIT License (MIT)

# See license file or visit www.open3d.org for details



from open3d import *

import numpy as np

import copy



def draw_registration_result(source, target, transformation):
    source_temp = copy.deepcopy(source)
    target_temp = copy.deepcopy(target)
    source_temp.paint_uniform_color([1, 0.706, 0])
    target_temp.paint_uniform_color([0, 0.651, 0.929])
    source_temp.transform(transformation)
    draw_geometries([source_temp, target_temp])

if __name__ == "__main__":
    source = source1
    target = target1
    threshold = 0.02
    trans_init = np.asarray(
                [[0.862, 0.011, -0.507,  0.5],
                [-0.139, 0.967, -0.215,  0.7],
                [0.487, 0.255,  0.835, -1.4],
                [0.0, 0.0, 0.0, 1.0]])
    draw_registration_result(source, target, trans_init)
    print("Initial alignment")
    evaluation = evaluate_registration(source, target,
            threshold, trans_init)
    print(evaluation)

    print("Apply point-to-point ICP")
    reg_p2p = registration_icp(source, target, threshold, trans_init,
            TransformationEstimationPointToPoint())
    print(reg_p2p)
    print("Transformation is:")
    print(reg_p2p.transformation)
    print("")
    draw_registration_result(source, target, reg_p2p.transformation)

    print("Apply point-to-plane ICP")
    reg_p2l = registration_icp(source, target, threshold, trans_init,
            TransformationEstimationPointToPlane())
    print(reg_p2l)
    print("Transformation is:")
    print(reg_p2l.transformation)
    print("")
    draw_registration_result(source, target, reg_p2l.transformation)