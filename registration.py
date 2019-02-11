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

def preprocess_point_cloud(pcd, voxel_size):
    print(":: Downsample with a voxel size %.3f." % voxel_size)
    pcd_down = voxel_down_sample(pcd, voxel_size)
    radius_normal = voxel_size * 2
    print(":: Estimate normal with search radius %.3f." % radius_normal)
    estimate_normals(pcd_down, KDTreeSearchParamHybrid(
            radius = radius_normal, max_nn = 30))
    radius_feature = voxel_size * 5
    print(":: Compute FPFH feature with search radius %.3f." % radius_feature)
    pcd_fpfh = compute_fpfh_feature(pcd_down,
            KDTreeSearchParamHybrid(radius = radius_feature, max_nn = 100))
    return pcd_down, pcd_fpfh

def prepare_dataset(voxel_size):
    print(":: Load two point clouds and disturb initial pose.")
    source = source1
    target = target1
    trans_init = np.asarray([[0.0, 0.0, 1.0, 0.0],
                            [1.0, 0.0, 0.0, 0.0],
                            [0.0, 1.0, 0.0, 0.0],
                            [0.0, 0.0, 0.0, 1.0]])
    source.transform(trans_init)
    draw_registration_result(source, target, np.identity(4))

    source_down, source_fpfh = preprocess_point_cloud(source, voxel_size)
    target_down, target_fpfh = preprocess_point_cloud(target, voxel_size)
    return source, target, source_down, target_down, source_fpfh, target_fpfh

def execute_global_registration(
        source_down, target_down, source_fpfh, target_fpfh, voxel_size):
    distance_threshold = voxel_size * 1.5
    print(":: RANSAC registration on downsampled point clouds.")
    print("   Since the downsampling voxel size is %.3f," % voxel_size)
    print("   we use a liberal distance threshold %.3f." % distance_threshold)
    result = registration_ransac_based_on_feature_matching(
            source_down, target_down, source_fpfh, target_fpfh,
            distance_threshold,
            TransformationEstimationPointToPoint(False), 4,
            [CorrespondenceCheckerBasedOnEdgeLength(0.9),
            CorrespondenceCheckerBasedOnDistance(distance_threshold)],
            RANSACConvergenceCriteria(4000000, 500))
    return result


def refine_registration(source, target, source_fpfh, target_fpfh, voxel_size):
    distance_threshold = voxel_size * 0.4
    print(":: Point-to-plane ICP registration is applied on original point")
    print("   clouds to refine the alignment. This time we use a strict")
    print("   distance threshold %.3f." % distance_threshold)
    result = registration_icp(source, target, distance_threshold,
            result_ransac.transformation,
            TransformationEstimationPointToPlane())
    return result


if __name__ == "__main__":
    # voxel_size = 0.05  # means 5cm for the dataset
    voxel_size = 0.5  # means 5cm for the dataset
    source, target, source_down, target_down, source_fpfh, target_fpfh = \
            prepare_dataset(voxel_size)
    result_ransac = execute_global_registration(source_down, target_down,
            source_fpfh, target_fpfh, voxel_size)
    print(result_ransac)
    draw_registration_result(source_down, target_down,
            result_ransac.transformation)
    result_icp = refine_registration(source, target,
            source_fpfh, target_fpfh, voxel_size)
    print(result_icp)
    draw_registration_result(source, target, result_icp.transformation)