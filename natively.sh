#!/bin/bash

# export MESHES_DIR="/home/endre/r2r_ws/src/r2r_ur_controller/preparation/scenario/meshes"
# export SCENARIO_DIR="/home/endre/r2r_ws/src/r2r_ur_controller/preparation/scenario/transforms/scenario_2"
export MESHES_DIR="/home/endre/docker_ws/src/campx_demo_jun_2025/shared_folder/meshes"
export SCENARIO_DIR="/home/endre/docker_ws/src/campx_demo_jun_2025/shared_folder/transforms/scenario_final"
export REDIS_HOST="127.0.0.1"
export REDIS_PORT="6379"

echo "Environment variables set:"
echo "MESHES_DIR=${MESHES_DIR}"
echo "SCENARIO_DIR=${SCENARIO_DIR}"
echo "REDIS_HOST=${REDIS_HOST}"
echo "REDIS_PORT=${REDIS_PORT}"