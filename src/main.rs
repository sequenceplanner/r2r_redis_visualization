use r2r::builtin_interfaces::msg::Duration;

use r2r::tf2_msgs::msg::TFMessage;
use r2r::visualization_msgs::msg::{Marker, MarkerArray};
use r2r::QosProfile;
use std::error::Error;
use std::sync::Arc;

use r2r::geometry_msgs::msg::{Transform, TransformStamped, Vector3};

use r2r::builtin_interfaces::msg::Time;
use r2r::geometry_msgs::msg::{Point, Pose, Quaternion};
use r2r::std_msgs::msg::{ColorRGBA, Header};

use micro_sp::*;

pub static NODE_ID: &'static str = "redis_visualization";
pub static BUFFER_MAINTAIN_RATE: u64 = 20;
pub static MARKER_PUBLISH_RATE: u64 = 20;
pub static FRAME_LIFETIME: i32 = 3; //seconds

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    initialize_env_logger();

    // setup the node
    let ctx = r2r::Context::create()?;
    let mut node = r2r::Node::create(ctx, NODE_ID, "")?;

    let meshes_dir = std::env::var("MESHES_DIR").expect("MESHES_DIR is not set");
    let scenario_dir = std::env::var("SCENARIO_DIR").expect("SCENARIO_DIR is not set");

    let connection_manager = ConnectionManager::new().await;
    let mut con = connection_manager.get_connection().await;
    let _ = TransformsManager::load_transforms_from_path(&mut con, &scenario_dir.to_string()).await?;

    let marker_publisher_timer =
        node.create_wall_timer(std::time::Duration::from_millis(MARKER_PUBLISH_RATE))?;

    let zone_marker_publisher =
        node.create_publisher::<MarkerArray>("zone_markers", QosProfile::default())?;

    let mesh_marker_publisher =
        node.create_publisher::<MarkerArray>("mesh_markers", QosProfile::default())?;

    let static_frame_broadcaster = node.create_publisher::<TFMessage>(
        "tf_static",
        QosProfile::transient_local(QosProfile::default()),
    )?;

    let active_frame_broadcaster = node
        .create_publisher::<TFMessage>("tf", QosProfile::transient_local(QosProfile::default()))?;

    let con_arc = Arc::new(connection_manager);
    tokio::task::spawn(async move {
        let result = visualization_server(
            mesh_marker_publisher,
            zone_marker_publisher,
            active_frame_broadcaster,
            static_frame_broadcaster,
            con_arc,
            marker_publisher_timer,
            meshes_dir,
        )
        .await;
        match result {
            Ok(()) => {
                log::info!(target: &&format!("r2r_redis_visualization"), "Visualization Server suceeded.")
            }
            Err(e) => {
                log::error!(target: &&format!("r2r_redis_visualization"), "Visualization Server failed with: {}.", e)
            }
        };
    });

    // keep the node alive
    let handle = std::thread::spawn(move || loop {
        node.spin_once(std::time::Duration::from_millis(1000));
    });

    log::warn!(target: &&format!("r2r_redis_visualization"), "Node started.");

    handle.join().unwrap();

    Ok(())
}

pub async fn visualization_server(
    mesh_publisher: r2r::Publisher<MarkerArray>,
    zone_publisher: r2r::Publisher<MarkerArray>,
    active_frame_broadcaster: r2r::Publisher<TFMessage>,
    static_frame_broadcaster: r2r::Publisher<TFMessage>,
    connection_manager: Arc<ConnectionManager>,
    mut timer: r2r::Timer,
    meshes_dir: String,
) -> Result<(), Box<dyn std::error::Error>> {
    
    loop {
        timer.tick().await?;
        let mut con = connection_manager.get_connection().await;
        if let Err(_) = connection_manager
            .check_redis_health("redis_visualization")
            .await
        {
            continue;
        }
        let mut mesh_markers: Vec<Marker> = vec![];
        let mut zone_markers: Vec<Marker> = vec![];
        let mut active_transforms = vec![];
        let mut static_transforms = vec![];
        let frames_local = TransformsManager::get_all_transforms(&mut con).await?;
        let mut id: i32 = 0;
        for (_, frame) in frames_local {
            let mut clock = r2r::Clock::create(r2r::ClockType::RosTime).unwrap();
            let now = clock.get_now().unwrap();
            let time_stamp = r2r::Clock::to_builtin_time(&now);

            if frame.active_transform {
                active_transforms.push(TransformStamped {
                    header: Header {
                        stamp: time_stamp.clone(),
                        frame_id: frame.parent_frame_id.clone(),
                    },
                    child_frame_id: frame.child_frame_id.clone(),
                    transform: Transform {
                        translation: Vector3 {
                            x: *frame.transform.translation.x,
                            y: *frame.transform.translation.y,
                            z: *frame.transform.translation.z,
                        },
                        rotation: Quaternion {
                            x: *frame.transform.rotation.x,
                            y: *frame.transform.rotation.y,
                            z: *frame.transform.rotation.z,
                            w: *frame.transform.rotation.w,
                        },
                    },
                });
            } else {
                static_transforms.push(TransformStamped {
                    header: Header {
                        stamp: time_stamp.clone(),
                        frame_id: frame.parent_frame_id.clone(),
                    },
                    child_frame_id: frame.child_frame_id.clone(),
                    transform: Transform {
                        translation: Vector3 {
                            x: *frame.transform.translation.x,
                            y: *frame.transform.translation.y,
                            z: *frame.transform.translation.z,
                        },
                        rotation: Quaternion {
                            x: *frame.transform.rotation.x,
                            y: *frame.transform.rotation.y,
                            z: *frame.transform.rotation.z,
                            w: *frame.transform.rotation.w,
                        },
                    },
                });
            }

            let metadata = decode_metadata(&frame.metadata);
            if metadata.visualize_mesh {
                match metadata.mesh_file {
                    Some(path) => {
                        id = id + 1;
                        let indiv_marker = Marker {
                            header: Header {
                                stamp: Time { sec: 0, nanosec: 0 },
                                frame_id: frame.child_frame_id.to_string(),
                            },
                            ns: "".to_string(),
                            id,
                            type_: metadata.mesh_type,
                            action: 0,
                            pose: Pose {
                                position: Point {
                                    x: 0.0,
                                    y: 0.0,
                                    z: 0.0,
                                },
                                orientation: Quaternion {
                                    x: 0.0,
                                    y: 0.0,
                                    z: 0.0,
                                    w: 1.0,
                                },
                            },
                            lifetime: Duration { sec: 2, nanosec: 0 },
                            scale: Vector3 {
                                x: if metadata.mesh_scale != 0.0 {
                                    metadata.mesh_scale as f64
                                } else {
                                    1.0
                                },
                                y: if metadata.mesh_scale != 0.0 {
                                    metadata.mesh_scale as f64
                                } else {
                                    1.0
                                },
                                z: if metadata.mesh_scale != 0.0 {
                                    metadata.mesh_scale as f64
                                } else {
                                    1.0
                                },
                            },
                            color: ColorRGBA {
                                r: metadata.mesh_r,
                                g: metadata.mesh_g,
                                b: metadata.mesh_b,
                                a: metadata.mesh_a,
                            },
                            mesh_resource: format!("file://{}/{}", meshes_dir, path.to_string()),
                            ..Marker::default()
                        };
                        mesh_markers.push(indiv_marker);
                    }
                    None => (),
                }
            }
            if metadata.visualize_zone {
                if !(metadata.zone == 0.0) {
                    id = id + 1;
                    let indiv_marker = Marker {
                        header: Header {
                            stamp: Time { sec: 0, nanosec: 0 },
                            frame_id: frame.child_frame_id.to_string(),
                        },
                        ns: "".to_string(),
                        id,
                        type_: 2,
                        action: 0,
                        pose: Pose {
                            position: Point {
                                x: 0.0,
                                y: 0.0,
                                z: 0.0,
                            },
                            orientation: Quaternion {
                                x: 0.0,
                                y: 0.0,
                                z: 0.0,
                                w: 1.0,
                            },
                        },
                        lifetime: Duration { sec: 2, nanosec: 0 },
                        scale: Vector3 {
                            x: metadata.zone,
                            y: metadata.zone,
                            z: metadata.zone,
                        },
                        color: ColorRGBA {
                            r: 0.0,
                            g: 255.0,
                            b: 0.0,
                            a: 0.15,
                        },
                        ..Marker::default()
                    };
                    zone_markers.push(indiv_marker)
                }
            }
        }

        let active_msg = TFMessage {
            transforms: active_transforms,
        };

        let static_msg = TFMessage {
            transforms: static_transforms,
        };

        let zone_array_msg = MarkerArray {
            markers: zone_markers,
        };

        let mesh_array_msg = MarkerArray {
            markers: mesh_markers,
        };

        match active_frame_broadcaster.publish(&active_msg) {
            Ok(()) => (),
            Err(e) => {
                log::error!(target: &&format!("r2r_redis_visualization"),
                    "Active broadcaster failed to send a message with: '{}'",
                    e
                );
            }
        };

        match static_frame_broadcaster.publish(&static_msg) {
            Ok(()) => (),
            Err(e) => {
                log::error!(target: &&format!("r2r_redis_visualization"),
                    "Static broadcaster failed to send a message with: '{}'",
                    e
                );
            }
        };

        match zone_publisher.publish(&zone_array_msg) {
            Ok(()) => (),
            Err(e) => {
                log::error!(target: &&format!("r2r_redis_visualization"),
                    "Publisher failed to send zone marker message with: {}",
                    e
                );
            }
        };

        match mesh_publisher.publish(&mesh_array_msg) {
            Ok(()) => (),
            Err(e) => {
                log::error!(target: &&format!("r2r_redis_visualization"),
                    "Publisher failed to send mesh marker message with: {}",
                    e
                );
            }
        };
    }
}
