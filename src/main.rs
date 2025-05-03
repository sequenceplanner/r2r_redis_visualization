use r2r::builtin_interfaces::msg::Duration;

use r2r::tf2_msgs::msg::TFMessage;
use r2r::visualization_msgs::msg::{Marker, MarkerArray};
use r2r::QosProfile;
use std::error::Error;
use tokio::sync::{mpsc, oneshot};

use r2r::geometry_msgs::msg::{Transform, TransformStamped, Vector3};

use r2r::builtin_interfaces::msg::Time;
use r2r::geometry_msgs::msg::{Point, Pose, Quaternion};
use r2r::std_msgs::msg::{ColorRGBA, Header};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use micro_sp::*;

pub static NODE_ID: &'static str = "redis_visualization";
pub static BUFFER_MAINTAIN_RATE: u64 = 100;
pub static MARKER_PUBLISH_RATE: u64 = 20;
pub static FRAME_LIFETIME: i32 = 3; //seconds

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PotentialTransformMetadata {
    pub next_frame: Option<HashSet<String>>, // next frame, good for visualizing path plans
    pub frame_type: Option<String>,          // can be used separate waypoint, tag, human, etc.
    pub visualize_mesh: bool,
    pub visualize_zone: bool,
    pub zone: f64,      // when are you "at" the frame, threshold, in meters
    pub mesh_type: i32, // 1 - cube, 2 - sphere, 3 - cylinder or 10 - mesh (provide path)
    pub mesh_path: Option<String>,
    pub mesh_scale: f32,
    pub mesh_r: f32,
    pub mesh_g: f32,
    pub mesh_b: f32,
    pub mesh_a: f32,
}

impl Default for PotentialTransformMetadata {
    fn default() -> Self {
        PotentialTransformMetadata {
            next_frame: None,
            frame_type: None,
            visualize_mesh: false,
            visualize_zone: false,
            zone: 0.0,
            mesh_type: 10,
            mesh_path: None,
            mesh_scale: 0.001,
            mesh_r: 1.0,
            mesh_g: 1.0,
            mesh_b: 1.0,
            mesh_a: 1.0,
        }
    }
}

pub fn decode_metadata(map_value: &MapOrUnknown) -> PotentialTransformMetadata {
    let mut metadata = PotentialTransformMetadata::default();

    let map = match map_value {
        MapOrUnknown::Map(map) => map,
        MapOrUnknown::UNKNOWN => return metadata,
    };

    for (key_sp, sp_value) in map {
        let key_str = match key_sp {
            SPValue::String(StringOrUnknown::String(s)) => s.as_str(),
            _ => continue,
        };

        match key_str {
            "next_frame" => {
                if let SPValue::Array(ArrayOrUnknown::Array(arr)) = sp_value {
                    let mut string_set = HashSet::new();
                    for item_sp in arr {
                        if let SPValue::String(StringOrUnknown::String(s)) = item_sp {
                            string_set.insert(s.clone());
                        }
                    }
                    if !string_set.is_empty() {
                        metadata.next_frame = Some(string_set);
                    }
                }
            }
            "frame_type" => {
                if let SPValue::String(StringOrUnknown::String(s)) = sp_value {
                    metadata.frame_type = Some(s.clone());
                }
            }
            "visualize_mesh" => {
                if let SPValue::Bool(BoolOrUnknown::Bool(b)) = sp_value {
                    metadata.visualize_mesh = *b;
                }
            }
            "visualize_zone" => {
                if let SPValue::Bool(BoolOrUnknown::Bool(b)) = sp_value {
                    metadata.visualize_zone = *b;
                }
            }
            "zone" => {
                if let SPValue::Float64(FloatOrUnknown::Float64(of)) = sp_value {
                    metadata.zone = of.into_inner();
                }
            }
            "mesh_type" => {
                if let SPValue::Int64(IntOrUnknown::Int64(i)) = sp_value {
                    if let Ok(i32_val) = (*i).try_into() {
                        metadata.mesh_type = i32_val;
                    }
                }
            }
            "mesh_path" => {
                if let SPValue::String(StringOrUnknown::String(s)) = sp_value {
                    metadata.mesh_path = Some(s.clone());
                }
            }
            "mesh_scale" => {
                if let SPValue::Float64(FloatOrUnknown::Float64(of)) = sp_value {
                    metadata.mesh_scale = of.into_inner() as f32;
                }
            }
            "mesh_r" => {
                if let SPValue::Float64(FloatOrUnknown::Float64(of)) = sp_value {
                    metadata.mesh_r = of.into_inner() as f32;
                }
            }
            "mesh_g" => {
                if let SPValue::Float64(FloatOrUnknown::Float64(of)) = sp_value {
                    metadata.mesh_g = of.into_inner() as f32;
                }
            }
            "mesh_b" => {
                if let SPValue::Float64(FloatOrUnknown::Float64(of)) = sp_value {
                    metadata.mesh_b = of.into_inner() as f32;
                }
            }
            "mesh_a" => {
                if let SPValue::Float64(FloatOrUnknown::Float64(of)) = sp_value {
                    metadata.mesh_a = of.into_inner() as f32;
                }
            }
            _ => {} // Ignore unknown keys
        }
    }

    metadata
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // setup the node
    let ctx = r2r::Context::create()?;
    let mut node = r2r::Node::create(ctx, NODE_ID, "")?;

    let (tx, rx) = mpsc::channel(32);

    let path = "/home/endre/rust_crates/micro_sp/src/transforms/examples/data/";

    tokio::task::spawn(async move {
        match redis_state_manager(rx, State::new()).await {
            Ok(()) => (),
            Err(e) => r2r::log_error!(NODE_ID, "Node started."),
        };
    });

    tx.send(StateManagement::LoadTransformScenario(path.to_string()))
        .await
        .expect("failed");

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

    let tx_clone = tx.clone();
    tokio::task::spawn(async move {
        let result = visualization_server(
            mesh_marker_publisher,
            zone_marker_publisher,
            active_frame_broadcaster,
            static_frame_broadcaster,
            tx_clone,
            marker_publisher_timer,
        )
        .await;
        match result {
            Ok(()) => r2r::log_info!(NODE_ID, "Visualization Server succeeded."),
            Err(e) => r2r::log_error!(NODE_ID, "Visualization Server failed with: {}.", e),
        };
    });

    // keep the node alive
    let handle = std::thread::spawn(move || loop {
        node.spin_once(std::time::Duration::from_millis(1000));
    });

    r2r::log_warn!(NODE_ID, "Node started.");

    handle.join().unwrap();

    Ok(())
}

pub async fn visualization_server(
    mesh_publisher: r2r::Publisher<MarkerArray>,
    zone_publisher: r2r::Publisher<MarkerArray>,
    active_frame_broadcaster: r2r::Publisher<TFMessage>,
    static_frame_broadcaster: r2r::Publisher<TFMessage>,
    command_sender: mpsc::Sender<StateManagement>,
    mut timer: r2r::Timer,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let mut mesh_markers: Vec<Marker> = vec![];
        let mut zone_markers: Vec<Marker> = vec![];
        let mut active_transforms = vec![];
        let mut static_transforms = vec![];
        let (response_tx, response_rx) = oneshot::channel();
        command_sender
            .send(StateManagement::GetAllTransforms(response_tx))
            .await
            .expect("failed");

        let frames_local = response_rx.await.expect("failed");
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
                match metadata.mesh_path {
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
                            mesh_resource: format!("file://{}", path.to_string()),
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
                r2r::log_error!(
                    NODE_ID,
                    "Active broadcaster failed to send a message with: '{}'",
                    e
                );
            }
        };

        match static_frame_broadcaster.publish(&static_msg) {
            Ok(()) => (),
            Err(e) => {
                r2r::log_error!(
                    NODE_ID,
                    "Static broadcaster failed to send a message with: '{}'",
                    e
                );
            }
        };

        match zone_publisher.publish(&zone_array_msg) {
            Ok(()) => (),
            Err(e) => {
                r2r::log_error!(
                    NODE_ID,
                    "Publisher failed to send zone marker message with: {}",
                    e
                );
            }
        };

        match mesh_publisher.publish(&mesh_array_msg) {
            Ok(()) => (),
            Err(e) => {
                r2r::log_error!(
                    NODE_ID,
                    "Publisher failed to send mesh marker message with: {}",
                    e
                );
            }
        };

        timer.tick().await?;
    }
}
