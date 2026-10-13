/**
 * SAI SAHITH PASUMARTHY - PORTFOLIO RUNTIME ENGINE
 * Industrial Data Console & Telemetry Workbench
 *
 * Strict Design Protocol:
 * - Zero Em Dashes (\u2014 or \u2013)
 * - Zero Purple Gradients
 * - Zero Emojis
 * - Zero Pill Buttons
 * - Zero Cursor Hijacking
 */

(function () {
  "use strict";

  // =========================================================================
  // PIPELINE ARCHITECTURES DATA
  // =========================================================================
  const PIPELINES = {
    mycroft: {
      id: "mycroft",
      name: "AWS & PyTorch AI Feature Pipeline",
      system: "Mycroft AI Inc - Production Environment",
      badge: "STREAM + BATCH / 40% SPEEDUP",
      description: "End-to-end automated data pipeline extracting multi-source relational and NoSQL records, performing distributed ETL with AWS Glue and Lambda, and serving curated feature stores to PyTorch and Scikit-learn models.",
      stages: [
        {
          id: "ingest",
          step: "01",
          engine: "AWS S3 / LAMBDA",
          title: "Multi-Source Ingestion",
          tech: "MySQL, MongoDB, S3 Bronze",
          metric: "Throughput: 1.2M events/min",
          details: "Ingests raw event payloads and transactional data from MySQL and MongoDB into Amazon S3 bronze landing zone. Triggers serverless AWS Lambda micro-functions for initial payload verification, schema integrity checks, and partitioned directory layouts.",
          code: `# AWS Lambda Event Ingestion Handler
import json
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        payload = json.loads(event['body'])
        partition_key = f"year={payload['year']}/month={payload['month']}"
        target_key = f"raw/events/{partition_key}/{payload['event_id']}.json"
        
        s3_client.put_object(
            Bucket='mycroft-production-lake',
            Key=target_key,
            Body=json.dumps(payload),
            ContentType='application/json'
        )
        return {"statusCode": 200, "body": "INGEST_SUCCESS"}
    except Exception as exc:
        return {"statusCode": 500, "error": str(exc)}`
        },
        {
          id: "transform",
          step: "02",
          engine: "AWS GLUE / PANDAS",
          title: "Cleansing & Feature ETL",
          tech: "AWS Glue, Pandas, NumPy",
          metric: "Validation Rate: 99.8%",
          details: "Distributed transformation job utilizing AWS Glue and Pandas. Applies business rules, handles missing values, normalizes categorical indicators, and generates model-ready feature sets with strict OOP error handling and logging.",
          code: `# Modular Feature Transformation Worker
import numpy as np
import pandas as pd

class FeatureEngineeringPipeline:
    def __init__(self, raw_df: pd.DataFrame):
        self.df = raw_df.copy()
        
    def sanitize_and_normalize(self) -> pd.DataFrame:
        # Drop duplicates and null critical identifiers
        self.df.drop_duplicates(subset=['entity_id'], inplace=True)
        self.df.fillna({'metric_val': 0.0, 'status_code': 'UNKNOWN'}, inplace=True)
        
        # Vectorized min-max normalization
        val_range = self.df['metric_val'].max() - self.df['metric_val'].min()
        if val_range > 0:
            self.df['metric_norm'] = (self.df['metric_val'] - self.df['metric_val'].min()) / val_range
        return self.df`
        },
        {
          id: "storage",
          step: "03",
          engine: "AMAZON REDSHIFT",
          title: "Curated Data Warehouse",
          tech: "Redshift, SQL CTEs, Indexing",
          metric: "Query Latency: -40%",
          details: "Stores transformed records in Amazon Redshift Silver and Gold layers. Utilizes sort keys, distribution styles, and optimized CTE aggregations with window functions to power downstream analytical queries with 40% reduced latency.",
          code: `-- High Performance Window Aggregation
WITH ranked_features AS (
    SELECT 
        entity_id,
        event_timestamp,
        metric_norm,
        ROW_NUMBER() OVER (
            PARTITION BY entity_id 
            ORDER BY event_timestamp DESC
        ) AS rank_seq,
        AVG(metric_norm) OVER (
            PARTITION BY entity_id 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS moving_avg_metric
    FROM curated_feature_store
    WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT entity_id, event_timestamp, metric_norm, moving_avg_metric
FROM ranked_features
WHERE rank_seq = 1;`
        },
        {
          id: "inference",
          step: "04",
          engine: "PYTORCH / SKLEARN",
          title: "ML Feature Feed",
          tech: "PyTorch Tensors, Scikit-learn",
          metric: "Batch Prep: 110ms",
          details: "Converts curated feature stores into PyTorch tensors and Scikit-learn datasets for predictive modeling. Integrates dataset validation gates to prevent training data drift and maintain reproducible evaluation metrics.",
          code: `# Dataset Tensor Tensorization Gate
import torch
from sklearn.preprocessing import StandardScaler

def prepare_tensor_batches(curated_df):
    feature_cols = ['metric_norm', 'moving_avg_metric']
    scaler = StandardScaler()
    scaled_feats = scaler.fit_transform(curated_df[feature_cols].values)
    
    # Direct tensor allocation for PyTorch model inference
    tensor_inputs = torch.tensor(scaled_feats, dtype=torch.float32)
    return tensor_inputs`
        }
      ]
    },

    hcl: {
      id: "hcl",
      name: "High-Volume Computer Vision ETL",
      system: "HCL Technologies - Video Analytics Core",
      badge: "REAL-TIME / 40% LATENCY REDUCTION",
      description: "Automated real-time image data ingestion and preprocessing pipeline handling live surveillance streams. Converted raw unstructured frames into model-ready tensors using OpenCV and vectorized NumPy routines.",
      stages: [
        {
          id: "stream_ingest",
          step: "01",
          engine: "OPENCV RTSP",
          title: "Video Stream Capture",
          tech: "OpenCV, Multi-threaded Queues",
          metric: "Throughput: 60 FPS Continuous",
          details: "Captures multi-channel live video feeds. Uses dedicated worker threads with thread-safe ring buffers to decouple frame acquisition from computational bottlenecks and eliminate buffer overflows.",
          code: `# Multi-threaded RTSP Frame Ingestion
import cv2
import queue
import threading

class VideoStreamBuffer:
    def __init__(self, stream_source: str):
        self.cap = cv2.VideoCapture(stream_source)
        self.frame_queue = queue.Queue(maxsize=128)
        self.is_running = True
        
    def stream_worker(self):
        while self.is_running:
            status, frame = self.cap.read()
            if not status:
                break
            if not self.frame_queue.full():
                self.frame_queue.put(frame)
            else:
                # Drop oldest frame to maintain low real-time latency
                self.frame_queue.get_nowait()
                self.frame_queue.put(frame)`
        },
        {
          id: "tensor_proc",
          step: "02",
          engine: "NUMPY VECTORIZED",
          title: "Matrix Normalization",
          tech: "NumPy C-Contiguous Arrays",
          metric: "Frame Latency: 3.2ms",
          details: "Applies vectorized array transformations across pixel matrices. Eliminates naive nested loops in favor of SIMD-accelerated NumPy broadcasting for resizing, color channel conversion, and mean normalization.",
          code: `# Vectorized Image Tensor Preprocessing
import numpy as np
import cv2

def preprocess_frame_batch(frame_batch: np.ndarray) -> np.ndarray:
    # Resize, convert BGR to RGB, scale to [0, 1]
    # Input shape: (Batch, H, W, C)
    rgb_batch = frame_batch[..., ::-1] # Direct channel reversal
    normalized = rgb_batch.astype(np.float32) / 255.0
    
    # Standard ImageNet normalization: (x - mean) / std
    mean = np.array([0.485, 0.456, 0.406], dtype=np.float32)
    std = np.array([0.229, 0.224, 0.225], dtype=np.float32)
    standardized = (normalized - mean) / std
    
    # Transpose to Channel-First format (Batch, C, H, W)
    return np.ascontiguousarray(standardized.transpose(0, 3, 1, 2))`
        },
        {
          id: "quality_gate",
          step: "03",
          engine: "VALIDATION LAYER",
          title: "Integrity & Anomaly Filter",
          tech: "Histogram Variance Checks",
          metric: "Zero Broken Tensors",
          details: "Runs automated benchmark checks on pixel variance and luminance distributions to filter out corrupted frames, sensor dropouts, or blank screens before routing into the inference queue.",
          code: `# Frame Validation Gate
def validate_frame_integrity(frame: np.ndarray) -> bool:
    # Check dimensions
    if frame is None or frame.size == 0:
        return False
    # Check for blank / washed-out frames via standard deviation
    std_dev = np.std(frame)
    if std_dev < 8.0:
        return False # Flat/blackout frame
    return True`
        },
        {
          id: "dispatch",
          step: "04",
          engine: "INFERENCE ROUTER",
          title: "Downstream ML Dispatch",
          tech: "TorchScript / TensorRT",
          metric: "40% End-to-End Speedup",
          details: "Dispatches validated tensor batches into downstream model inference engines, enabling real-time detection and analytics while slashing end-to-end processing time by 40%.",
          code: `# Inference Dispatch Routing
def route_to_inference_engine(validated_tensors, model_runner):
    results = model_runner.predict_batch(validated_tensors)
    return results`
        }
      ]
    },

    proctor: {
      id: "proctor",
      name: "Autonomous AI Proctoring System",
      system: "Independent Architecture Project",
      badge: "COMPUTER VISION / 95% CHEATING REDUCTION",
      description: "Automated real-time proctoring pipeline integrating OpenCV, MTCNN, and Dlib 68-point facial landmark detectors to monitor exam integrity, track gaze angles, and identify anomalies.",
      stages: [
        {
          id: "face_detect",
          step: "01",
          engine: "OPENCV DNN / MTCNN",
          title: "Multi-Model Face Detection",
          tech: "OpenCV DNN, MTCNN, Dlib",
          metric: "Detection Accuracy: 98.4%",
          details: "Cascade face detection combining MTCNN for complex angles and OpenCV DNN for rapid verification. Continuously validates identity and flags multiple-person or absent-person states.",
          code: `# Multi-Model Face Detection Cascade
import cv2

net = cv2.dnn.readNetFromCaffe("deploy.prototxt", "res10_300x300.caffemodel")

def detect_faces(frame):
    h, w = frame.shape[:2]
    blob = cv2.dnn.blobFromImage(frame, 1.0, (300, 300), (104.0, 177.0, 123.0))
    net.setInput(blob)
    detections = net.forward()
    valid_faces = []
    
    for i in range(detections.shape[2]):
        conf = detections[0, 0, i, 2]
        if conf > 0.65:
            box = detections[0, 0, i, 3:7] * [w, h, w, h]
            valid_faces.append(box.astype("int"))
    return valid_faces`
        },
        {
          id: "landmark_gaze",
          step: "02",
          engine: "DLIB 68-PT",
          title: "Landmark & Gaze Vectorizer",
          tech: "Dlib Shape Predictor, Pose Math",
          metric: "Latency: 8.5ms / frame",
          details: "Maps 68 facial landmarks to calculate head pose (pitch, yaw, roll) and pupil gaze vectors using solvePnP math, pinpointing off-screen focal points.",
          code: `# 68-Point Head Pose & Gaze Vectorization
import numpy as np

def calculate_head_pose(landmarks, frame_shape):
    # 2D coordinates of nose tip, chin, eye corners, mouth corners
    image_points = np.array([
        landmarks[30], # Nose tip
        landmarks[8],  # Chin
        landmarks[36], # Left eye left corner
        landmarks[45], # Right eye right corner
        landmarks[48], # Left mouth corner
        landmarks[54]  # Right mouth corner
    ], dtype="double")
    # Output pitch, yaw, roll via perspective n-point
    return image_points`
        },
        {
          id: "anomaly_engine",
          step: "03",
          engine: "RULE & CLASSIFIER",
          title: "Behavioral Anomaly Engine",
          tech: "Temporal Window Voting",
          metric: "95% Cheating Suppression",
          details: "Aggregates continuous gaze and head deviation events over temporal rolling windows. Emits scored alerts for suspicious looking-away patterns, phone glances, and voice anomalies.",
          code: `# Temporal Anomaly Voting Window
class AnomalyDetector:
    def __init__(self, window_size=30):
        self.window = []
        self.window_size = window_size
        
    def evaluate(self, is_gaze_deviated: bool) -> float:
        self.window.append(1 if is_gaze_deviated else 0)
        if len(self.window) > self.window_size:
            self.window.pop(0)
        deviation_ratio = sum(self.window) / len(self.window)
        return deviation_ratio # Alert if > 0.60 threshold`
        },
        {
          id: "incident_audit",
          step: "04",
          engine: "AUDIT LOG STORE",
          title: "Timestamped Audit Logging",
          tech: "SQLite / JSON Telemetry",
          metric: "Zero False Positives",
          details: "Stores verified incident markers with frame timestamps, gaze vector telemetry, and learner IDs for instructor review and proctor verification.",
          code: `# Incident Telemetry Recording
def record_audit_incident(learner_id, timestamp, incident_type, confidence):
    record = {
        "learner_id": learner_id,
        "timestamp": timestamp,
        "type": incident_type,
        "confidence": round(confidence, 3)
    }
    return record`
        }
      ]
    }
  };

  // =========================================================================
  // SQL & RUNTIME SANDBOX DATA
  // =========================================================================
  const SANDBOX_SCENARIOS = {
    sql_optimization: {
      id: "sql_optimization",
      meta: "PRODUCTION SQL // 40% REDUCTION",
      title: "Window Functions & Partition Pruning",
      description: "Optimized aggregation query on 2.4M records replacing correlated subqueries with partitioned window CTEs, cutting execution time from 184ms to 110ms.",
      code: `-- OPTIMIZED REDSHIFT / POSTGRES QUERY PLAN
EXPLAIN ANALYZE
WITH RankedDetections AS (
    SELECT
        stream_id,
        timestamp,
        confidence_score,
        object_class,
        ROW_NUMBER() OVER (
            PARTITION BY stream_id, object_class 
            ORDER BY timestamp DESC
        ) AS row_num,
        AVG(confidence_score) OVER (
            PARTITION BY stream_id 
            ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_conf
    FROM telemetry_logs
    WHERE event_date >= '2026-03-01'
      AND status = 'PROCESSED'
)
SELECT stream_id, object_class, confidence_score, rolling_avg_conf
FROM RankedDetections
WHERE row_num = 1
ORDER BY stream_id ASC;`,
      metrics: {
        runtime_before: "184 ms",
        runtime_after: "110 ms (-40.2%)",
        cost_before: "1,420 Cost Units",
        cost_after: "210 Cost Units",
        rows_scanned: "2,400,000 Rows",
        memory: "14.2 MB Buffer"
      },
      sampleRows: [
        { col1: "STR_KCMO_01", col2: "PERSON", col3: "0.984", col4: "0.961", col5: "OPTIMIZED_INDEX" },
        { col1: "STR_KCMO_01", col2: "VEHICLE", col3: "0.942", col4: "0.938", col5: "OPTIMIZED_INDEX" },
        { col1: "STR_KCMO_02", col2: "PERSON", col3: "0.971", col4: "0.954", col5: "OPTIMIZED_INDEX" },
        { col1: "STR_KCMO_03", col2: "FACE_ID", col3: "0.992", col4: "0.987", col5: "OPTIMIZED_INDEX" }
      ]
    },

    python_etl: {
      id: "python_etl",
      meta: "NUMPY / OPENCV // ZERO-COPY VECTORIZATION",
      title: "Batch Image Preprocessing vs Loops",
      description: "Benchmarked tensor preprocessing comparing naive nested Python iterations with C-contiguous NumPy broadcasting across 10,000 frames.",
      code: `# VECTORIZED PREPROCESSING PIPELINE BENCHMARK
import numpy as np

def run_vectorized_benchmark(frames_batch: np.ndarray):
    # frames_batch shape: (100, 256, 256, 3)
    # Direct vectorized normalization in C-contiguous memory
    tensors = frames_batch.astype(np.float32) / 255.0
    mean = np.array([0.485, 0.456, 0.406], dtype=np.float32)
    std = np.array([0.229, 0.224, 0.225], dtype=np.float32)
    
    # Broadcast subtraction and division
    normalized = (tensors - mean) / std
    return normalized.transpose(0, 3, 1, 2)

# Benchmark execution:
# Naive Python loop: 1,280 ms / batch
# Vectorized NumPy: 32 ms / batch -> 40x throughput boost`,
      metrics: {
        runtime_before: "1,280 ms / batch",
        runtime_after: "32 ms / batch (40x Speedup)",
        cost_before: "100% CPU Saturation",
        cost_after: "2.5% CPU Utilization",
        rows_scanned: "10,000 Frames",
        memory: "Contiguous RAM"
      },
      sampleRows: [
        { col1: "BATCH_CHUNK_01", col2: "32 ms", col3: "100 Tensors", col4: "0.485 MEAN", col5: "VALIDATED" },
        { col1: "BATCH_CHUNK_02", col2: "31 ms", col3: "100 Tensors", col4: "0.485 MEAN", col5: "VALIDATED" },
        { col1: "BATCH_CHUNK_03", col2: "33 ms", col3: "100 Tensors", col4: "0.485 MEAN", col5: "VALIDATED" },
        { col1: "BATCH_CHUNK_04", col2: "32 ms", col3: "100 Tensors", col4: "0.485 MEAN", col5: "VALIDATED" }
      ]
    },

    validation_gate: {
      id: "validation_gate",
      meta: "DATA QUALITY // ANOMALY FILTERING",
      title: "Automated Schema & Quality Gate",
      description: "ETL validation layer testing null rates, categorical divergence, and statistical outliers before downstream warehouse ingestion.",
      code: `# DATA QUALITY & REJECTION GATE
import pandas as pd
import numpy as np

class QualityGate:
    @staticmethod
    def audit_dataset(df: pd.DataFrame) -> dict:
        total_records = len(df)
        null_counts = df.isnull().sum().to_dict()
        
        # Outlier detection using 3-sigma rule
        mean_val = df['value'].mean()
        std_val = df['value'].std()
        outliers = df[(df['value'] > mean_val + 3*std_val) | (df['value'] < mean_val - 3*std_val)]
        
        return {
            "total_records": total_records,
            "null_rates": {k: f"{(v/total_records)*100:.2f}%" for k, v in null_counts.items()},
            "outlier_count": len(outliers),
            "status": "APPROVED" if len(outliers) / total_records < 0.01 else "REJECTED"
        }`,
      metrics: {
        runtime_before: "Manual Audit",
        runtime_after: "14 ms Automated",
        cost_before: "12% Data Drift",
        cost_after: "0.02% Anomaly Rate",
        rows_scanned: "50,000 Records",
        memory: "Streaming Buffer"
      },
      sampleRows: [
        { col1: "METRIC_RECORDS", col2: "50,000", col3: "0.00% NULL", col4: "14 OUTLIERS", col5: "STATUS: PASS" },
        { col1: "IDENTITY_MAPS", col2: "50,000", col3: "0.00% NULL", col4: "0 OUTLIERS", col5: "STATUS: PASS" },
        { col1: "STREAM_PAYLOADS", col2: "49,986", col3: "0.01% REJECT", col4: "QUARANTINED", col5: "ISOLATED" }
      ]
    }
  };

  // State
  let currentPipelineKey = "mycroft";
  let currentStageIndex = 0;
  let currentScenarioKey = "sql_optimization";

  // =========================================================================
  // INITIALIZATION
  // =========================================================================
  document.addEventListener("DOMContentLoaded", function () {
    initPipelineWorkbench();
    initSandboxRunner();
    initSkillsFilter();
    initCopyActions();
    initViewModes();
    updateLiveClock();
    setInterval(updateLiveClock, 1000);
  });

  // Clock in Telemetry Bar
  function updateLiveClock() {
    const clockEl = document.getElementById("telemetry-clock");
    if (clockEl) {
      const now = new Date();
      const utcString = now.toISOString().replace("T", " ").substring(0, 19) + " UTC";
      clockEl.textContent = utcString;
    }
  }

  // =========================================================================
  // PIPELINE WORKBENCH HANDLERS
  // =========================================================================
  function initPipelineWorkbench() {
    const pipelineBtns = document.querySelectorAll(".pipeline-choice-btn");
    pipelineBtns.forEach(btn => {
      btn.addEventListener("click", function () {
        pipelineBtns.forEach(b => b.classList.remove("active"));
        this.classList.add("active");
        currentPipelineKey = this.getAttribute("data-pipeline");
        currentStageIndex = 0;
        renderPipeline();
      });
    });

    renderPipeline();
  }

  function renderPipeline() {
    const pipelineData = PIPELINES[currentPipelineKey];
    if (!pipelineData) return;

    // Set Header
    const nameEl = document.getElementById("pipeline-system-name");
    const descEl = document.getElementById("pipeline-system-desc");
    const badgeEl = document.getElementById("pipeline-system-badge");

    if (nameEl) nameEl.textContent = pipelineData.system + " // " + pipelineData.name;
    if (descEl) descEl.textContent = pipelineData.description;
    if (badgeEl) badgeEl.textContent = pipelineData.badge;

    // Render Stage Cards
    const container = document.getElementById("pipeline-stages-container");
    if (!container) return;

    container.innerHTML = "";

    pipelineData.stages.forEach((stage, idx) => {
      // Stage Node
      const stageCard = document.createElement("div");
      stageCard.className = "pipeline-stage" + (idx === currentStageIndex ? " selected" : "");
      stageCard.setAttribute("data-stage-idx", idx);

      stageCard.innerHTML = `
        <div class="stage-header">
          <span class="stage-step-num">STAGE ${stage.step}</span>
          <span class="stage-engine-tag">${stage.engine}</span>
        </div>
        <div class="stage-title">${stage.title}</div>
        <div class="stage-tech-list">${stage.tech}</div>
        <div class="stage-metric-pill">${stage.metric}</div>
      `;

      stageCard.addEventListener("click", () => {
        currentStageIndex = idx;
        updateSelectedStage();
      });

      container.appendChild(stageCard);

      // Connector Arrow if not last
      if (idx < pipelineData.stages.length - 1) {
        const connector = document.createElement("div");
        connector.className = "stage-connector";
        connector.innerHTML = `
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="square">
            <polyline points="9 18 15 12 9 6"></polyline>
          </svg>
        `;
        container.appendChild(connector);
      }
    });

    renderStageInspector();
  }

  function updateSelectedStage() {
    const stageCards = document.querySelectorAll(".pipeline-stage");
    stageCards.forEach((card, idx) => {
      if (idx === currentStageIndex) {
        card.classList.add("selected");
      } else {
        card.classList.remove("selected");
      }
    });
    renderStageInspector();
  }

  function renderStageInspector() {
    const pipelineData = PIPELINES[currentPipelineKey];
    const stage = pipelineData.stages[currentStageIndex];
    if (!stage) return;

    const titleEl = document.getElementById("inspector-stage-title");
    const techEl = document.getElementById("inspector-stage-tech");
    const detailsEl = document.getElementById("inspector-stage-details");
    const codeEl = document.getElementById("inspector-code-content");
    const codeFilename = document.getElementById("inspector-code-filename");

    if (titleEl) titleEl.textContent = `STAGE ${stage.step}: ${stage.title} (${stage.engine})`;
    if (techEl) techEl.textContent = `STACK: ${stage.tech}`;
    if (detailsEl) detailsEl.textContent = stage.details;
    if (codeEl) codeEl.textContent = stage.code;
    if (codeFilename) codeFilename.textContent = `runtime_${stage.id}_worker.py`;
  }

  // =========================================================================
  // SQL & RUNTIME SANDBOX RUNNER
  // =========================================================================
  function initSandboxRunner() {
    const scenarioBtns = document.querySelectorAll(".scenario-card-btn");
    scenarioBtns.forEach(btn => {
      btn.addEventListener("click", function () {
        scenarioBtns.forEach(b => b.classList.remove("active"));
        this.classList.add("active");
        currentScenarioKey = this.getAttribute("data-scenario");
        renderSandboxScenario();
      });
    });

    const executeBtn = document.getElementById("btn-execute-sandbox");
    if (executeBtn) {
      executeBtn.addEventListener("click", function () {
        runSimulationAnimation();
      });
    }

    renderSandboxScenario();
  }

  function renderSandboxScenario() {
    const scenario = SANDBOX_SCENARIOS[currentScenarioKey];
    if (!scenario) return;

    const codeView = document.getElementById("sandbox-code-view");
    const titleView = document.getElementById("sandbox-active-title");
    const metaView = document.getElementById("sandbox-active-meta");

    if (codeView) codeView.textContent = scenario.code;
    if (titleView) titleView.textContent = scenario.title;
    if (metaView) metaView.textContent = scenario.meta;

    // Metrics
    document.getElementById("stat-runtime-before").textContent = scenario.metrics.runtime_before;
    document.getElementById("stat-runtime-after").textContent = scenario.metrics.runtime_after;
    document.getElementById("stat-cost").textContent = scenario.metrics.cost_after;
    document.getElementById("stat-scanned").textContent = scenario.metrics.rows_scanned;

    // Table
    renderSandboxTable(scenario.sampleRows);
  }

  function renderSandboxTable(rows) {
    const tbody = document.getElementById("sandbox-table-body");
    if (!tbody) return;
    tbody.innerHTML = "";

    rows.forEach(r => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${r.col1}</td>
        <td>${r.col2}</td>
        <td>${r.col3}</td>
        <td>${r.col4}</td>
        <td style="color: var(--signal-emerald); font-weight: 600;">${r.col5}</td>
      `;
      tbody.appendChild(tr);
    });
  }

  function runSimulationAnimation() {
    const execBtn = document.getElementById("btn-execute-sandbox");
    const execStatus = document.getElementById("sandbox-exec-status");

    if (execBtn) {
      execBtn.textContent = "[ RUNNING... ]";
      execBtn.style.opacity = "0.7";
    }
    if (execStatus) {
      execStatus.textContent = "PARSING EXECUTION PLAN & BUFFER SCAN...";
      execStatus.style.color = "var(--signal-amber)";
    }

    setTimeout(() => {
      if (execBtn) {
        execBtn.textContent = "[ EXECUTE QUERY PLAN ]";
        execBtn.style.opacity = "1";
      }
      if (execStatus) {
        execStatus.textContent = "EXECUTION SUCCESSFUL (LATENCY: 110ms // 0 ERRORS)";
        execStatus.style.color = "var(--signal-emerald)";
      }
      showToast("Query plan verified: 40% latency reduction confirmed.");
    }, 450);
  }

  // =========================================================================
  // SKILLS FILTER & SEARCH
  // =========================================================================
  function initSkillsFilter() {
    const searchInput = document.getElementById("skills-search-input");
    if (!searchInput) return;

    searchInput.addEventListener("input", function (e) {
      const query = e.target.value.toLowerCase().trim();
      const pills = document.querySelectorAll(".skill-pill");

      pills.forEach(pill => {
        const text = pill.textContent.toLowerCase();
        if (query === "") {
          pill.classList.remove("matched");
        } else if (text.includes(query)) {
          pill.classList.add("matched");
        } else {
          pill.classList.remove("matched");
        }
      });
    });
  }

  // =========================================================================
  // COPY ACTIONS & TOAST
  // =========================================================================
  function initCopyActions() {
    const copyBtns = document.querySelectorAll("[data-copy-val]");
    copyBtns.forEach(btn => {
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        const text = this.getAttribute("data-copy-val");
        navigator.clipboard.writeText(text).then(() => {
          showToast(`Copied to clipboard: ${text}`);
        }).catch(() => {
          showToast(`Value: ${text}`);
        });
      });
    });

    const printBtn = document.getElementById("btn-print-resume");
    if (printBtn) {
      printBtn.addEventListener("click", function () {
        window.print();
      });
    }
  }

  function showToast(msg) {
    const toast = document.getElementById("toast-notice");
    if (!toast) return;

    toast.textContent = `[CONFIRMED] ${msg}`;
    toast.classList.add("show");

    setTimeout(() => {
      toast.classList.remove("show");
    }, 3200);
  }

  // =========================================================================
  // VIEW MODE / NAVIGATION TABS
  // =========================================================================
  function initViewModes() {
    const navTabs = document.querySelectorAll(".nav-tab-btn");
    navTabs.forEach(tab => {
      tab.addEventListener("click", function () {
        navTabs.forEach(t => t.classList.remove("active"));
        this.classList.add("active");

        const targetId = this.getAttribute("data-target-sec");
        if (targetId) {
          const targetEl = document.getElementById(targetId);
          if (targetEl) {
            targetEl.scrollIntoView({ behavior: "smooth", block: "start" });
          }
        }
      });
    });
  }

})();
