import { useCallback } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  addEdge,
  useNodesState,
  useEdgesState,
  Handle,
  Position,
} from "@xyflow/react";

import "@xyflow/react/dist/style.css";
import "./App.css";

// Custom Node
function ServiceNode({ data }) {
  return (
    <div className={`service-node ${data.type}`}>
      <Handle type="target" position={Position.Left} />

      <div className="node-icon">{data.icon}</div>

      <div>
        <div className="node-title">{data.label}</div>
        <div className="node-subtitle">{data.subtitle}</div>
      </div>

      <Handle type="source" position={Position.Right} />
    </div>
  );
}

// Partition Node
function PartitionNode({ data }) {
  return (
    <div className="partition-node">
      <Handle type="target" position={Position.Left} />

      <div className="partition-header">
        <span className="partition-icon">▣</span>
        <strong>{data.label}</strong>
      </div>

      <div className="partition-status">
        <span className="status-dot"></span>
        Active
      </div>

      <div className="partition-info">
        Kafka Partition
      </div>

      <Handle type="source" position={Position.Right} />
    </div>
  );
}

const nodeTypes = {
  service: ServiceNode,
  partition: PartitionNode,
};

const initialNodes = [
  {
    id: "producer",
    type: "service",
    position: { x: 50, y: 250 },
    data: {
      label: "Telemetry Producer",
      subtitle: "Python • IoT Data",
      icon: "🚚",
      type: "producer",
    },
  },

  {
    id: "kafka",
    type: "service",
    position: { x: 390, y: 250 },
    data: {
      label: "Apache Kafka",
      subtitle: "truck-telemetry",
      icon: "⚡",
      type: "kafka",
    },
  },

  {
    id: "p0",
    type: "partition",
    position: { x: 730, y: 80 },
    data: {
      label: "Partition 0",
    },
  },

  {
    id: "p1",
    type: "partition",
    position: { x: 730, y: 210 },
    data: {
      label: "Partition 1",
    },
  },

  {
    id: "p2",
    type: "partition",
    position: { x: 730, y: 340 },
    data: {
      label: "Partition 2",
    },
  },

  {
    id: "p3",
    type: "partition",
    position: { x: 730, y: 470 },
    data: {
      label: "Partition 3",
    },
  },

  {
    id: "worker",
    type: "service",
    position: { x: 1070, y: 250 },
    data: {
      label: "Stream Processor",
      subtitle: "Week 2 • Coming Next",
      icon: "⚙️",
      type: "worker",
    },
  },
];

const initialEdges = [
  {
    id: "producer-kafka",
    source: "producer",
    target: "kafka",
    animated: true,
    label: "Telemetry Events",
  },

  {
    id: "kafka-p0",
    source: "kafka",
    target: "p0",
    animated: true,
  },

  {
    id: "kafka-p1",
    source: "kafka",
    target: "p1",
    animated: true,
  },

  {
    id: "kafka-p2",
    source: "kafka",
    target: "p2",
    animated: true,
  },

  {
    id: "kafka-p3",
    source: "kafka",
    target: "p3",
    animated: true,
  },

  {
    id: "p0-worker",
    source: "p0",
    target: "worker",
  },

  {
    id: "p1-worker",
    source: "p1",
    target: "worker",
  },

  {
    id: "p2-worker",
    source: "p2",
    target: "worker",
  },

  {
    id: "p3-worker",
    source: "p3",
    target: "worker",
  },
];

function App() {
  const [nodes, setNodes, onNodesChange] =
    useNodesState(initialNodes);

  const [edges, setEdges, onEdgesChange] =
    useEdgesState(initialEdges);

  const onConnect = useCallback(
    (params) =>
      setEdges((eds) =>
        addEdge(
          {
            ...params,
            animated: true,
          },
          eds
        )
      ),
    [setEdges]
  );

  return (
    <div className="app">

      {/* Header */}
      <header className="header">
        <div>
          <h1>StreamForge</h1>
          <p>Distributed Python Event Processor</p>
        </div>

        <div className="system-status">
          <span className="online-dot"></span>
          System Online
        </div>
      </header>

      {/* Main Dashboard */}
      <main className="dashboard">

        {/* Statistics */}
        <section className="stats">

          <div className="stat-card">
            <span className="stat-icon">📡</span>
            <div>
              <h3>Telemetry</h3>
              <p>Live Streaming</p>
            </div>
          </div>

          <div className="stat-card">
            <span className="stat-icon">⚡</span>
            <div>
              <h3>Kafka</h3>
              <p>Connected</p>
            </div>
          </div>

          <div className="stat-card">
            <span className="stat-icon">▣</span>
            <div>
              <h3>Partitions</h3>
              <p>4 Active</p>
            </div>
          </div>

          <div className="stat-card">
            <span className="stat-icon">🚚</span>
            <div>
              <h3>Truck IDs</h3>
              <p>50,000 Supported</p>
            </div>
          </div>

        </section>

        {/* Topology */}
        <section className="topology-card">

          <div className="topology-header">
            <div>
              <h2>System Topology</h2>
              <p>
                Real-time distributed event processing architecture
              </p>
            </div>

            <div className="topic-badge">
              Topic: truck-telemetry
            </div>
          </div>

          <div className="flow-container">

            <ReactFlow
              nodes={nodes}
              edges={edges}
              nodeTypes={nodeTypes}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              fitView
              attributionPosition="bottom-left"
            >
              <Background gap={20} />
              <Controls />
              <MiniMap />
            </ReactFlow>

          </div>

        </section>

        {/* Architecture Information */}
        <section className="info-section">

          <div className="info-card">
            <h3>Data Flow</h3>
            <p>
              IoT truck telemetry is generated by the Python producer
              and published to the Kafka topic.
            </p>
          </div>

          <div className="info-card">
            <h3>Kafka Layer</h3>
            <p>
              The truck-telemetry topic distributes events across
              four Kafka partitions for scalable processing.
            </p>
          </div>

          <div className="info-card">
            <h3>Next Phase</h3>
            <p>
              Stream processing workers will consume partitions and
              calculate truck-level rolling averages.
            </p>
          </div>

        </section>

      </main>

      <footer>
        StreamForge • Distributed Systems & Big Data • Week 1
      </footer>

    </div>
  );
}

export default App;