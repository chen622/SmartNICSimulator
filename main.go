package main

import "fmt"

const (
	CPS                      = 100  // New connections per second.
	PPS                      = 1000 // Packets per second.
	BATCH_SIZE               = 4    // Packets per batch.
	MAX_OFFLOAD_SPEED        = 10 * 1000 * 1000
	MAX_SLOW_PATH_SPEED      = 100 * 1000 * 1000
	TURNS                    = 100 // Each turn represents 1 second.
	ELEPHANT_FLOW_PROPORTION = 20  // The proportion of elephant flows.
)

var flowCount uint64 = 0      // The index to generate new flow.
var offloadThreshold uint = 5 // The threshold of packet amount to offload a flow.

var offloadCount uint64 = 0  // The quantity of offloaded flows in this turn.
var slowPathCount uint64 = 0 // The quantity of packets in slow path in this turn.
var dropCount uint64 = 0     // The quantity of dropped packets in this turn.

type Flow struct {
	IsOffloaded      bool
	FastPathCount    uint64
	SlowPathCount    uint64
	RemainingPackets uint64
}

var flowMap []*Flow
var packetQueueAmount uint64 = 0
var packetQueue = make([][]int, PPS)

func GenerateBatchPacket(flowIndex int) {
	for i := 0; i < BATCH_SIZE; i++ {
		packetQueue[packetQueueAmount][i] = 0
	}
	for i := 0; i < Min(BATCH_SIZE, flowMap[flowIndex].RemainingPackets); i++ {
		packetQueue[packetQueueAmount][i] = flowIndex
	}
}

func PacketGenerator() {
	// Generate new flows.
	for j := 0; j < CPS; j++ {
		seed := float64(flowCount % 100)
		/* The proportion of packets.
		packet amount	proportion
		512				0.1 * 0.2 = 0.02
		256				0.15 * 0.2 = 0.03
		128				0.2 * 0.2 = 0.04
		64				0.25 * 0.2 = 0.05
		32				0.3 * 0.2 = 0.06
		16				0.1 * 0.8 = 0.08
		12				0.2 * 0.8 = 0.16
		8				0.3 * 0.8 = 0.24
		4				0.4 * 0.8 = 0.32 */
		if seed < 0.4*(100-ELEPHANT_FLOW_PROPORTION) {
			flowMap = append(flowMap, &Flow{false, 0, 0, 2})
		} else if seed < (0.4+0.3)*(100-ELEPHANT_FLOW_PROPORTION) {
			flowMap = append(flowMap, &Flow{false, 0, 0, 4})
		} else if seed < (0.7+0.2)*(100-ELEPHANT_FLOW_PROPORTION) {
			flowMap = append(flowMap, &Flow{false, 0, 0, 8})
		} else if seed < (0.9+0.1)*(100-ELEPHANT_FLOW_PROPORTION) {
			flowMap = append(flowMap, &Flow{false, 0, 0, 16})
		} else if seed < 0.3*ELEPHANT_FLOW_PROPORTION+(100-ELEPHANT_FLOW_PROPORTION) {
			flowMap = append(flowMap, &Flow{false, 0, 0, 32})
		} else if seed < (0.3+0.25)*ELEPHANT_FLOW_PROPORTION+(100-ELEPHANT_FLOW_PROPORTION) {
			flowMap = append(flowMap, &Flow{false, 0, 0, 64})
		} else if seed < (0.55+0.2)*ELEPHANT_FLOW_PROPORTION+(100-ELEPHANT_FLOW_PROPORTION) {
			flowMap = append(flowMap, &Flow{false, 0, 0, 128})
		} else if seed < (0.75+0.15)*ELEPHANT_FLOW_PROPORTION+(100-ELEPHANT_FLOW_PROPORTION) {
			flowMap = append(flowMap, &Flow{false, 0, 0, 256})
		} else {
			flowMap = append(flowMap, &Flow{false, 0, 0, 512})
		}

		GenerateBatchPacket(int(flowCount))
		packetQueueAmount++
		flowCount++
	}

	// Generate packets of existing flows.
	var flowIndex uint64 = 1
	for ; packetQueueAmount < PPS; packetQueueAmount++ {
		if flowIndex >= flowCount-CPS {
			break
		} else {
			GenerateBatchPacket(int(flowIndex))
		}
	}
}

func main() {
	// Initialize the packet queue for each turn.
	for i := 0; i < PPS; i++ {
		packetQueue[i] = make([]int, BATCH_SIZE)
	}

	// FlowMap is used to store the information of flows and the index start from 1
	flowMap = append(flowMap, &Flow{false, 0, 0, 0})
	flowCount++

	//for i := 0; i < TURNS; i++ {
	// Generate new flows.
	packetQueueAmount = 0
	PacketGenerator()
	fmt.Println(packetQueue)
	//}
}

func Min(v1 int, v2 uint64) int {
	v2Int := int(v2)
	if v1 <= v2Int {
		return v1
	} else {
		return v2Int
	}
}
