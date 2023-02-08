package main

import "fmt"

const (
	CPS                       = 750 * 1000       // New connections per second.
	PPS                       = 25 * 1000 * 1000 // Packets per second.
	RAT_BATCH_SIZE            = 4                // Packets per batch for rat flow.
	ELEPHANT_START_BATCH_SIZE = 16
	ELEPHANT_BATCH_SIZE       = 64 // Packets per batch for elephant flow.
	MAX_OFFLOAD_SPEED         = 200 * 1000
	MAX_SLOW_PATH_SPEED       = 8 * 1000 * 1000
	TURNS                     = 50 // Each turn represents 1 second.
	ELEPHANT_FLOW_PROPORTION  = 20 // The proportion of elephant flows.
)

var offloadThreshold int = 16 // The threshold of packet amount to offload a flow.

type Flow struct {
	IsOffloaded      bool
	FastPathCount    uint64
	SlowPathCount    uint64
	RemainingPackets uint64
}

var flowMap []*Flow                  // The flow map to save information of each flow.
var packetQueue = make([][]int, PPS) // The packets for this turn.
var packetQueueAmount uint64 = 0     // The amount of flow for this turn.
var packetAmount uint64 = 0          // The amount of packets for this turn.

var flowCount uint64 = 0            // The index to generate new flow.
var slowPathCount uint64 = 0        // The quantity of packets in slow path in this turn.
var dropCount uint64 = 0            // The quantity of dropped packets in this turn.
var fastPathCount uint64 = 0        // The quantity of packets in fast path in this turn.
var offloadRuleCount uint64 = 0     // The quantity of offloaded flows in this turn.
var overOffloadRuleCount uint64 = 0 // The quantity of flows that are over offload speed in this turn.

var totalPacketAmount uint64 = 0  // The total amount of packets.
var totalDropCount uint64 = 0     // The total amount of dropped packets.
var totalSlowPathCount uint64 = 0 // The total amount of packets in slow path.
var totalFastPathCount uint64 = 0 // The total amount of packets in fast path.

// GenerateBatchPacket
//
//	@Description: Generate batch packets of 'flowIndex' flow.
//	@param flowIndex the index of the flow.
//	@return the amount of packets generated.
func GenerateBatchPacket(flowIndex int, isElephant bool, isNew bool) int {
	batchSize := RAT_BATCH_SIZE
	if isElephant {
		if isNew {
			batchSize = offloadThreshold
		} else {
			batchSize = ELEPHANT_BATCH_SIZE
		}
	}
	//for i := 0; i < batchSize; i++ {
	//	packetQueue[packetQueueAmount][i] = 0
	//}
	packetQueue[packetQueueAmount] = []int{}

	count := 0
	for i := 0; i < Min(batchSize, flowMap[flowIndex].RemainingPackets); i++ {
		packetQueue[packetQueueAmount] = append(packetQueue[packetQueueAmount], flowIndex)
		//packetQueue[packetQueueAmount][i] = flowIndex
		count++
	}
	flowMap[flowIndex].RemainingPackets -= uint64(count)
	return count
}

// PacketGenerator
//
//	@Description: Generate packets for this turn.
func PacketGenerator() {
	// Generate new flows.
	for j := 0; j < CPS; j++ {
		seed := float64((flowCount - 1) % 100)
		// The proportion of packets.
		// packet amount	proportion
		// 512				0.1 * 0.2 = 0.02
		// 256				0.15 * 0.2 = 0.03
		// 128				0.2 * 0.2 = 0.04
		// 64				0.25 * 0.2 = 0.05
		// 32				0.3 * 0.2 = 0.06
		// 16				0.1 * 0.8 = 0.08
		// 12				0.2 * 0.8 = 0.16
		// 8				0.3 * 0.8 = 0.24
		// 4				0.4 * 0.8 = 0.32
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

		packetAmount += uint64(GenerateBatchPacket(int(flowCount), flowMap[flowCount].RemainingPackets >= 32, true))
		packetQueueAmount++
		flowCount++
	}

	// Generate packets of existing flows.
	var flowId uint64 = 1
	for packetAmount < PPS && flowId < flowCount-CPS {
		if flowMap[flowId].RemainingPackets > 0 {
			packetAmount += uint64(GenerateBatchPacket(int(flowId), flowMap[flowId].RemainingPackets >= 32, false))
			packetQueueAmount++
		}
		flowId++
	}
}

// PacketProcessor
//
//	@Description: Process packets in this turn.
func PacketProcessor() {
	var flowIndex uint64 = 0
	for ; flowIndex < packetQueueAmount; flowIndex++ {
		flowId := packetQueue[flowIndex][0]
		if flowId <= 0 {
			panic("flowId <= 0 when processing packets")
		}
		// Count the amount of packets in this batch.
		count := len(packetQueue[flowIndex])
		//for j := 0; j < ELEPHANT_BATCH_SIZE; j++ {
		//	if packetQueue[flowIndex][j] == flowId {
		//		count++
		//	} else {
		//		break
		//	}
		//}

		flow := flowMap[flowId]
		if flow.IsOffloaded { // This flow has been offloaded info fast path.
			flow.FastPathCount += uint64(count)
			fastPathCount += uint64(count)
		} else { // This flow should be processed by the slow path.
			if slowPathCount >= MAX_SLOW_PATH_SPEED {
				dropCount += uint64(count)
				continue
			} else if slowPathCount+uint64(count) > MAX_SLOW_PATH_SPEED {
				dropCount += uint64(count) - (MAX_SLOW_PATH_SPEED - slowPathCount)
				count = int(MAX_SLOW_PATH_SPEED - slowPathCount)
			}

			flow.SlowPathCount += uint64(count)
			slowPathCount += uint64(count)

			if flow.SlowPathCount >= uint64(offloadThreshold) { // If the slow path packet count of this flow exceeds the threshold, offload it to the fast path.
				if offloadRuleCount < MAX_OFFLOAD_SPEED {
					offloadRuleCount++
					flow.IsOffloaded = true
				} else {
					overOffloadRuleCount++
					//fmt.Printf("offload speed exceeds the limit\n")
				}
			}
		}
	}
}

func main() {
	fmt.Printf("CPS: %d, PPS: %d, TURNS: %d, ELEPHANT_START_BATCH_SIZE: %d, ELEPHANT_BATCH_SIZE: %d, ELEPHANT_FLOW_PROPORTION: %d, MAX_OFFLOAD_SPEED: %d, MAX_SLOW_PATH_SPEED: %d\n", CPS, PPS, TURNS, ELEPHANT_START_BATCH_SIZE, ELEPHANT_BATCH_SIZE, ELEPHANT_FLOW_PROPORTION, MAX_OFFLOAD_SPEED, MAX_SLOW_PATH_SPEED)
	// Initialize the packet queue for each turn.
	//for i := 0; i < PPS; i++ {
	//	packetQueue[i] = make([]int, ELEPHANT_BATCH_SIZE)
	//}

	threshold := []int{2, 4, 8, 16, 32, 64}
	for i := 0; i < len(threshold); i++ {
		offloadThreshold = threshold[i]

		// FlowMap is used to store the information of flows and the index start from 1
		flowMap = []*Flow{}
		flowMap = append(flowMap, &Flow{false, 0, 0, 0})
		flowCount = 1
		totalFastPathCount, totalDropCount, totalSlowPathCount, totalPacketAmount = 0, 0, 0, 0

		for turn := 0; turn < TURNS; turn++ {
			// Generate new flows.
			packetQueueAmount = 0
			packetAmount = 0
			PacketGenerator()

			//var i uint64 = 0
			//for ; i < packetQueueAmount; i++ {
			//	fmt.Printf("%d\n", packetQueue[i])
			//}

			slowPathCount, dropCount, fastPathCount, offloadRuleCount, overOffloadRuleCount = 0, 0, 0, 0, 0
			PacketProcessor()
			//fmt.Printf("turn: %-4d packetCount: %-8d slowPathCount: %-8d dropCount: %-6d fastPathCount: %-8d offloadCount: %-6d overOffloadCount: %-6d\n", turn, packetAmount, slowPathCount, dropCount, fastPathCount, offloadRuleCount, overOffloadRuleCount)
			totalPacketAmount += packetAmount
			totalDropCount += dropCount
			totalSlowPathCount += slowPathCount
			totalFastPathCount += fastPathCount
		}
		fmt.Printf("threshold: %d, drop rate: %f, latency: %f\n", offloadThreshold, float64(totalDropCount)/float64(totalPacketAmount), (float64(totalSlowPathCount)*10+float64(totalPacketAmount)*80)/float64(totalSlowPathCount+totalFastPathCount))
	}

}

func Min(v1 int, v2 uint64) int {
	v2Int := int(v2)
	if v1 <= v2Int {
		return v1
	} else {
		return v2Int
	}
}
