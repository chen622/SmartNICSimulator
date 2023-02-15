package main

import (
	"fmt"
	"math"
	"sync"
)

const (
	CPS                      = 1400 * 1000      // New connections per second.
	PPS                      = 20 * 1000 * 1000 // Packets per second.
	RAT_BATCH_SIZE           = 4                // Packets per batch for rat flow.
	ELEPHANT_BATCH_SIZE      = 64               // Packets per batch for elephant flow.
	MAX_OFFLOAD_SPEED        = 240 * 1000       // Maximum rule insertion speed to the SmartNIC.
	MAX_SLOW_PATH_SPEED      = 20 * 1000 * 1000 // Maximum packet processing speed of the slow path.
	SLOW_PATH_LATNECY_US     = 80               // The average latency of the slow path.
	FAST_PATH_LATENCY_US     = 10               // The average latency of the fast path.
	TURNS                    = 50               // Each turn represents 1 second.
	ELEPHANT_FLOW_PROPORTION = 20               // The proportion of elephant flows.
	THRESHOLD_ADJUST_METHOD  = 0                // 0: No adjust. 1: Adjust threshold by overOffloadCount. 2: Adjust threshold by offloadCount & overOffloadCount & dropCount.
	ALPHA_PARAM              = 0.9              // The alpha parameter for adjust threshold.
	OMEGA_PARAM_1            = 1                // The first omega parameter for adjust threshold.
)

type Flow struct {
	IsOffloaded      bool
	FastPathCount    uint64
	SlowPathCount    uint64
	RemainingPackets uint16
	StartTurn        int8
	FinishTurn       int8
}

// SS
// @Description: The control block of each goroutine in this simulator.
type SS struct {
	OffloadThreshold int     // The threshold of packet amount to offload a flow.
	FlowMap          []*Flow // The flow map to save information of each flow.

	Turn                 int8    // The current turn.
	PacketQueue          [][]int // The packets for this turn.
	PacketQueueAmount    uint64  // The amount of flow for this turn.
	PacketAmount         uint64  // The amount of packets for this turn.
	FlowCount            uint64  // The index to generate new flow.
	FastPathCount        uint64  // The quantity of packets in fast path in this turn.
	SlowPathCount        uint64  // The quantity of packets in slow path in this turn.
	DropCount            uint64  // The quantity of dropped packets in this turn.
	OffloadRuleCount     uint64  // The quantity of offloaded flows in this turn.
	OverOffloadRuleCount uint64  // The quantity of flows that are over offload speed in this turn.

	TotalPacketAmount  uint64 // The total amount of packets.
	TotalDropCount     uint64 // The total amount of dropped packets.
	TotalSlowPathCount uint64 // The total amount of packets in slow path.
	TotalFastPathCount uint64 // The total amount of packets in fast path.
}

// GenerateBatchPacket
//
//	@Description: Generate batch packets of 'flowIndex' flow.
//	@param ss The context of the simulator.
//	@param flowIndex The index of the flow.
//	@param isElephant Whether the flow is an elephant flow.
//	@param isNew Whether the flow is a new flow.
//	@return The amount of packets generated.
func GenerateBatchPacket(ss *SS, flowIndex int, isElephant bool, isNew bool) int {
	batchSize := RAT_BATCH_SIZE
	if isElephant {
		if isNew {
			if ss.OffloadThreshold > 0 {
				batchSize = ss.OffloadThreshold
			} else {
				batchSize = RAT_BATCH_SIZE
			}
		} else {
			batchSize = ELEPHANT_BATCH_SIZE
		}
	}
	//for i := 0; i < batchSize; i++ {
	//	packetQueue[packetQueueAmount][i] = 0
	//}
	ss.PacketQueue[ss.PacketQueueAmount] = []int{}

	count := 0
	for i := 0; i < Min(batchSize, ss.FlowMap[flowIndex].RemainingPackets); i++ {
		ss.PacketQueue[ss.PacketQueueAmount] = append(ss.PacketQueue[ss.PacketQueueAmount], flowIndex)
		//packetQueue[packetQueueAmount][i] = flowIndex
		count++
	}
	ss.FlowMap[flowIndex].RemainingPackets -= uint16(count)
	return count
}

// PacketGenerator
//
//	@Description: Generate packets for this turn.
//	@param ss The context of the simulator.
func PacketGenerator(ss *SS) {
	// Generate new flows.
	for j := 0; j < CPS; j++ {
		seed := float64((ss.FlowCount - 1) % 100)
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
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, 2, ss.Turn, -1})
		} else if seed < (0.4+0.3)*(100-ELEPHANT_FLOW_PROPORTION) {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, 4, ss.Turn, -1})
		} else if seed < (0.7+0.2)*(100-ELEPHANT_FLOW_PROPORTION) {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, 8, ss.Turn, -1})
		} else if seed < (0.9+0.1)*(100-ELEPHANT_FLOW_PROPORTION) {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, 16, ss.Turn, -1})
		} else if seed < 0.3*ELEPHANT_FLOW_PROPORTION+(100-ELEPHANT_FLOW_PROPORTION) {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, 32, ss.Turn, -1})
		} else if seed < (0.3+0.25)*ELEPHANT_FLOW_PROPORTION+(100-ELEPHANT_FLOW_PROPORTION) {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, 64, ss.Turn, -1})
		} else if seed < (0.55+0.2)*ELEPHANT_FLOW_PROPORTION+(100-ELEPHANT_FLOW_PROPORTION) {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, 128, ss.Turn, -1})
		} else if seed < (0.75+0.15)*ELEPHANT_FLOW_PROPORTION+(100-ELEPHANT_FLOW_PROPORTION) {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, 256, ss.Turn, -1})
		} else {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, 512, ss.Turn, -1})
		}

		ss.PacketAmount += uint64(GenerateBatchPacket(ss, int(ss.FlowCount), ss.FlowMap[ss.FlowCount].RemainingPackets >= 32, true))
		ss.PacketQueueAmount++
		ss.FlowCount++
	}

	// Generate packets of existing flows.
	var flowId uint64 = 1
	for ss.PacketAmount+ELEPHANT_BATCH_SIZE <= PPS && flowId < ss.FlowCount-CPS {
		if ss.FlowMap[flowId].RemainingPackets > 0 {
			ss.PacketAmount += uint64(GenerateBatchPacket(ss, int(flowId), ss.FlowMap[flowId].RemainingPackets >= 32, false))
			ss.PacketQueueAmount++
		}
		flowId++
	}
}

// PacketProcessor
//
//	@Description: Process packets in this turn.
//	@param ss The context of the simulator.
func PacketProcessor(ss *SS) {
	var flowIndex uint64 = 0
	for ; flowIndex < ss.PacketQueueAmount; flowIndex++ {
		flowId := ss.PacketQueue[flowIndex][0]
		if flowId <= 0 {
			panic("flowId <= 0 when processing packets")
		}
		// Count the amount of packets in this batch.
		count := len(ss.PacketQueue[flowIndex])
		//for j := 0; j < ELEPHANT_BATCH_SIZE; j++ {
		//	if packetQueue[flowIndex][j] == flowId {
		//		count++
		//	} else {
		//		break
		//	}
		//}

		flow := ss.FlowMap[flowId]
		if flow.IsOffloaded { // This flow has been offloaded info fast path.
			flow.FastPathCount += uint64(count)
			ss.FastPathCount += uint64(count)
			if flow.RemainingPackets <= 0 && flow.FinishTurn == -1 {
				flow.FinishTurn = ss.Turn
			}
		} else { // This flow should be processed by the slow path.
			if ss.SlowPathCount >= MAX_SLOW_PATH_SPEED { // The slow path is full.
				ss.DropCount += uint64(count)
				ss.FlowMap[flowId].RemainingPackets += uint16(count)
				continue
			} else if ss.SlowPathCount+uint64(count) > MAX_SLOW_PATH_SPEED { // The slow path is nearly full.
				ss.DropCount += uint64(count) - (MAX_SLOW_PATH_SPEED - ss.SlowPathCount)
				ss.FlowMap[flowId].RemainingPackets += uint16(uint64(count) - (MAX_SLOW_PATH_SPEED - ss.SlowPathCount))
				count = int(MAX_SLOW_PATH_SPEED - ss.SlowPathCount)
			}

			flow.SlowPathCount += uint64(count)
			ss.SlowPathCount += uint64(count)

			if flow.SlowPathCount >= uint64(ss.OffloadThreshold) && ss.OffloadThreshold != -1 { // If the slow path packet count of this flow exceeds the threshold, offload it to the fast path.
				if ss.OffloadRuleCount < MAX_OFFLOAD_SPEED {
					ss.OffloadRuleCount++
					flow.IsOffloaded = true
				} else {
					ss.OverOffloadRuleCount++
					//fmt.Printf("offload speed exceeds the limit\n")
				}
			}
		}
		if flow.RemainingPackets <= 0 && flow.FinishTurn == -1 {
			flow.FinishTurn = ss.Turn
		}
	}
}

// AdjustThreshold
//
//	@Description: Adjust the threshold to offloading.
//	@param ss The context of the simulator.
func AdjustThreshold(ss *SS) {
	if THRESHOLD_ADJUST_METHOD == 0 {
		return
	} else if THRESHOLD_ADJUST_METHOD == 1 {
		if ss.OverOffloadRuleCount > 0 {
			ss.OffloadThreshold *= 2
		} else if ss.OffloadRuleCount == 0 {
			ss.OffloadThreshold /= 2
		}
	} else if THRESHOLD_ADJUST_METHOD == 2 {
		//fmt.Printf("fr: %f rqc:%f pqc: %f\n", float64(ss.OffloadRuleCount)/MAX_OFFLOAD_SPEED, float64(MAX_OFFLOAD_SPEED+ss.OverOffloadRuleCount)/MAX_OFFLOAD_SPEED,
		//	float64(ss.DropCount)/(float64(MAX_SLOW_PATH_SPEED)*0.5))
		//ss.OffloadThreshold = int(float64(ss.OffloadThreshold) *
		//	math.Pow(2.0, float64(ALPHA_PARAM)*
		//		(float64(ss.OffloadRuleCount*(MAX_OFFLOAD_SPEED+ss.OverOffloadRuleCount))/math.Pow(float64(MAX_OFFLOAD_SPEED), 2.0)-OMEGA_PARAM_1)-
		//		float64(ss.DropCount)*float64(ss.DropCount-OMEGA_PARAM_2*MAX_SLOW_PATH_SPEED)/math.Pow(float64(MAX_SLOW_PATH_SPEED), 2.0)))
		ss.OffloadThreshold = int(float64(ss.OffloadThreshold) *
			math.Pow(2.0, float64(ALPHA_PARAM)*
				(float64(ss.OffloadRuleCount*(MAX_OFFLOAD_SPEED+ss.OverOffloadRuleCount))/math.Pow(float64(MAX_OFFLOAD_SPEED), 2.0)-OMEGA_PARAM_1)-
				float64(ss.DropCount)/(float64(MAX_SLOW_PATH_SPEED)*0.5)))
	} else {
		panic("invalid threshold adjust method")
	}
}

func main() {
	fmt.Printf("CPS: %d, PPS: %d, TURNS: %d, ELEPHANT_BATCH_SIZE: %d, ELEPHANT_FLOW_PROPORTION: %d, MAX_OFFLOAD_SPEED: %d, ADJUST_METHOD: %d, ALPHA: %f, MAX_SLOW_PATH_SPEED: %d\n", CPS, PPS, TURNS, ELEPHANT_BATCH_SIZE, ELEPHANT_FLOW_PROPORTION, MAX_OFFLOAD_SPEED, THRESHOLD_ADJUST_METHOD, ALPHA_PARAM, MAX_SLOW_PATH_SPEED)
	// Initialize the packet queue for each turn.
	//for i := 0; i < PPS; i++ {
	//	packetQueue[i] = make([]int, ELEPHANT_BATCH_SIZE)
	//}

	var threshold []int
	if THRESHOLD_ADJUST_METHOD == 0 {
		threshold = []int{-1, 2, 4, 8, 16, 32, 64}
	} else {
		threshold = []int{4}
	}
	ssList := make([]*SS, len(threshold))
	var wg sync.WaitGroup
	for i := 0; i < len(threshold); i++ {
		ssList[i] = &SS{
			OffloadThreshold: threshold[i],
			FlowMap:          []*Flow{},
			PacketQueue:      make([][]int, PPS),
		}
		wg.Add(1)
		go func(ss *SS) {
			defer wg.Done()
			// FlowMap is used to store the information of flows and the index start from 1
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, 0, 0, 0})
			ss.FlowCount = 1

			for ss.Turn = 0; ss.Turn < TURNS; ss.Turn++ {
				// Generate new flows.
				ss.PacketQueueAmount = 0
				ss.PacketAmount = 0
				PacketGenerator(ss)

				//var i uint64 = 0
				//for ; i < packetQueueAmount; i++ {
				//	fmt.Printf("%d\n", packetQueue[i])
				//}

				ss.SlowPathCount, ss.DropCount, ss.FastPathCount, ss.OffloadRuleCount, ss.OverOffloadRuleCount = 0, 0, 0, 0, 0
				PacketProcessor(ss)
				if THRESHOLD_ADJUST_METHOD != 0 {
					fmt.Printf("turn: %-4d offloadThreshold: %-4d packetCount: %-8d slowPathCount: %-8d dropCount: %-8d fastPathCount: %-8d offloadCount: %-6d overOffloadCount: %-6d\n",
						ss.Turn, ss.OffloadThreshold, ss.PacketAmount, ss.SlowPathCount, ss.DropCount, ss.FastPathCount, ss.OffloadRuleCount, ss.OverOffloadRuleCount)
				}
				AdjustThreshold(ss)
				ss.TotalPacketAmount += ss.PacketAmount
				ss.TotalDropCount += ss.DropCount
				ss.TotalSlowPathCount += ss.SlowPathCount
				ss.TotalFastPathCount += ss.FastPathCount
			}

		}(ssList[i])
	}
	wg.Wait()
	for i := 0; i < len(threshold); i++ {
		flowFinishTime := 0
		flowFinishCount := 0
		for j := 1; j < len(ssList[i].FlowMap); j++ {
			if ssList[i].FlowMap[j].FinishTurn == -1 {
				continue
			} else if ssList[i].FlowMap[j].FinishTurn == ssList[i].FlowMap[j].StartTurn {
				flowFinishTime += RAT_BATCH_SIZE * SLOW_PATH_LATNECY_US
			} else {
				flowFinishTime += int(ssList[i].FlowMap[j].FinishTurn-ssList[i].FlowMap[j].StartTurn) * 1000 * 1000
			}
			flowFinishCount++
		}
		fmt.Printf("threshold: %d, drop rate: %f%%, latency: %f us, flow finish rate: %f, average flow finish time: %f s\n",
			ssList[i].OffloadThreshold, float64(ssList[i].TotalDropCount)/float64(ssList[i].TotalPacketAmount)*100,
			(float64(ssList[i].TotalFastPathCount)*FAST_PATH_LATENCY_US+float64(ssList[i].TotalSlowPathCount+ssList[i].TotalDropCount)*SLOW_PATH_LATNECY_US)/float64(ssList[i].TotalSlowPathCount+ssList[i].TotalFastPathCount),
			float64(flowFinishCount)/float64(len(ssList[i].FlowMap)), float64(flowFinishTime)/float64(flowFinishCount)/1000/1000)
	}
}

func Min(v1 int, v2 uint16) int {
	v2Int := int(v2)
	if v1 <= v2Int {
		return v1
	} else {
		return v2Int
	}
}
