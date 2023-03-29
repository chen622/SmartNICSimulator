package main

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"sync"
)

const (
	CPS                         = 1200 * 1000      // New connections per second.
	PPS                         = 36 * 1000 * 1000 // Packets per second.
	RAT_BATCH_SIZE              = 4                // Packets per batch for rat flow.
	ELEPHANT_BATCH_SIZE         = 32               // Packets per batch for elephant flow.
	MAX_OFFLOAD_SPEED           = 220 * 1000       // Maximum rule insertion speed to the SmartNIC.
	MAX_SLOW_PATH_SPEED         = 19 * 1000 * 1000 // Maximum packet processing speed of the slow path.
	SLOW_PATH_LATNECY_US        = 50               // The average latency of the slow path.
	FAST_PATH_LATENCY_US        = 10               // The average latency of the fast path.
	TURNS                       = 100              // Each turn represents 1 second.
	ELEPHANT_FLOW_PROPORTION    = 20               // The proportion of elephant flows.
	THRESHOLD_ADJUST_METHOD     = 3                // 0: No adjust. 1: Adjust threshold by overOffloadCount. 2: Adjust threshold by offloadCount & overOffloadCount & dropCount. 3: elixir.
	ALPHA_PARAM                 = 1                // The alpha parameter for adjust threshold.
	OMEGA_PARAM_1               = 1.5              // The first omega parameter for adjust threshold.
	ELIXIR_HASH_FUNCTIONS       = 8
	ELIXIR_HASH_SLOTS           = CPS
	ELIXIR_IDENTIFY_WINDOW_SIZE = 3
	ELIXIR_REPLACE_WINDOW_SIZE  = 3
	ELIXIR_MINHEAP_SIZE         = MAX_OFFLOAD_SPEED * ELIXIR_REPLACE_WINDOW_SIZE
	ELIXIR_HASH_P               = math.MaxInt32
	ELIXIR_MAX_HASH             = ELIXIR_HASH_P - 1
)

// MAGNIFICATION_OF_EACH_STAGE Make CPS fluctuate.
var MAGNIFICATION_OF_EACH_STAGE = []float64{0.5, 0.75, 1, 1, 1, 0.75, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25}

//var MAGNIFICATION_OF_EACH_STAGE = []float64{1}

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
	AlphaParameter   float64
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
	OffloadRuleQueue     []int   // The queue of offloaded flows.
	OverOffloadRuleCount uint64  // The quantity of flows that are over offload speed in this turn.

	ElixirArray          [][][]uint64         // The array to save information for elixir.
	ElixirTime           int                  // The index of time slot in the array.
	ElixirHashParameters []struct{ a, b int } // The parameters for hash function.
	ElixirMinHeap        PriorityQueue        // The min-heap to help find the top K flows.
	ElixirMap            map[int]*Item        // The map to look up the item in the min-heap.

	TotalPacketAmount  uint64 // The total amount of packets.
	TotalDropCount     uint64 // The total amount of dropped packets.
	TotalSlowPathCount uint64 // The total amount of packets in slow path.
	TotalFastPathCount uint64 // The total amount of packets in fast path.
}

// SketchCounter
//
//	@Description: Update the counter of sketch and sort flows with min-heap.
//	@param ss The control block of each goroutine in this simulator.
//	@param flowIndex The index of the flow.
//	@param value The value to add to the counter.
func SketchCounter(ss *SS, flowIndex int, value uint64) {
	for i := 0; i < ELIXIR_HASH_FUNCTIONS; i++ {
		hash := ss.ElixirHashParameters[i].a*flowIndex + ss.ElixirHashParameters[i].b
		hash %= ELIXIR_HASH_P
		hash %= ELIXIR_HASH_SLOTS
		ss.ElixirArray[i][hash][ss.ElixirTime] += value
	}
	var flowSize uint64 = math.MaxUint64
	for i := 0; i < ELIXIR_HASH_FUNCTIONS; i++ {
		hash := ss.ElixirHashParameters[i].a*flowIndex + ss.ElixirHashParameters[i].b
		hash %= ELIXIR_HASH_P
		hash %= ELIXIR_HASH_SLOTS
		var count uint64 = 0
		for j := 0; j < ELIXIR_IDENTIFY_WINDOW_SIZE/ELIXIR_REPLACE_WINDOW_SIZE; j++ {
			count += ss.ElixirArray[i][hash][j]
		}
		if count < flowSize {
			flowSize = count
		}
	}
	item := ss.ElixirMap[flowIndex]
	if item == nil {
		item = &Item{
			value:    flowIndex,
			priority: flowSize,
		}
		if len(ss.ElixirMinHeap) < ELIXIR_MINHEAP_SIZE {
			heap.Push(&ss.ElixirMinHeap, item)
			ss.ElixirMap[flowIndex] = item
		} else if flowSize > ss.ElixirMinHeap[0].priority {
			oldItem := heap.Pop(&ss.ElixirMinHeap)
			delete(ss.ElixirMap, oldItem.(*Item).value)
			heap.Push(&ss.ElixirMinHeap, item)
			ss.ElixirMap[flowIndex] = item
		}
	} else {
		ss.ElixirMinHeap.Update(item, flowIndex, flowSize)
	}
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
	if isElephant && !isNew {
		batchSize = ELEPHANT_BATCH_SIZE
	}
	//if isElephant {
	//	if isNew {
	//		if ss.OffloadThreshold > 0 {
	//			batchSize = ss.OffloadThreshold
	//		} else {
	//			batchSize = RAT_BATCH_SIZE
	//		}
	//	} else {
	//		batchSize = ELEPHANT_BATCH_SIZE
	//	}
	//}
	ss.PacketQueue[ss.PacketQueueAmount] = []int{}

	count := 0
	for i := 0; i < Min(batchSize, ss.FlowMap[flowIndex].RemainingPackets); i++ {
		ss.PacketQueue[ss.PacketQueueAmount] = append(ss.PacketQueue[ss.PacketQueueAmount], flowIndex)
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

	ratio := MAGNIFICATION_OF_EACH_STAGE[int(ss.Turn)/int(math.Ceil(float64(TURNS)/float64(len(MAGNIFICATION_OF_EACH_STAGE))))]
	//cpsTarget := int(CPS * )
	cpsTarget := int(CPS * ratio)

	// Slow start.
	cps := 400000
	cps = cps * int(ss.Turn+1)
	if cps > cpsTarget {
		cps = cpsTarget
	}
	//fmt.Printf("%d ", cps)
	for j := 0; j < cps; j++ {
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
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, uint16(4), ss.Turn, -1})
		} else if seed < (0.4+0.3)*(100-ELEPHANT_FLOW_PROPORTION) {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, uint16(8), ss.Turn, -1})
		} else if seed < (0.7+0.2)*(100-ELEPHANT_FLOW_PROPORTION) {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, uint16(12), ss.Turn, -1})
		} else if seed < (0.9+0.1)*(100-ELEPHANT_FLOW_PROPORTION) {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, uint16(16), ss.Turn, -1})
		} else if seed < 0.3*ELEPHANT_FLOW_PROPORTION+(100-ELEPHANT_FLOW_PROPORTION) {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, uint16(32), ss.Turn, -1})
		} else if seed < (0.3+0.25)*ELEPHANT_FLOW_PROPORTION+(100-ELEPHANT_FLOW_PROPORTION) {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, uint16(64), ss.Turn, -1})
		} else if seed < (0.55+0.2)*ELEPHANT_FLOW_PROPORTION+(100-ELEPHANT_FLOW_PROPORTION) {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, uint16(128), ss.Turn, -1})
		} else if seed < (0.75+0.15)*ELEPHANT_FLOW_PROPORTION+(100-ELEPHANT_FLOW_PROPORTION) {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, uint16(256), ss.Turn, -1})
		} else {
			ss.FlowMap = append(ss.FlowMap, &Flow{false, 0, 0, uint16(512), ss.Turn, -1})
		}

		ss.PacketAmount += uint64(GenerateBatchPacket(ss, int(ss.FlowCount), ss.FlowMap[ss.FlowCount].RemainingPackets >= 32, true))
		ss.PacketQueueAmount++
		ss.FlowCount++
	}

	// Generate packets of existing flows.
	var flowId uint64 = 1
	for ss.PacketAmount+ELEPHANT_BATCH_SIZE <= uint64(PPS) && flowId < ss.FlowCount-uint64(cps) {
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
		flow := ss.FlowMap[flowId]

		for i := 0; i < count; i++ {
			if flow.IsOffloaded { // This flow has been offloaded info fast path.
				flow.FastPathCount += uint64(count - i)
				ss.FastPathCount += uint64(count - i)
				if THRESHOLD_ADJUST_METHOD == 3 {
					SketchCounter(ss, flowId, uint64(count-i))
				}
				break
			} else { // This flow should be processed by the slow path.
				if ss.SlowPathCount >= MAX_SLOW_PATH_SPEED { // The slow path is full.
					ss.DropCount += uint64(count - i)
					ss.FlowMap[flowId].RemainingPackets += uint16(count - i)
					break
				} else {
					flow.SlowPathCount++
					ss.SlowPathCount++
					if THRESHOLD_ADJUST_METHOD == 3 {
						SketchCounter(ss, flowId, 1)
					}
					// If the slow path packet count of this flow exceeds the threshold, offload it to the fast path.
					if flow.SlowPathCount >= uint64(ss.OffloadThreshold) && ss.OffloadThreshold != -1 {
						if ss.OffloadRuleCount < MAX_OFFLOAD_SPEED {
							ss.OffloadRuleCount++
							flow.IsOffloaded = true
						} else if ss.OverOffloadRuleCount < MAX_OFFLOAD_SPEED {
							ss.OverOffloadRuleCount++
						}
					}
				}
			}
		}
		if flow.RemainingPackets <= 0 && flow.FinishTurn == -1 {
			flow.FinishTurn = ss.Turn
		}
	}
}

// PostProcess
//
//	@Description: The operation after packet processing.
//	@param ss The context of the simulator.
func PostProcess(ss *SS) {
	if THRESHOLD_ADJUST_METHOD == 0 {
		return
	} else if THRESHOLD_ADJUST_METHOD == 1 {
		if ss.OverOffloadRuleCount > 0 {
			ss.OffloadThreshold *= 2
		} else if ss.OffloadRuleCount == 0 {
			ss.OffloadThreshold /= 2
		}
	} else if THRESHOLD_ADJUST_METHOD == 2 {
		//fmt.Printf("rc: %f rqc:%f dc: %f\n", float64(ss.OffloadRuleCount)/MAX_OFFLOAD_SPEED, float64(MAX_OFFLOAD_SPEED+ss.OverOffloadRuleCount)/MAX_OFFLOAD_SPEED,
		//	float64(ss.DropCount)/float64(MAX_SLOW_PATH_SPEED))
		ss.OffloadThreshold = int(float64(ss.OffloadThreshold) *
			math.Pow(2.0, ss.AlphaParameter*
				(float64(ss.OffloadRuleCount*(MAX_OFFLOAD_SPEED+ss.OverOffloadRuleCount))/math.Pow(float64(MAX_OFFLOAD_SPEED), 2.0)-OMEGA_PARAM_1)-
				float64(ss.DropCount)/float64(MAX_SLOW_PATH_SPEED)))
		if ss.OffloadThreshold < 2 {
			ss.OffloadThreshold = 2
		}
		//fmt.Printf("%f %f %f\n", math.Pow(2.0, ss.AlphaParameter*
		//	(float64(ss.OffloadRuleCount*(MAX_OFFLOAD_SPEED+ss.OverOffloadRuleCount))/math.Pow(float64(MAX_OFFLOAD_SPEED), 2.0)-OMEGA_PARAM_1)-
		//	float64(ss.DropCount)/(float64(MAX_SLOW_PATH_SPEED)*0.5)),
		//	float64(ss.OffloadRuleCount*(MAX_OFFLOAD_SPEED+ss.OverOffloadRuleCount))/math.Pow(float64(MAX_OFFLOAD_SPEED), 2.0)-OMEGA_PARAM_1,
		//	float64(ss.DropCount)/(float64(MAX_SLOW_PATH_SPEED)*0.5))
	} else if THRESHOLD_ADJUST_METHOD == 3 {
		if ss.Turn%ELIXIR_REPLACE_WINDOW_SIZE == 0 {
			for _, flow := range ss.FlowMap {
				flow.IsOffloaded = false
			}
			for _, item := range ss.ElixirMap {
				ss.FlowMap[item.value].IsOffloaded = true
			}
			ss.ElixirTime = (ss.ElixirTime + 1) % (ELIXIR_IDENTIFY_WINDOW_SIZE / ELIXIR_REPLACE_WINDOW_SIZE)
			for i := 0; i < ELIXIR_HASH_FUNCTIONS; i++ {
				for j := 0; j < ELIXIR_HASH_SLOTS; j++ {
					ss.ElixirArray[i][j][ss.ElixirTime] = 0
				}
			}
		}
	} else {
		panic("invalid threshold adjust method")
	}
}

func main() {
	fmt.Printf("CPS: %d, PPS: %d, TURNS: %d, MAGNIFICATION:%v, ELEPHANT_BATCH_SIZE: %d, ELEPHANT_FLOW_PROPORTION: %d, MAX_OFFLOAD_SPEED: %d, ADJUST_METHOD: %d, ALPHA: %v, MAX_SLOW_PATH_SPEED: %d\n", CPS, PPS, TURNS, MAGNIFICATION_OF_EACH_STAGE, ELEPHANT_BATCH_SIZE, ELEPHANT_FLOW_PROPORTION, MAX_OFFLOAD_SPEED, THRESHOLD_ADJUST_METHOD, ALPHA_PARAM, MAX_SLOW_PATH_SPEED)

	var threshold []int
	var ssList []*SS
	if THRESHOLD_ADJUST_METHOD == 0 {
		threshold = []int{-1, 2, 4, 8, 16, 32, 64}
		//threshold = []int{8}
		ssList = make([]*SS, len(threshold))
		for i := 0; i < len(threshold); i++ {
			ssList[i] = &SS{
				OffloadThreshold: threshold[i],
				FlowMap:          []*Flow{},
				PacketQueue:      make([][]int, PPS),
			}
		}
	} else if THRESHOLD_ADJUST_METHOD == 1 {
		threshold = []int{4}
		ssList = append(ssList, &SS{
			AlphaParameter:   ALPHA_PARAM,
			OffloadThreshold: 4,
			FlowMap:          []*Flow{},
			PacketQueue:      make([][]int, PPS),
		})
	} else if THRESHOLD_ADJUST_METHOD == 2 {
		alpha := []float64{0.75, 1, 1.25}
		//alpha := []float64{0.75}
		ssList = make([]*SS, len(alpha))
		for i := 0; i < len(alpha); i++ {
			ssList[i] = &SS{
				AlphaParameter:   alpha[i],
				OffloadThreshold: 4,
				FlowMap:          []*Flow{},
				PacketQueue:      make([][]int, PPS),
			}
		}
	} else if THRESHOLD_ADJUST_METHOD == 3 {
		ssList = append(ssList, &SS{
			FlowMap:          []*Flow{},
			OffloadThreshold: -1, // Do not use offload threshold to filter flow.
			PacketQueue:      make([][]int, PPS),
			ElixirArray:      make([][][]uint64, ELIXIR_HASH_FUNCTIONS),
			ElixirTime:       0,
		})
		for i := 0; i < ELIXIR_HASH_FUNCTIONS; i++ {
			ssList[0].ElixirHashParameters = append(ssList[0].ElixirHashParameters, struct{ a, b int }{a: rand.Intn(ELIXIR_MAX_HASH), b: rand.Intn(ELIXIR_MAX_HASH)})
			ssList[0].ElixirArray[i] = make([][]uint64, ELIXIR_HASH_SLOTS)
			for j := 0; j < ELIXIR_HASH_SLOTS; j++ {
				ssList[0].ElixirArray[i][j] = make([]uint64, ELIXIR_IDENTIFY_WINDOW_SIZE/ELIXIR_REPLACE_WINDOW_SIZE)
			}
		}
	} else {
		panic("invalid threshold adjust method")
	}

	var wg sync.WaitGroup
	for i := 0; i < len(ssList); i++ {
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

				if THRESHOLD_ADJUST_METHOD == 3 {
					ss.ElixirMap = make(map[int]*Item)
					ss.ElixirMinHeap = make(PriorityQueue, 0)
					heap.Init(&ss.ElixirMinHeap)
				}

				PacketGenerator(ss)

				ss.SlowPathCount, ss.DropCount, ss.FastPathCount, ss.OffloadRuleCount, ss.OverOffloadRuleCount = 0, 0, 0, 0, 0
				PacketProcessor(ss)
				if len(ssList) == 1 {
					//fmt.Printf("%d\n", ss.PacketAmount)
					//fmt.Printf("turn: %-4d offloadThreshold: %-4d packetCount: %-8d slowPathCount: %-8d dropCount: %-8d fastPathCount: %-8d offloadCount: %-6d overOffloadCount: %-6d\n",
					//	ss.Turn, ss.OffloadThreshold, ss.PacketAmount, ss.SlowPathCount, ss.DropCount, ss.FastPathCount, ss.OffloadRuleCount, ss.OverOffloadRuleCount)
				}
				PostProcess(ss)
				ss.TotalPacketAmount += ss.PacketAmount
				ss.TotalDropCount += ss.DropCount
				ss.TotalSlowPathCount += ss.SlowPathCount
				ss.TotalFastPathCount += ss.FastPathCount
			}

		}(ssList[i])
	}
	wg.Wait()
	for i := 0; i < len(ssList); i++ {
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
		if THRESHOLD_ADJUST_METHOD != 3 {
			fmt.Printf("threshold: %d, alpha: %f, drop rate: %f%%, latency: %f us, flow finish rate: %f%%, average flow finish time: %f s\n",
				ssList[i].OffloadThreshold, ssList[i].AlphaParameter, float64(ssList[i].TotalDropCount)/float64(ssList[i].TotalPacketAmount)*100,
				(float64(ssList[i].TotalFastPathCount)*FAST_PATH_LATENCY_US+float64(ssList[i].TotalSlowPathCount+ssList[i].TotalDropCount)*SLOW_PATH_LATNECY_US)/float64(ssList[i].TotalSlowPathCount+ssList[i].TotalFastPathCount),
				float64(flowFinishCount)/float64(len(ssList[i].FlowMap))*100, float64(flowFinishTime)/float64(flowFinishCount)/1000/1000)
		} else {
			fmt.Printf("identification window: %d, replacement window: %d, drop rate: %f%%, latency: %f us, flow finish rate: %f%%, average flow finish time: %f s\n",
				ELIXIR_IDENTIFY_WINDOW_SIZE, ELIXIR_REPLACE_WINDOW_SIZE, float64(ssList[i].TotalDropCount)/float64(ssList[i].TotalPacketAmount)*100,
				(float64(ssList[i].TotalFastPathCount)*FAST_PATH_LATENCY_US+float64(ssList[i].TotalSlowPathCount+ssList[i].TotalDropCount)*SLOW_PATH_LATNECY_US)/float64(ssList[i].TotalSlowPathCount+ssList[i].TotalFastPathCount),
				float64(flowFinishCount)/float64(len(ssList[i].FlowMap))*100, float64(flowFinishTime)/float64(flowFinishCount)/1000/1000)
		}
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
