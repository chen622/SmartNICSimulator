# SmartNIC Simulator

The simulator is used to simulate the adjustment process of the SmartNIC offload threshold.
And the threshold is used to determine whether to offload a flow to the SmartNIC or not.
The reason for implementing a threshold is that the process of applying a rule to the SmartNIC is slow.
Therefore, we need a threshold to filter flows and only offload those which may have a large number of packets.

## Design

The simulator will simulate the process of offloading flow using an SmartNIC. Each second in the real
environment will be treated as a round of iteration in the simulator. The process is as follows:

1. In each round, the simulator will first use a packet distribution for new
flows, and generate all new flows for this round based on the `CPS` pre-set in the program.

2. The simulator then sends all packets based on the new flows and packets from historical unfinished flow,
until all flows have been traversed or the number of packets reaches the PPS set in the simulator, thus obtaining
all packets for this round.

3. After obtaining all packets for this round, the simulator will process each packet and determine whether it will
appear on the fast path or slow path based on the offloading status recorded in the flow table in the simulator. If it is
on the fast path, only counting is required. If it is on the slow path, it needs to be determined whether the flow
satisfies the offloading threshold `offloadThreshold`, and whether the speed of offloaded flows `offloadCount` for this
round is less than the maximum offloading capacity `MAX_OFFLOAD_SPEED`. If it satisfies, the flow is marked as offloaded
and increase `offloadCount`. If it exceeds the maximum offloading capacity, will increase the number of overflow offloads
`overOffloadCount`. In both cases, will increase the slow path counter `slowPathCount`. When
`slowPathCount` exceeds the maximum processing capacity of the slow path `MAX_SLOW_PATH_SPEED`, the subsequent data packets
are marked as discarded, and increase the discard counter `dropCount`.

4. After the above process is completed, it indicates that the processing for this round is completed. If the dynamic
threshold algorithm is used at this time, the threshold will be adjusted based on the three parameters `offloadCount`,
`overOffloadCount`, `dropCount`, and the simulator will continue to generate traffic for the next round.


