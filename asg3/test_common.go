package asg3

import (
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// ==================================
//  Helper methods used in test code
// ==================================

// Directory containing all the test files
const testDir = "test_data"

// Read the topology from a ".top" file.
// The expected format of the file is as follows:
//   - The first line contains number of nodes N (e.g. "2")
//   - The next N lines each contains the node ID and the number of tokens on
//     that node, in the form "[nodeId] [numTokens]" (e.g. "N1 1")
//   - The rest of the lines represent unidirectional links in the form "[src dst]"
//     (e.g. "N1 N2")
func readTopologyFile(fileName string, sim *ChandyLamportSim) {
	b, err := ioutil.ReadFile(path.Join(testDir, fileName))
	checkError(err)
	lines := strings.FieldsFunc(string(b), func(r rune) bool { return r == '\n' })

	// Must call this before we start logging
	sim.logger.NewEpoch()

	// Parse topology from lines
	numNodesLeft := -1
	for _, line := range lines {
		// Ignore comments
		if strings.HasPrefix(line, "#") {
			continue
		}
		if numNodesLeft < 0 {
			numNodesLeft, err = strconv.Atoi(line)
			checkError(err)
			continue
		}
		// Otherwise, always expect 2 tokens
		parts := strings.Fields(line)
		if len(parts) != 2 {
			log.Fatal("Expected 2 tokens in line: ", line)
		}
		if numNodesLeft > 0 {
			// This is a node
			nodeId := parts[0]
			numTokens, err := strconv.Atoi(parts[1])
			checkError(err)
			sim.AddNode(nodeId, numTokens)
			numNodesLeft--
		} else {
			// This is a link
			src := parts[0]
			dest := parts[1]
			sim.AddLink(src, dest)
		}
	}
}

// Read the events from a ".events" file and inject the events into the simulator.
// The expected format of the file is as follows:
//   - "tick N" indicates N time steps has elapsed (default N = 1)
//   - "send N1 N2 1" indicates that N1 sends 1 token to N2
//   - "snapshot N2" indicates the beginning of the snapshot process, starting on N2
//
// Note that concurrent events are indicated by the lack of ticks between the events.
// This function waits until all the snapshot processes have terminated before returning
// the snapshots collected.
func readEventsFile(fileName string, sim *ChandyLamportSim) []*GlobalSnapshot {
	b, err := ioutil.ReadFile(path.Join(testDir, fileName))
	checkError(err)

	snapshots := make([]*GlobalSnapshot, 0)
	getSnapshots := make(chan *GlobalSnapshot, 100)
	numSnapshots := 0

	lines := strings.FieldsFunc(string(b), func(r rune) bool { return r == '\n' })
	// for _, line := range lines {
	// 	fmt.Println(line)
	// }
	// fmt.Println("before for loop")
	for _, line := range lines {
		fmt.Println(line)
		// Ignore comments
		if strings.HasPrefix("#", line) {
			continue
		}
		parts := strings.Fields(line)
		switch parts[0] {
		case "send":
			src := parts[1]
			dest := parts[2]
			tokens, err := strconv.Atoi(parts[3])
			checkError(err)
			sim.ProcessEvent(PassTokenEvent{src, dest, tokens})
		case "snapshot":
			numSnapshots++
			nodeId := parts[1]
			snapshotId := sim.nextSnapshotId
			sim.ProcessEvent(SnapshotEvent{nodeId})
			go func(id int) {
				getSnapshots <- sim.CollectSnapshot(id)
			}(snapshotId)
		case "tick":
			numTicks := 1
			if len(parts) > 1 {
				numTicks, err = strconv.Atoi(parts[1])
				checkError(err)
			}
			for i := 0; i < numTicks; i++ {
				sim.Tick()
			}
		default:
			log.Fatal("Unknown event command: ", parts[0])
		}
	}
	fmt.Println("after for loop")
	// Keep ticking until snapshots complete
	for numSnapshots > 0 {
		select {
		case snap := <-getSnapshots:
			snapshots = append(snapshots, snap)
			numSnapshots--
		default:
			sim.Tick()
		}
	}
	fmt.Println("after snapcomplete loop")
	// Keep ticking until we're sure that the last message has been delivered
	for i := 0; i < maxDelay+1; i++ {
		sim.Tick()
	}
	fmt.Println("after last message")

	return snapshots
}

// Read the state of snapshot from a ".snap" file.
// The expected format of the file is as follows:
//   - The first line contains the snapshot ID (e.g. "0")
//   - The next N lines contains the node ID and the number of tokens on that node,
//     in the form "[nodeId] [numTokens]" (e.g. "N1 0"), one line per node
//   - The rest of the lines represent messages exchanged between the nodes,
//     in the form "[src] [dest] [message]" (e.g. "N1 N2 token(1)")
func readSnapshotFile(fileName string) *GlobalSnapshot {
	b, err := ioutil.ReadFile(path.Join(testDir, fileName))
	checkError(err)
	snapshot := GlobalSnapshot{0, make(map[string]int), make([]*MsgSnapshot, 0)}
	lines := strings.FieldsFunc(string(b), func(r rune) bool { return r == '\n' })
	for _, line := range lines {
		// Ignore comments
		if strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) == 1 {
			// Snapshot ID
			snapshot.id, err = strconv.Atoi(line)
			checkError(err)
		} else if len(parts) == 2 {
			// Node and its tokens
			nodeId := parts[0]
			numTokens, err := strconv.Atoi(parts[1])
			checkError(err)
			snapshot.tokenMap[nodeId] = numTokens
		} else if len(parts) == 3 {
			// Src, dest and message
			src := parts[0]
			dest := parts[1]
			messageString := parts[2]
			var message Message
			if strings.Contains(messageString, "token") {
				pattern := regexp.MustCompile(`[0-9]+`)
				matches := pattern.FindStringSubmatch(messageString)
				if len(matches) != 1 {
					log.Fatal("Unable to parse token message: ", messageString)
				}
				numTokens, err := strconv.Atoi(matches[0])
				checkError(err)
				message = Message{isMarker: false, data: numTokens}
			} else {
				log.Fatal("Unknown message: ", messageString)
			}
			snapshot.messages =
				append(snapshot.messages, &MsgSnapshot{src, dest, message})
		}
	}
	return &snapshot
}

// Helper function to pretty print the tokens in the given snapshot state
func tokensString(tokens map[string]int, prefix string) string {
	str := make([]string, 0)
	for _, nodeId := range getSortedKeys(tokens) {
		numTokens := tokens[nodeId]
		maybeS := "s"
		if numTokens == 1 {
			maybeS = ""
		}
		str = append(str, fmt.Sprintf(
			"%v%v: %v token%v", prefix, nodeId, numTokens, maybeS))
	}
	return strings.Join(str, "\n")
}

// Helper function to pretty print the messages in the given snapshot state
func messagesString(messages []*MsgSnapshot, prefix string) string {
	str := make([]string, 0)
	for _, msg := range messages {
		str = append(str, fmt.Sprintf(
			"%v%v -> %v: %v", prefix, msg.src, msg.dest, msg.message))
	}
	return strings.Join(str, "\n")
}

// Assert that the two snapshot states are equal.
// If they are not equal, throw an error with a helpful message.
func assertEqual(expected, actual *GlobalSnapshot) {
	if expected.id != actual.id {
		log.Fatalf("Snapshot IDs do not match: %v != %v\n", expected.id, actual.id)
	}
	if len(expected.tokenMap) != len(actual.tokenMap) {
		log.Fatalf(
			"Snapshot %v: Number of tokens do not match."+
				"\nExpected:\n%v\nActual:\n%v\n",
			expected.id,
			tokensString(expected.tokenMap, "\t"),
			tokensString(actual.tokenMap, "\t"))
	}
	if len(expected.messages) != len(actual.messages) {
		log.Fatalf(
			"Snapshot %v: Number of messages do not match."+
				"\nExpected:\n%v\nActual:\n%v\n",
			expected.id,
			messagesString(expected.messages, "\t"),
			messagesString(actual.messages, "\t"))
	}
	for id, tok := range expected.tokenMap {
		if actual.tokenMap[id] != tok {
			log.Fatalf(
				"Snapshot %v: Tokens on %v do not match."+
					"\nExpected:\n%v\nActual:\n%v\n",
				expected.id,
				id,
				tokensString(expected.tokenMap, "\t"),
				tokensString(actual.tokenMap, "\t"))
		}
	}
	// Ensure message order is preserved per destination
	// Note that we don't require ordering of messages across all nodes to match
	expectedMessages := make(map[string][]*MsgSnapshot)
	actualMessages := make(map[string][]*MsgSnapshot)
	for i := 0; i < len(expected.messages); i++ {
		em := expected.messages[i]
		am := actual.messages[i]
		_, ok1 := expectedMessages[em.dest]
		_, ok2 := actualMessages[am.dest]
		if !ok1 {
			expectedMessages[em.dest] = make([]*MsgSnapshot, 0)
		}
		if !ok2 {
			actualMessages[am.dest] = make([]*MsgSnapshot, 0)
		}
		expectedMessages[em.dest] = append(expectedMessages[em.dest], em)
		actualMessages[am.dest] = append(actualMessages[am.dest], am)
	}
	// Test message order per destination
	for dest := range expectedMessages {
		ems := expectedMessages[dest]
		ams := actualMessages[dest]
		if !reflect.DeepEqual(ems, ams) {
			log.Fatalf(
				"Snapshot %v: Messages received at %v do not match."+
					"\nExpected:\n%v\nActual:\n%v\n",
				expected.id,
				dest,
				messagesString(ems, "\t"),
				messagesString(ams, "\t"))
		}
	}
}

// Helper function to sort the snapshot states by ID.
func sortSnapshots(snaps []*GlobalSnapshot) {
	sort.Slice(snaps, func(i, j int) bool {
		s1 := snaps[i]
		s2 := snaps[j]
		return s2.id > s1.id
	})
}

// Verify that the total number of tokens recorded in the snapshot preserves
// the number of tokens in the system
func checkTokens(sim *ChandyLamportSim, snapshots []*GlobalSnapshot) {
	expectedTokens := 0
	for _, node := range sim.nodes {
		expectedTokens += node.tokens
	}
	for _, snap := range snapshots {
		snapTokens := 0
		// Add tokens recorded on nodes
		for _, tok := range snap.tokenMap {
			snapTokens += tok
		}
		// Add tokens from messages in-flight
		for _, message := range snap.messages {
			if !message.message.isMarker {
				snapTokens += message.message.data
			}
			// switch msg := message.message.(type) {
			// case TokenMessage:
			// 	snapTokens += msg.numTokens
			// }
		}
		if expectedTokens != snapTokens {
			log.Fatalf("Snapshot %v: simulator has %v tokens, snapshot has %v:\n%v\n%v",
				snap.id,
				expectedTokens,
				snapTokens,
				tokensString(snap.tokenMap, "\t"),
				messagesString(snap.messages, "\t"))
		}
	}
}
