package asg3

import (
	// "fmt"
	"log"
	"sync"
)

// The main participant of the distributed snapshot protocol.
// nodes exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one node to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.

type Node struct {
	sim           *ChandyLamportSim
	id            string
	tokens        int
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: add more fields here (what does each node need to keep track of?)
	snapshotId	 int             // snapshot ID
	prevTokens      map[int]int     // map of snapshot ID to previous tokens
	markersReceived map[string]bool // map of node ID to marker received status
	mutex           sync.Mutex      // mutex for synchronization
	MsgSnapshot	 []*MsgSnapshot
}

// A unidirectional communication channel between two nodes
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src      string
	dest     string
	msgQueue *Queue
}

func CreateNode(id string, tokens int, sim *ChandyLamportSim) *Node {
	return &Node{
		sim:           sim,
		id:            id,
		tokens:        tokens,
		outboundLinks: make(map[string]*Link),
		inboundLinks:  make(map[string]*Link),
		// TODO: You may need to modify this if you make modifications above
		prevTokens:      make(map[int]int),
		markersReceived: make(map[string]bool),
		mutex:           sync.Mutex{},
		MsgSnapshot:	 make([]*MsgSnapshot, 0),
	}
}

// Add a unidirectional link to the destination node
func (node *Node) AddOutboundLink(dest *Node) {
	if node == dest {
		return
	}
	l := Link{node.id, dest.id, NewQueue()}
	node.outboundLinks[dest.id] = &l
	dest.inboundLinks[node.id] = &l
}

// Send a message on all of the node's outbound links
func (node *Node) SendToNeighbors(message Message) {
	for _, nodeId := range getSortedKeys(node.outboundLinks) {
		link := node.outboundLinks[nodeId]
		node.sim.logger.RecordEvent(
			node,
			SentMsgRecord{node.id, link.dest, message})
		link.msgQueue.Push(SendMsgEvent{
			node.id,
			link.dest,
			message,
			node.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this node
func (node *Node) SendTokens(numTokens int, dest string) {
	if node.tokens < numTokens {
		log.Fatalf("node %v attempted to send %v tokens when it only has %v\n",
			node.id, numTokens, node.tokens)
	}
	message := Message{isMarker: false, data: numTokens}
	node.sim.logger.RecordEvent(node, SentMsgRecord{node.id, dest, message})
	// Update local state before sending the tokens
	node.tokens -= numTokens
	link, ok := node.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from node %v\n", dest, node.id)
	}

	link.msgQueue.Push(SendMsgEvent{
		node.id,
		dest,
		message,
		node.sim.GetReceiveTime()})
}

func (node *Node) HandlePacket(src string, message Message) {
	// TODO: Write this method
	// node.sim.CollectingSnapshot.Wait()
	if message.isMarker {
		// fmt.Println("Node", node.id, "received marker", message.data, "from", src, "with", node.tokens, "tokens")
		if _, ok := node.prevTokens[message.data]; ok {
			// fmt.Println("tokens before snapshot returned", node.prevTokens[message.data])
		} else {
			node.inboundLinks[src].msgQueue = NewQueue()
			node.StartSnapshot(message.data)
		}
		node.markersReceived[src] = true
		if len(node.markersReceived) == len(node.inboundLinks) {
			node.sim.NotifyCompletedSnapshot(node.id, message.data)
		}
	} else {
		node.tokens += message.data
		if _, ok := node.prevTokens[node.snapshotId]; ok && !node.markersReceived[src] {
			if diff := node.tokens - node.prevTokens[node.snapshotId]; diff > 0 {
				node.MsgSnapshot = append(node.MsgSnapshot, &MsgSnapshot{src, node.id, Message{isMarker: false, data: diff}})
			}
		}
	}
}

func (node *Node) StartSnapshot(snapshotId int) {
	// ToDo: Write this methodz
	node.sim.snapshotComplete.Add(1)
	node.prevTokens[snapshotId] = node.tokens
	node.SendToNeighbors(Message{isMarker: true, data: snapshotId})
}
