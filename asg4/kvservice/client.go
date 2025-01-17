package kvservice

import (
	"log"
	"net/rpc"
	"strconv"
	"sysmonitor"
)

// import "time"
// import "crypto/rand"
// import "math/big"

type KVClient struct {
	monitorClnt *sysmonitor.Client

	// view provides information about which is primary, and which is backup.
	// Use updateView() to update this view when doing get and put as needed.
	view sysmonitor.View
	id   string // should be generated to be a random string
}

func MakeKVClient(monitorServer string) *KVClient {
	client := new(KVClient)
	client.monitorClnt = sysmonitor.MakeClient("", monitorServer)
	client.view = sysmonitor.View{} // An empty view.

	// ToDo: Generate a random id for the client.
	// ==================================
	client.id = strconv.FormatInt(nrand(), 10)
	//====================================

	return client
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

// You can use this method to update the client's view when needed during get and put operations.
func (client *KVClient) updateView() {
	view, _ := client.monitorClnt.Get()
	client.view = view
}

// Fetch a key's value from the current primary via an RPC call.
// You can get the primary from the client's view.
// If the key was never set, "" is expected.
// This must keep trying until it gets a response.
func (client *KVClient) Get(key string) string {
	// Your code here.
	if key == "" {
		log.Printf("Get: key is empty\n")
		return ""
	}
	for {
		client.updateView()
		primary := client.view.Primary
		args := GetArgs{Key: key, IsClient: true}
		var reply GetReply
		ok := call(primary, "KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			return reply.Value
		}
	}
}

// This should tell the primary to update key's value via an RPC call.
// must keep trying until it succeeds.
// You can get the primary from the client's current view.
func (client *KVClient) PutAux(key string, value string, dohash bool) string {
	// Your code here.
	if key == "" {
		log.Printf("PutAux: key is empty\n")
		return ""
	}
	requestID := strconv.FormatInt(nrand(), 10)
	requestID = client.id + "_" + requestID
	for {
		primary := client.view.Primary
		args := PutArgs{Key: key, Value: value, DoHash: dohash, IsClient: true, RequestID: requestID}
		var reply PutReply
		ok := call(primary, "KVServer.Put", &args, &reply)
		//fmt.Println("PutAux: ", ok)
		if ok && reply.Err == OK {
			return reply.PreviousValue
		}
		client.updateView()
	}
}

// Both put and puthash rely on the auxiliary method PutAux. No modifications needed below.
func (client *KVClient) Put(key string, value string) {
	client.PutAux(key, value, false)
}

func (client *KVClient) PutHash(key string, value string) string {
	v := client.PutAux(key, value, true)
	return v
}
