package kvservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"sysmonitor"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type KVServer struct {
	l           net.Listener
	dead        bool // for testing
	unreliable  bool // for testing
	id          string
	monitorClnt *sysmonitor.Client
	view        sysmonitor.View
	done        sync.WaitGroup
	finish      chan interface{}

	// Add your declarations here.
	hasBackup bool // Indicates if the server has a backup.
	backup    string // The backup server's ID.
	data      map[string]string // The key/value database.
	requestID map[string]string // The request ID for each key.
	reqreply  map[string]PutReply // The reply for each request ID.
	mu        sync.RWMutex // Mutex for the key/value database.
}

// Store the key/value pair in the Request database.
func (server *KVServer) updateRequest(args *PutArgs, reply *PutReply) {
	// Store the request ID for the key.
	server.requestID[args.Key] = args.RequestID
	// Store the reply for the request ID.
	server.reqreply[args.RequestID] = *reply
}

func (server *KVServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	server.mu.Lock()
	defer server.mu.Unlock()

	// Check if the request ID is already stored for the key.
	if args.RequestID == server.requestID[args.Key] {
		// If the request ID is already stored for the key, return the previous reply.
		prevReply := server.reqreply[args.RequestID]
		//fmt.Println("Previous Reply: ", prevReply)
		reply.Err = prevReply.Err
		reply.PreviousValue = prevReply.PreviousValue
		return nil
	}
	
	if args.IsClient {
		if server.id == server.view.Primary {
			if args.DoHash {
				// If the PutArgs has DoHash set to true, hash the value before storing it.
				previousValue, ok := server.data[args.Key] 
				hashedValue := hash(previousValue + args.Value)
				reply.Err = OK
				if ok {
					reply.PreviousValue = previousValue
				} else {
					reply.PreviousValue = ""
				}
				val := fmt.Sprintf("%v", hashedValue)
				server.data[args.Key] = val
				server.updateRequest(args, reply)
				if server.hasBackup {
					call_args := &PutArgs{args.Key, val, false, false, args.RequestID}
					call_reply := &PutReply{}
					err := call(server.backup, "KVServer.Put", call_args, call_reply)
					if err != true {
						return nil
					}
				}
			} else {
				// If the PutArgs has DoHash set to false, store the value as is.
				previousValue, ok := server.data[args.Key]
				reply.Err = OK
				if ok {
					reply.PreviousValue = previousValue
				} else {
					reply.PreviousValue = ""
				}
				server.data[args.Key] = args.Value
				server.updateRequest(args, reply)
				// Forward the data to the backup if it exists.
				if server.hasBackup {
					call_args := &PutArgs{args.Key, args.Value, false, false, args.RequestID}
					call_reply := &PutReply{}
					err := call(server.backup, "KVServer.Put", call_args, call_reply)
					if err != true {
						return nil
					}
				}
			}
		} else {
			reply.Err = ErrWrongServer
			reply.PreviousValue = ""
		}
	} else {
		// If the PutArgs is not from a client, store the value as is.
		if args.DoHash {
			previousValue, ok := server.data[args.Key]
			hashedValue := hash(args.Value + previousValue)
			reply.Err = OK
			if ok {
				reply.PreviousValue = previousValue
			} else {
				reply.PreviousValue = ""
			}
			val := fmt.Sprintf("%v", hashedValue)
			server.updateRequest(args, reply)
			server.data[args.Key] = val
		} else {
			previousValue, ok := server.data[args.Key]
			reply.Err = OK
			if ok {
				reply.PreviousValue = previousValue
			} else {
				reply.PreviousValue = ""
			}
			server.data[args.Key] = args.Value
			server.updateRequest(args, reply)
		}
	}

	return nil
}

func (server *KVServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	server.mu.RLock()
	defer server.mu.RUnlock()

	// Get the value from the key/value database.
	if args.IsClient {
		if server.id == server.view.Primary {
			value, ok := server.data[args.Key]
			if ok {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongServer
			reply.Value = ""
		}
	} else {
		// If the GetArgs is not from a client, return the value as is.
		value, ok := server.data[args.Key]
		if ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
	}
	return nil
}

// ping the viewserver periodically.
func (server *KVServer) tick() {
	// This line will give an error initially as view and err are not used.
	view, err := server.monitorClnt.Ping(server.view.Viewnum)
	if err != nil {
		return
	}
	// Your code here.
	server.view = view
	// Determine the server's role based on the view.
	if server.id == server.view.Primary {
		// If the server is the primary and detects a new backup, handle the data forwarding.
		if server.view.Backup != "" {
			if server.view.Backup != server.backup {
				// If the server is the primary and there is a new backup, update the backup state.
				server.backup = server.view.Backup
				server.hasBackup = true
				// Forward data to the new backup.
				for key, value := range server.data {
					reqID := server.requestID[key]
					args := &PutArgs{key, value, false, false, reqID}
					reply := &PutReply{}
					err := call(server.backup, "KVServer.Put", args, reply)
					if err != true {
						return
					}
				}
			}
		} else {
			// If the server is the primary and there is no backup, reset the backup state.
			server.hasBackup = false
			server.backup = ""
		}
	} else if server.id == server.view.Backup {
		// If the server is the backup, reset the backup state.
		server.hasBackup = false
		server.backup = ""
	} else {
		// If the server is neither the primary nor the backup, reset the backup state.
		server.hasBackup = false
		server.backup = ""
		server.data = make(map[string]string)
		server.requestID = make(map[string]string)
		server.reqreply = make(map[string]PutReply)
	}

}

// tell the server to shut itself down.
// please do not change this function.
func (server *KVServer) Kill() {
	server.dead = true
	server.l.Close()
}

func StartKVServer(monitorServer string, id string) *KVServer {
	server := new(KVServer)
	server.id = id
	server.monitorClnt = sysmonitor.MakeClient(id, monitorServer)
	server.view = sysmonitor.View{}
	server.finish = make(chan interface{})

	// Add your server initializations here
	// ==================================
	server.backup = ""
	server.hasBackup = false
	server.data = make(map[string]string)
	server.requestID = make(map[string]string)
	server.reqreply = make(map[string]PutReply)
	//====================================

	rpcs := rpc.NewServer()
	rpcs.Register(server)

	os.Remove(server.id)
	l, e := net.Listen("unix", server.id)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	server.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for server.dead == false {
			conn, err := server.l.Accept()
			if err == nil && server.dead == false {
				if server.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if server.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					server.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						server.done.Done()
					}()
				} else {
					server.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						server.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && server.dead == false {
				fmt.Printf("KVServer(%v) accept: %v\n", id, err.Error())
				server.Kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", server.id)
		server.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(server.finish)
	}()

	server.done.Add(1)
	go func() {
		for server.dead == false {
			server.tick()
			time.Sleep(sysmonitor.PingInterval)
		}
		server.done.Done()
	}()

	return server
}
